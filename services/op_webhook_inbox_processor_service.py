"""
Process webhook_inbox rows into OpenPhone destination tables.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from services.database import ConnectionFactory


SUPPORTED_OPENPHONE_EVENT_TYPES = {"sms"}


@dataclass
class WebhookInboxProcessingSummary:
    scanned: int = 0
    processed: int = 0
    failed: int = 0
    skipped: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "scanned": self.scanned,
            "processed": self.processed,
            "failed": self.failed,
            "skipped": self.skipped,
        }


@dataclass
class OpenPhoneWebhookInboxProcessorService:
    """
    Move webhook_inbox rows into OpenPhone final tables.
    """

    connection_factory: ConnectionFactory

    def process_unprocessed(
        self,
        *,
        limit: int = 100,
        source: str = "openphone",
        max_attempts: Optional[int] = None,
    ) -> WebhookInboxProcessingSummary:
        if limit <= 0:
            raise ValueError("limit must be greater than zero.")
        if max_attempts is not None and max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero when provided.")

        summary = WebhookInboxProcessingSummary()
        row_ids = self._fetch_candidate_ids(
            limit=limit,
            source=source,
            max_attempts=max_attempts,
        )
        summary.scanned = len(row_ids)

        for row_id in row_ids:
            result = self._process_one_row(row_id=row_id, source=source)
            if result == "processed":
                summary.processed += 1
            elif result == "failed":
                summary.failed += 1
            else:
                summary.skipped += 1

        return summary

    def _fetch_candidate_ids(
        self,
        *,
        limit: int,
        source: str,
        max_attempts: Optional[int],
    ) -> list[int]:
        with self.connection_factory.connect() as conn:
            query = """
                SELECT id
                FROM webhook_inbox
                WHERE status IN ('unprocessed', 'failed')
                  AND source = ?
                  AND event_type = 'sms'
                """
            query_params: list[Any] = [source]
            if max_attempts is not None:
                query += """
                  AND COALESCE(attempts, 0) < ?
                """
                query_params.append(max_attempts)
            query += """
                ORDER BY id ASC
                LIMIT ?;
                """
            query_params.append(limit)
            rows = conn.execute(query, tuple(query_params)).fetchall()
        return [int(row["id"]) for row in rows]

    def _process_one_row(self, *, row_id: int, source: str) -> str:
        try:
            with self.connection_factory.connect() as conn:
                row = conn.execute(
                    """
                    SELECT *
                    FROM webhook_inbox
                    WHERE id = ?;
                    """,
                    (row_id,),
                ).fetchone()
                if row is None:
                    return "skipped"
                if row["status"] not in {"unprocessed", "failed"} or row["source"] != source:
                    return "skipped"
                if str(row["event_type"] or "").strip().lower() != "sms":
                    return "skipped"

                started_at = self._utcnow_iso()
                conn.execute(
                    """
                    UPDATE webhook_inbox
                    SET status = 'processing',
                        last_attempted_at = ?,
                        error_message = NULL
                    WHERE id = ?;
                    """,
                    (started_at, row_id),
                )

                payload = self._load_payload(row=row)
                processed_table, processed_row_id = self._route_openphone_event(
                    conn=conn,
                    row=row,
                    payload=payload,
                )

                conn.execute(
                    """
                    UPDATE webhook_inbox
                    SET status = 'processed',
                        processed_at = ?,
                        error_message = NULL,
                        processed_table = ?,
                        processed_row_id = ?
                    WHERE id = ?;
                    """,
                    (
                        self._utcnow_iso(),
                        processed_table,
                        str(processed_row_id),
                        row_id,
                    ),
                )
            return "processed"
        except Exception as exc:
            self._mark_failed(row_id=row_id, error_message=str(exc))
            return "failed"

    def _mark_failed(self, *, row_id: int, error_message: str) -> None:
        with self.connection_factory.connect() as conn:
            conn.execute(
                """
                UPDATE webhook_inbox
                SET status = 'failed',
                    attempts = COALESCE(attempts, 0) + 1,
                    last_attempted_at = ?,
                    error_message = ?,
                    processed_table = NULL,
                    processed_row_id = NULL
                WHERE id = ?;
                """,
                (
                    self._utcnow_iso(),
                    (error_message or "Unknown processing error")[:1000],
                    row_id,
                ),
            )

    def _route_openphone_event(
        self,
        *,
        conn: Any,
        row: Any,
        payload: dict[str, Any],
    ) -> tuple[str, int]:
        event_type = str(row["event_type"] or "").strip().lower()
        if event_type not in SUPPORTED_OPENPHONE_EVENT_TYPES:
            raise ValueError(f"Unsupported event_type for OpenPhone processor: {event_type}")

        sms_row_id = self._upsert_sms_message(conn=conn, row=row, payload=payload)
        return ("openphone_sms_messages", sms_row_id)

    def _upsert_sms_message(self, *, conn: Any, row: Any, payload: dict[str, Any]) -> int:
        obj = self._extract_message_object(payload=payload)
        sms_id = (obj.get("id") or row["message_id"] or "").strip()
        if not sms_id:
            raise ValueError("Payload is missing message id.")

        direction = self._normalize_direction(obj.get("direction"))
        from_phone = self._normalize_phone(obj.get("from") or obj.get("from_number"))
        to_phone = self._normalize_phone(self._extract_first_phone(obj.get("to")))
        if direction == "inbound":
            guest_phone = from_phone
            our_phone = to_phone
        else:
            guest_phone = to_phone
            our_phone = from_phone
        if not guest_phone:
            raise ValueError("Unable to resolve guest_phone from payload.")
        if not our_phone:
            raise ValueError("Unable to resolve our_phone from payload.")

        guest_id = self._resolve_guest_id(conn=conn, guest_phone=guest_phone)

        sent_at = (
            obj.get("createdAt")
            or obj.get("sentAt")
            or row["received_at_utc"]
            or row["received_at"]
            or self._utcnow_iso()
        )
        body = obj.get("text")
        if body is None:
            body = obj.get("body")
        if body is None:
            body = ""

        phone_number_id = obj.get("phoneNumberId") or row["phone_number_id"]
        user_id = obj.get("userId")
        message_status = obj.get("status")
        updated_at = obj.get("updatedAt")

        columns = self._get_table_columns(conn=conn, table_name="openphone_sms_messages")
        values_by_column: dict[str, Any] = {
            "openphone_sms_id": sms_id,
            "guest_id": guest_id,
            "guest_phone": guest_phone,
            "our_phone": our_phone,
            "direction": direction,
            "body": str(body),
            "sent_at": str(sent_at),
            "openphone_phone_number_id": str(phone_number_id) if phone_number_id else None,
            "openphone_user_id": str(user_id) if user_id else None,
            "status": str(message_status) if message_status is not None else None,
            "updated_at": str(updated_at) if updated_at is not None else None,
        }
        required_columns = {
            "openphone_sms_id",
            "guest_phone",
            "our_phone",
            "direction",
            "body",
            "sent_at",
        }
        missing_required = sorted(required_columns - columns)
        if missing_required:
            raise RuntimeError(
                "openphone_sms_messages is missing required columns: "
                + ", ".join(missing_required),
            )

        preferred_column_order = [
            "openphone_sms_id",
            "guest_id",
            "guest_phone",
            "our_phone",
            "direction",
            "body",
            "sent_at",
            "openphone_phone_number_id",
            "openphone_user_id",
            "status",
            "updated_at",
        ]
        insert_columns = [name for name in preferred_column_order if name in columns]
        update_columns = [name for name in insert_columns if name != "openphone_sms_id"]
        placeholders = ", ".join("?" for _ in insert_columns)
        conflict_updates = ", ".join(
            (
                "guest_id = COALESCE(excluded.guest_id, openphone_sms_messages.guest_id)"
                if column_name == "guest_id"
                else f"{column_name} = excluded.{column_name}"
            )
            for column_name in update_columns
        )
        sql = f"""
            INSERT INTO openphone_sms_messages ({", ".join(insert_columns)})
            VALUES ({placeholders})
            ON CONFLICT(openphone_sms_id) DO UPDATE SET
            {conflict_updates};
            """
        conn.execute(
            sql,
            tuple(values_by_column[column_name] for column_name in insert_columns),
        )

        row_result = conn.execute(
            """
            SELECT id
            FROM openphone_sms_messages
            WHERE openphone_sms_id = ?;
            """,
            (sms_id,),
        ).fetchone()
        if row_result is None:
            raise RuntimeError("Failed to resolve row id in openphone_sms_messages.")
        return int(row_result["id"])

    def _resolve_guest_id(self, *, conn: Any, guest_phone: str) -> Optional[int]:
        guests_columns = self._get_table_columns(conn=conn, table_name="guests")
        if not guests_columns:
            return None

        required_columns = {"id", "primary_phone"}
        if not required_columns.issubset(guests_columns):
            return None

        if "is_current" in guests_columns:
            row = conn.execute(
                """
                SELECT id
                FROM guests
                WHERE primary_phone = ?
                ORDER BY COALESCE(is_current, 0) DESC, id DESC
                LIMIT 1;
                """,
                (guest_phone,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id
                FROM guests
                WHERE primary_phone = ?
                ORDER BY id DESC
                LIMIT 1;
                """,
                (guest_phone,),
            ).fetchone()

        if row is None:
            return None
        return int(row["id"])

    @staticmethod
    def _extract_message_object(payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a JSON object.")

        nested_data = payload.get("data")
        if isinstance(nested_data, dict):
            nested_obj = nested_data.get("object")
            if isinstance(nested_obj, dict):
                return nested_obj

        if payload.get("id"):
            return payload

        raise ValueError("Payload does not contain message object data.")

    @staticmethod
    def _normalize_direction(raw_direction: Any) -> str:
        direction = str(raw_direction or "").strip().lower()
        if direction in {"incoming", "inbound"}:
            return "inbound"
        if direction in {"outgoing", "outbound"}:
            return "outbound"
        raise ValueError(f"Unsupported message direction: {raw_direction}")

    @staticmethod
    def _extract_first_phone(to_value: Any) -> Optional[str]:
        if isinstance(to_value, list):
            for item in to_value:
                normalized = OpenPhoneWebhookInboxProcessorService._normalize_phone(item)
                if normalized:
                    return normalized
            return None
        return OpenPhoneWebhookInboxProcessorService._normalize_phone(to_value)

    @staticmethod
    def _normalize_phone(raw_value: Any) -> Optional[str]:
        if raw_value is None:
            return None
        value = str(raw_value).strip()
        return value or None

    @staticmethod
    def _load_payload(*, row: Any) -> dict[str, Any]:
        payload_candidates = [row["payload_json"], row["raw_payload"]]
        for raw in payload_candidates:
            if raw is None:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            if isinstance(raw, str):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    return parsed
                raise ValueError("Payload JSON must be an object.")
            if isinstance(raw, dict):
                return raw
        raise ValueError("Payload is not valid JSON.")

    @staticmethod
    def _get_table_columns(*, conn: Any, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
        return {str(row["name"]) for row in rows}

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
