"""
Persistence for inbound OpenPhone webhook events.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from models.webhook_new_message import WebhookNewMessage
from services.database import ConnectionFactory


@dataclass
class OpenPhoneWebhookPersistenceService:
    """
    Stores webhook payloads in webhook_inbox for downstream processing.
    """

    connection_factory: ConnectionFactory

    @staticmethod
    def _normalize_event_type_for_storage(event_type: Optional[str]) -> str:
        """
        Keep event_type compatible with legacy webhook_inbox CHECK constraints.
        """
        if event_type == "message.received":
            return "sms"
        return event_type or "sms"

    def ensure_schema(self) -> None:
        """
        Ensure webhook inbox table/indexes exist.
        """
        with self.connection_factory.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS webhook_inbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message_id TEXT,
                    conversation_id TEXT,
                    phone_number_id TEXT,
                    payload_json TEXT NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                );
                """
            )
            self._migrate_webhook_inbox_schema(conn)
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_webhook_inbox_event_type
                ON webhook_inbox(event_type);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_webhook_inbox_message_id
                ON webhook_inbox(message_id);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_webhook_inbox_received_at_utc
                ON webhook_inbox(received_at_utc);
                """
            )

    @staticmethod
    def _migrate_webhook_inbox_schema(conn: Any) -> None:
        """
        Bring older webhook_inbox schemas up to date without destructive changes.
        """
        rows = conn.execute("PRAGMA table_info(webhook_inbox);").fetchall()
        existing_columns = {str(row["name"]) for row in rows}

        # Add required columns that may be missing in older schemas.
        required_additions = {
            "provider": "TEXT",
            "event_type": "TEXT",
            "payload_json": "TEXT",
            "received_at_utc": "TEXT",
        }
        for column_name, column_type in required_additions.items():
            if column_name not in existing_columns:
                conn.execute(
                    f"ALTER TABLE webhook_inbox ADD COLUMN {column_name} {column_type};",
                )
                existing_columns.add(column_name)

        # Map legacy timestamp column if present.
        if "received_at" in existing_columns:
            conn.execute(
                """
                UPDATE webhook_inbox
                SET received_at_utc = received_at
                WHERE received_at_utc IS NULL AND received_at IS NOT NULL;
                """
            )

        # Backfill required values where null to keep the table consistent.
        conn.execute(
            """
            UPDATE webhook_inbox
            SET provider = 'openphone'
            WHERE provider IS NULL OR provider = '';
            """
        )
        conn.execute(
            """
            UPDATE webhook_inbox
            SET event_type = 'sms'
            WHERE event_type IS NULL OR event_type = '';
            """
        )
        conn.execute(
            """
            UPDATE webhook_inbox
            SET payload_json = '{}'
            WHERE payload_json IS NULL OR payload_json = '';
            """
        )
        conn.execute(
            """
            UPDATE webhook_inbox
            SET received_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE received_at_utc IS NULL OR received_at_utc = '';
            """
        )

        # Add optional columns introduced by newer webhook receiver versions.
        optional_columns = {
            "message_id": "TEXT",
            "conversation_id": "TEXT",
            "phone_number_id": "TEXT",
            "created_at_utc": "TEXT",
        }
        for column_name, column_type in optional_columns.items():
            if column_name not in existing_columns:
                conn.execute(
                    f"ALTER TABLE webhook_inbox ADD COLUMN {column_name} {column_type};",
                )
                existing_columns.add(column_name)

        required_columns = {"provider", "event_type", "payload_json", "received_at_utc"}
        missing_required = sorted(required_columns - existing_columns)
        if missing_required:
            raise RuntimeError(
                "webhook_inbox is missing required columns for persistence: "
                + ", ".join(missing_required),
            )

    def insert_new_message_event(
        self,
        payload: Dict[str, Any],
        new_message: WebhookNewMessage,
        received_at_utc: Optional[str] = None,
    ) -> int:
        """
        Insert one message event row into webhook_inbox.
        Incoming `message.received` is normalized to `sms`
        for compatibility with legacy schemas.
        Returns inserted row id.
        """
        received_at = received_at_utc or datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, ensure_ascii=False)
        event_type_for_storage = self._normalize_event_type_for_storage(
            payload.get("type"),
        )

        with self.connection_factory.connect() as conn:
            rows = conn.execute("PRAGMA table_info(webhook_inbox);").fetchall()
            existing_columns = {str(row["name"]) for row in rows}

            values_by_column: dict[str, Any] = {
                "provider": "openphone",
                "source": "openphone",
                "event_type": event_type_for_storage,
                "message_id": new_message.id or None,
                "conversation_id": new_message.conversationId,
                "phone_number_id": new_message.phoneNumberId,
                "payload_json": payload_json,
                "raw_payload": payload_json,
                "received_at_utc": received_at,
                "received_at": received_at,
            }
            column_order = [
                "provider",
                "source",
                "event_type",
                "message_id",
                "conversation_id",
                "phone_number_id",
                "payload_json",
                "raw_payload",
                "received_at_utc",
                "received_at",
            ]
            insert_columns = [name for name in column_order if name in existing_columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            insert_sql = f"""
                INSERT INTO webhook_inbox ({", ".join(insert_columns)})
                VALUES ({placeholders});
                """
            insert_values = tuple(values_by_column[name] for name in insert_columns)

            cursor = conn.execute(
                insert_sql,
                insert_values,
            )
            return int(cursor.lastrowid)
