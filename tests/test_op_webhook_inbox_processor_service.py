import json
import os
import sqlite3
import tempfile
import unittest

from services.database import DatabaseDialect, DatabaseSettings, SQLiteConnectionFactory
from services.op_webhook_inbox_processor_service import OpenPhoneWebhookInboxProcessorService


class TestOpenPhoneWebhookInboxProcessorService(unittest.TestCase):
    def setUp(self) -> None:
        fd, self._db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self._create_schema(self._db_path)

        settings = DatabaseSettings(
            url=f"sqlite:///{self._db_path.replace(os.sep, '/')}",
            dialect=DatabaseDialect.SQLITE,
            sqlite_path=self._db_path,
        )
        self.connection_factory = SQLiteConnectionFactory(settings=settings)
        self.service = OpenPhoneWebhookInboxProcessorService(
            connection_factory=self.connection_factory,
        )

    def tearDown(self) -> None:
        if os.path.exists(self._db_path):
            os.remove(self._db_path)

    def _create_schema(self, db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE webhook_inbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL DEFAULT 'openphone',
                    event_type TEXT NOT NULL DEFAULT 'sms',
                    raw_payload TEXT NOT NULL,
                    received_at DATETIME NOT NULL DEFAULT (datetime('now')),
                    status TEXT NOT NULL DEFAULT 'unprocessed',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempted_at DATETIME,
                    processed_at DATETIME,
                    error_message TEXT,
                    processed_table TEXT,
                    processed_row_id TEXT,
                    received_at_utc TEXT,
                    provider TEXT,
                    payload_json TEXT,
                    message_id TEXT,
                    conversation_id TEXT,
                    phone_number_id TEXT,
                    created_at_utc TEXT
                );

                CREATE TABLE openphone_sms_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    openphone_sms_id TEXT UNIQUE,
                    guest_id INTEGER,
                    guest_phone TEXT NOT NULL,
                    our_phone TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('inbound', 'outbound')),
                    body TEXT NOT NULL,
                    sent_at DATETIME NOT NULL,
                    openphone_phone_number_id TEXT,
                    openphone_user_id TEXT,
                    status TEXT,
                    updated_at DATETIME
                );

                CREATE TABLE openphone_phone_numbers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    openphone_number_id TEXT UNIQUE NOT NULL,
                    phone_number TEXT NOT NULL,
                    label TEXT,
                    property_id INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE guests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    primary_phone TEXT,
                    is_current INTEGER DEFAULT 1
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _insert_inbox_row(self, *, event_type: str, payload: dict | None, raw_payload: str | None = None) -> int:
        raw_json = raw_payload if raw_payload is not None else json.dumps(payload or {})
        payload_json = json.dumps(payload) if payload is not None else None
        message_id = None
        phone_number_id = None
        if payload:
            obj = payload.get("data", {}).get("object", {})
            message_id = obj.get("id")
            phone_number_id = obj.get("phoneNumberId")

        with self.connection_factory.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO webhook_inbox (
                    source,
                    event_type,
                    raw_payload,
                    payload_json,
                    status,
                    attempts,
                    message_id,
                    phone_number_id
                )
                VALUES (?, ?, ?, ?, 'unprocessed', 0, ?, ?);
                """,
                (
                    "openphone",
                    event_type,
                    raw_json,
                    payload_json,
                    message_id,
                    phone_number_id,
                ),
            )
            return int(cur.lastrowid)

    def test_sms_event_is_processed_to_sms_and_phone_number_tables(self) -> None:
        with self.connection_factory.connect() as conn:
            guest_id = int(
                conn.execute(
                    """
                    INSERT INTO guests (primary_phone, is_current)
                    VALUES (?, ?);
                    """,
                    ("+15550000000", 1),
                ).lastrowid
            )

        payload = {
            "type": "message.received",
            "data": {
                "object": {
                    "id": "MSG_PROCESS_1",
                    "conversationId": "CN_PROCESS_1",
                    "phoneNumberId": "PN_PROCESS_1",
                    "userId": "US_PROCESS_1",
                    "from": "+15550000000",
                    "to": ["+15108227060"],
                    "direction": "incoming",
                    "text": "hello from inbox",
                    "status": "delivered",
                    "createdAt": "2026-02-24T12:00:00Z",
                    "updatedAt": "2026-02-24T12:00:05Z",
                }
            },
        }
        inbox_id = self._insert_inbox_row(event_type="sms", payload=payload)

        summary = self.service.process_unprocessed(limit=20, source="openphone").to_dict()
        self.assertEqual(summary["scanned"], 1)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(summary["skipped"], 0)

        with self.connection_factory.connect() as conn:
            inbox_row = conn.execute(
                "SELECT status, processed_table, processed_row_id, attempts, error_message FROM webhook_inbox WHERE id = ?",
                (inbox_id,),
            ).fetchone()
            self.assertEqual(inbox_row["status"], "processed")
            self.assertEqual(inbox_row["processed_table"], "openphone_sms_messages")
            self.assertIsNotNone(inbox_row["processed_row_id"])
            self.assertEqual(inbox_row["attempts"], 0)
            self.assertIsNone(inbox_row["error_message"])

            sms_row = conn.execute(
                """
                SELECT openphone_sms_id, guest_id, guest_phone, our_phone, direction, body,
                       openphone_phone_number_id, openphone_user_id, status, updated_at
                FROM openphone_sms_messages
                WHERE openphone_sms_id = ?;
                """,
                ("MSG_PROCESS_1",),
            ).fetchone()
            self.assertIsNotNone(sms_row)
            self.assertEqual(sms_row["guest_id"], guest_id)
            self.assertEqual(sms_row["guest_phone"], "+15550000000")
            self.assertEqual(sms_row["our_phone"], "+15108227060")
            self.assertEqual(sms_row["direction"], "inbound")
            self.assertEqual(sms_row["body"], "hello from inbox")
            self.assertEqual(sms_row["openphone_phone_number_id"], "PN_PROCESS_1")
            self.assertEqual(sms_row["openphone_user_id"], "US_PROCESS_1")
            self.assertEqual(sms_row["status"], "delivered")

            pn_row = conn.execute(
                """
                SELECT openphone_number_id, phone_number
                FROM openphone_phone_numbers
                WHERE openphone_number_id = ?;
                """,
                ("PN_PROCESS_1",),
            ).fetchone()
            self.assertIsNotNone(pn_row)
            self.assertEqual(pn_row["phone_number"], "+15108227060")

    def test_sms_event_type_is_supported_and_normalized_for_outbound_direction(self) -> None:
        payload = {
            "type": "message.received",
            "data": {
                "object": {
                    "id": "MSG_PROCESS_2",
                    "conversationId": "CN_PROCESS_2",
                    "phoneNumberId": "PN_PROCESS_2",
                    "from": "+15108227060",
                    "to": ["+15551112222"],
                    "direction": "outgoing",
                    "text": "outbound test",
                    "createdAt": "2026-02-24T13:00:00Z",
                }
            },
        }
        inbox_id = self._insert_inbox_row(event_type="sms", payload=payload)

        summary = self.service.process_unprocessed(limit=20, source="openphone").to_dict()
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["failed"], 0)

        with self.connection_factory.connect() as conn:
            inbox_row = conn.execute(
                "SELECT status FROM webhook_inbox WHERE id = ?",
                (inbox_id,),
            ).fetchone()
            self.assertEqual(inbox_row["status"], "processed")

            sms_row = conn.execute(
                """
                SELECT guest_phone, our_phone, direction
                FROM openphone_sms_messages
                WHERE openphone_sms_id = ?;
                """,
                ("MSG_PROCESS_2",),
            ).fetchone()
            self.assertEqual(sms_row["guest_phone"], "+15551112222")
            self.assertEqual(sms_row["our_phone"], "+15108227060")
            self.assertEqual(sms_row["direction"], "outbound")

    def test_invalid_payload_marks_row_failed_and_increments_attempts(self) -> None:
        inbox_id = self._insert_inbox_row(
            event_type="sms",
            payload=None,
            raw_payload="not-json",
        )

        summary = self.service.process_unprocessed(limit=20, source="openphone").to_dict()
        self.assertEqual(summary["scanned"], 1)
        self.assertEqual(summary["processed"], 0)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["skipped"], 0)

        with self.connection_factory.connect() as conn:
            inbox_row = conn.execute(
                "SELECT status, attempts, error_message FROM webhook_inbox WHERE id = ?",
                (inbox_id,),
            ).fetchone()
            self.assertEqual(inbox_row["status"], "failed")
            self.assertEqual(inbox_row["attempts"], 1)
            self.assertIn("Payload is not valid JSON", inbox_row["error_message"])

    def test_phone_number_is_not_inserted_when_number_already_exists(self) -> None:
        with self.connection_factory.connect() as conn:
            conn.execute(
                """
                INSERT INTO openphone_phone_numbers (
                    openphone_number_id,
                    phone_number,
                    label
                )
                VALUES (?, ?, ?);
                """,
                ("PN_EXISTING", "+15108227060", "existing"),
            )

        payload = {
            "type": "message.received",
            "data": {
                "object": {
                    "id": "MSG_PROCESS_3",
                    "conversationId": "CN_PROCESS_3",
                    "phoneNumberId": "PN_NEW_SHOULD_NOT_INSERT",
                    "from": "+15559990000",
                    "to": ["+15108227060"],
                    "direction": "incoming",
                    "text": "duplicate phone number test",
                    "createdAt": "2026-02-24T14:00:00Z",
                }
            },
        }
        self._insert_inbox_row(event_type="sms", payload=payload)
        summary = self.service.process_unprocessed(limit=20, source="openphone").to_dict()
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["failed"], 0)

        with self.connection_factory.connect() as conn:
            total_numbers = conn.execute(
                "SELECT COUNT(*) FROM openphone_phone_numbers;"
            ).fetchone()[0]
            self.assertEqual(total_numbers, 1)

            only_row = conn.execute(
                """
                SELECT openphone_number_id, phone_number
                FROM openphone_phone_numbers
                LIMIT 1;
                """
            ).fetchone()
            self.assertEqual(only_row["openphone_number_id"], "PN_EXISTING")
            self.assertEqual(only_row["phone_number"], "+15108227060")

    def test_guest_id_prefers_current_guest_for_matching_phone(self) -> None:
        with self.connection_factory.connect() as conn:
            old_guest_id = int(
                conn.execute(
                    """
                    INSERT INTO guests (primary_phone, is_current)
                    VALUES (?, ?);
                    """,
                    ("+17145558834", 0),
                ).lastrowid
            )
            current_guest_id = int(
                conn.execute(
                    """
                    INSERT INTO guests (primary_phone, is_current)
                    VALUES (?, ?);
                    """,
                    ("+17145558834", 1),
                ).lastrowid
            )

        payload = {
            "type": "message.received",
            "data": {
                "object": {
                    "id": "MSG_PROCESS_4",
                    "conversationId": "CN_PROCESS_4",
                    "phoneNumberId": "PN_PROCESS_4",
                    "from": "+17145558834",
                    "to": ["+15108227060"],
                    "direction": "incoming",
                    "text": "guest match test",
                    "createdAt": "2026-02-24T14:30:00Z",
                }
            },
        }
        self._insert_inbox_row(event_type="sms", payload=payload)

        summary = self.service.process_unprocessed(limit=20, source="openphone").to_dict()
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["failed"], 0)

        with self.connection_factory.connect() as conn:
            sms_row = conn.execute(
                """
                SELECT guest_id, guest_phone
                FROM openphone_sms_messages
                WHERE openphone_sms_id = ?;
                """,
                ("MSG_PROCESS_4",),
            ).fetchone()
            self.assertIsNotNone(sms_row)
            self.assertEqual(sms_row["guest_phone"], "+17145558834")
            self.assertEqual(sms_row["guest_id"], current_guest_id)
            self.assertNotEqual(sms_row["guest_id"], old_guest_id)


if __name__ == "__main__":
    unittest.main()
