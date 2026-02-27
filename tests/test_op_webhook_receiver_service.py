import os
import sqlite3
import tempfile
import unittest

from models.webhook_new_message import WebhookNewMessage
from services.database import DatabaseDialect, DatabaseSettings, SQLiteConnectionFactory
from services.op_webhook_receiver_service import OpenPhoneWebhookPersistenceService


class TestOpenPhoneWebhookPersistenceService(unittest.TestCase):
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
        self.service = OpenPhoneWebhookPersistenceService(connection_factory=self.connection_factory)

    def tearDown(self) -> None:
        if os.path.exists(self._db_path):
            os.remove(self._db_path)

    @staticmethod
    def _create_schema(db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE webhook_inbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT,
                    source TEXT,
                    event_type TEXT NOT NULL CHECK(event_type IN (
                        'sms', 'call', 'voicemail',
                        'reservation_created', 'reservation_updated',
                        'reservation_cancelled', 'new_message'
                    )),
                    message_id TEXT,
                    conversation_id TEXT,
                    phone_number_id TEXT,
                    payload_json TEXT NOT NULL,
                    raw_payload TEXT,
                    received_at_utc TEXT,
                    created_at_utc TEXT,
                    received_at TEXT
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def test_insert_call_completed_event_maps_event_type_to_call(self) -> None:
        payload = {
            "type": "call.completed",
            "data": {
                "object": {
                    "id": "CA_TEST_100",
                    "conversationId": "CN_TEST_100",
                    "phoneNumberId": "PN_TEST_100",
                }
            },
        }
        row_id = self.service.insert_call_completed_event(
            payload=payload,
            call_id="CA_TEST_100",
            conversation_id="CN_TEST_100",
            phone_number_id="PN_TEST_100",
        )

        with self.connection_factory.connect() as conn:
            row = conn.execute(
                """
                SELECT event_type, message_id, conversation_id, phone_number_id, created_at_utc
                FROM webhook_inbox
                WHERE id = ?;
                """,
                (row_id,),
            ).fetchone()
            self.assertEqual(row["event_type"], "call")
            self.assertEqual(row["message_id"], "CA_TEST_100")
            self.assertEqual(row["conversation_id"], "CN_TEST_100")
            self.assertEqual(row["phone_number_id"], "PN_TEST_100")
            self.assertIsNotNone(row["created_at_utc"])
            self.assertNotEqual(str(row["created_at_utc"]).strip(), "")

    def test_insert_new_message_event_maps_event_type_to_sms(self) -> None:
        payload = {
            "type": "message.received",
            "data": {
                "object": {
                    "id": "MSG_TEST_100",
                    "conversationId": "CN_TEST_200",
                    "phoneNumberId": "PN_TEST_200",
                    "from": "+15550000000",
                    "to": ["+15550000001"],
                    "direction": "incoming",
                    "text": "hello",
                }
            },
        }
        new_message = WebhookNewMessage.from_dict(payload)
        row_id = self.service.insert_new_message_event(payload=payload, new_message=new_message)

        with self.connection_factory.connect() as conn:
            row = conn.execute(
                """
                SELECT event_type, message_id, created_at_utc
                FROM webhook_inbox
                WHERE id = ?;
                """,
                (row_id,),
            ).fetchone()
            self.assertEqual(row["event_type"], "sms")
            self.assertEqual(row["message_id"], "MSG_TEST_100")
            self.assertIsNotNone(row["created_at_utc"])
            self.assertNotEqual(str(row["created_at_utc"]).strip(), "")


if __name__ == "__main__":
    unittest.main()
