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

    def insert_new_message_event(
        self,
        payload: Dict[str, Any],
        new_message: WebhookNewMessage,
        received_at_utc: Optional[str] = None,
    ) -> int:
        """
        Insert one message.received event row into webhook_inbox.
        Returns inserted row id.
        """
        received_at = received_at_utc or datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, ensure_ascii=False)

        with self.connection_factory.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO webhook_inbox (
                    provider,
                    event_type,
                    message_id,
                    conversation_id,
                    phone_number_id,
                    payload_json,
                    received_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "openphone",
                    payload.get("type") or "message.received",
                    new_message.id or None,
                    new_message.conversationId,
                    new_message.phoneNumberId,
                    payload_json,
                    received_at,
                ),
            )
            return int(cursor.lastrowid)
