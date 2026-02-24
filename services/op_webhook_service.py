from typing import List, Optional

from constants.op_webhook_constants import DEFAULT_CALL_WEBHOOK_EVENTS
from models.webhook_new_message import WebhookNewMessage
from services.op_service import OpenPhoneService


class OpenPhoneWebhookService(OpenPhoneService):
    """
    Webhook-focused service for OpenPhone.

    Responsibilities:
      - Manage webhook configuration via OpenPhone API (GET/POST webhooks)
      - Parse inbound webhook payloads for new message events
    """

    async def list_webhooks(self, user_id: Optional[str] = None) -> List[dict]:
        """
        Fetch all webhooks in the workspace.
        Optionally filter by userId.
        """
        params = {"userId": user_id} if user_id else None
        data = await self._request("GET", "webhooks", params=params)
        return data.get("data", [])

    async def delete_webhook_by_id(self, webhook_id: str) -> None:
        """
        Delete a webhook by ID.
        """
        await self._request("DELETE", f"webhooks/{webhook_id}")

    async def list_webhooks_by_url(
        self,
        webhook_url: str,
        user_id: Optional[str] = None,
        webhook_type: Optional[str] = None,
    ) -> List[dict]:
        """
        List webhooks matching a URL, optionally filtered by logical type.
        """
        webhooks = await self.list_webhooks(user_id=user_id)
        matched: List[dict] = []

        for webhook in webhooks:
            if webhook.get("url") != webhook_url:
                continue

            if webhook_type:
                events = webhook.get("events") or []
                if webhook_type == "message":
                    if "message.received" not in events:
                        continue
                elif webhook_type == "calls":
                    if not any(str(event).startswith("call.") for event in events):
                        continue

            matched.append(webhook)

        return matched

    async def delete_webhooks_by_url(
        self,
        webhook_url: str,
        user_id: Optional[str] = None,
        webhook_type: Optional[str] = None,
    ) -> List[str]:
        """
        Delete all webhooks matching a URL (optionally filtered by type).
        Returns deleted webhook IDs.
        """
        matching_webhooks = await self.list_webhooks_by_url(
            webhook_url=webhook_url,
            user_id=user_id,
            webhook_type=webhook_type,
        )
        deleted_ids: List[str] = []
        for webhook in matching_webhooks:
            webhook_id = webhook.get("id")
            if not webhook_id:
                continue
            await self.delete_webhook_by_id(webhook_id)
            deleted_ids.append(webhook_id)

        return deleted_ids

    async def find_message_received_webhook_by_url(
        self,
        webhook_url: str,
        user_id: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Find an existing webhook configured for only message.received at the given URL.
        """
        webhooks = await self.list_webhooks(user_id=user_id)
        for webhook in webhooks:
            events = webhook.get("events") or []
            if webhook.get("url") == webhook_url and events == ["message.received"]:
                return webhook
        return None

    async def create_message_received_webhook(
        self,
        webhook_url: str,
        label: Optional[str] = None,
        user_id: Optional[str] = None,
        resource_ids: Optional[List[str]] = None,
        status: str = "enabled",
    ) -> dict:
        """
        Create a webhook subscribed to only message.received events.
        """
        payload = {
            "events": ["message.received"],
            "url": webhook_url,
            "status": status,
        }
        if label:
            payload["label"] = label
        if user_id:
            payload["userId"] = user_id
        if resource_ids:
            payload["resourceIds"] = resource_ids

        data = await self._request("POST", "webhooks/messages", json_body=payload)
        return data.get("data", {})

    async def ensure_message_received_webhook(
        self,
        webhook_url: str,
        label: Optional[str] = None,
        user_id: Optional[str] = None,
        resource_ids: Optional[List[str]] = None,
    ) -> dict:
        """
        Reuse an existing message.received webhook if present, otherwise create it.
        """
        existing = await self.find_message_received_webhook_by_url(
            webhook_url=webhook_url,
            user_id=user_id,
        )
        if existing:
            return existing

        # TODO: Alert a Discord channel when webhook health check fails and we
        # need to recreate the webhook.
        # TODO: Alternative approach: automate secret rotation by saving the new
        # webhook signing key to a password/secret manager (for example
        # 1Password Connect, HashiCorp Vault, AWS Secrets Manager, or Doppler).
        return await self.create_message_received_webhook(
            webhook_url=webhook_url,
            label=label,
            user_id=user_id,
            resource_ids=resource_ids,
        )

    async def find_calls_webhook_by_url(
        self,
        webhook_url: str,
        events: Optional[List[str]] = None,
        user_id: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Find an existing call webhook at the given URL with the requested events.
        """
        target_events = sorted(events or list(DEFAULT_CALL_WEBHOOK_EVENTS))
        webhooks = await self.list_webhooks(user_id=user_id)
        for webhook in webhooks:
            webhook_events = webhook.get("events") or []
            if webhook.get("url") == webhook_url and sorted(webhook_events) == target_events:
                return webhook
        return None

    async def create_calls_webhook(
        self,
        webhook_url: str,
        events: Optional[List[str]] = None,
        label: Optional[str] = None,
        user_id: Optional[str] = None,
        resource_ids: Optional[List[str]] = None,
        status: str = "enabled",
    ) -> dict:
        """
        Create a webhook subscribed to call-related events.
        """
        payload = {
            "events": events or list(DEFAULT_CALL_WEBHOOK_EVENTS),
            "url": webhook_url,
            "status": status,
        }
        if label:
            payload["label"] = label
        if user_id:
            payload["userId"] = user_id
        if resource_ids:
            payload["resourceIds"] = resource_ids

        data = await self._request("POST", "webhooks/calls", json_body=payload)
        return data.get("data", {})

    async def ensure_calls_webhook(
        self,
        webhook_url: str,
        events: Optional[List[str]] = None,
        label: Optional[str] = None,
        user_id: Optional[str] = None,
        resource_ids: Optional[List[str]] = None,
    ) -> dict:
        """
        Reuse an existing call webhook if present, otherwise create it.
        """
        existing = await self.find_calls_webhook_by_url(
            webhook_url=webhook_url,
            events=events,
            user_id=user_id,
        )
        if existing:
            return existing

        return await self.create_calls_webhook(
            webhook_url=webhook_url,
            events=events,
            label=label,
            user_id=user_id,
            resource_ids=resource_ids,
        )

    @staticmethod
    def parse_new_message_event(payload: dict) -> Optional[WebhookNewMessage]:
        """
        Parse inbound payload as message.received webhook event.
        Returns None for unsupported event types or malformed payloads.
        """
        if not isinstance(payload, dict):
            return None

        event_type = payload.get("type")
        if event_type != "message.received":
            return None

        data = payload.get("data")
        obj = data.get("object") if isinstance(data, dict) else None
        if not isinstance(obj, dict):
            return None

        return WebhookNewMessage.from_dict(payload)
