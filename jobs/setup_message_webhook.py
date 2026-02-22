"""
Set up an OpenPhone webhook for inbound messages.

This script ensures a webhook exists for:
  - event: message.received
  - url:   <OPENPHONE_WEBHOOK_BASE_URL>/op_new_message
"""

import os
import asyncio
import argparse
import logging
from typing import List, Optional

from dotenv import load_dotenv

from services.openphone_webhook_service import OpenPhoneWebhookService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

WEBHOOK_PATH = "op_new_message"


def _build_webhook_url(base_url: str, path: str = WEBHOOK_PATH) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _parse_resource_ids(raw_value: Optional[str]) -> Optional[List[str]]:
    if not raw_value:
        return None
    resource_ids = [item.strip() for item in raw_value.split(",") if item.strip()]
    return resource_ids or None


async def ensure_message_received_webhook(
    api_key: str,
    webhook_url: str,
    label: Optional[str] = None,
    user_id: Optional[str] = None,
    resource_ids: Optional[List[str]] = None,
) -> dict:
    """
    Reuse an existing webhook if already configured for this URL + event;
    otherwise create it.
    """
    async with OpenPhoneWebhookService(api_key=api_key) as service:
        webhook = await service.ensure_message_received_webhook(
            webhook_url=webhook_url,
            label=label,
            user_id=user_id,
            resource_ids=resource_ids,
        )
        logger.info(
            "Webhook ready: id=%s status=%s url=%s events=%s",
            webhook.get("id"),
            webhook.get("status"),
            webhook.get("url"),
            webhook.get("events"),
        )
        return webhook


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Create/reuse OpenPhone webhook for message.received events.",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=os.environ.get("OPENPHONE_WEBHOOK_BASE_URL"),
        help=(
            "Public base URL for your webhook receiver, e.g. https://example.com "
            "(default: OPENPHONE_WEBHOOK_BASE_URL env var)"
        ),
    )
    parser.add_argument(
        "--label",
        type=str,
        default="op_new_message",
        help="Webhook label (default: op_new_message)",
    )
    parser.add_argument(
        "--user-id",
        type=str,
        default=None,
        help="Optional OpenPhone userId for webhook ownership.",
    )
    parser.add_argument(
        "--resource-ids",
        type=str,
        default=None,
        help='Optional CSV of phone number IDs (PN...). Use "*" for all numbers.',
    )
    args = parser.parse_args()

    api_key = os.environ.get("OPENPHONE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENPHONE_API_KEY environment variable is not set. Add it to your .env file.",
        )

    if not args.base_url:
        raise RuntimeError(
            "Missing base URL. Set OPENPHONE_WEBHOOK_BASE_URL or pass --base-url.",
        )

    webhook_url = _build_webhook_url(args.base_url, WEBHOOK_PATH)
    resource_ids = _parse_resource_ids(args.resource_ids)
    webhook = asyncio.run(
        ensure_message_received_webhook(
            api_key=api_key,
            webhook_url=webhook_url,
            label=args.label,
            user_id=args.user_id,
            resource_ids=resource_ids,
        )
    )

    logger.info("Ready: webhookId=%s", webhook.get("id"))
