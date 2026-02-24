"""
Set up OpenPhone webhooks for messages or calls.

Examples:
  python -m jobs.setup_webhook --type message
  python -m jobs.setup_webhook --type calls --base-url https://example.ngrok-free.app
"""

import argparse
import asyncio
import json
import logging
import os
from typing import List, Optional

from dotenv import load_dotenv

from constants.op_webhook_constants import (
    DEFAULT_CALL_WEBHOOK_EVENTS,
    NEW_CALLS_WEBHOOK_PATH,
    NEW_MESSAGE_WEBHOOK_PATH,
)
from services.op_webhook_service import OpenPhoneWebhookService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MESSAGE_WEBHOOK_EVENT = "message.received"

_WEBHOOK_TYPE_ALIASES = {
    "message": "message",
    "messages": "message",
    "sms": "message",
    "call": "calls",
    "calls": "calls",
}

_DEFAULT_PATH_BY_TYPE = {
    "message": NEW_MESSAGE_WEBHOOK_PATH,
    "calls": NEW_CALLS_WEBHOOK_PATH,
}


def _normalize_webhook_type(raw_value: str) -> str:
    webhook_type = _WEBHOOK_TYPE_ALIASES.get((raw_value or "").strip().lower())
    if not webhook_type:
        raise ValueError(f"Unsupported webhook type: {raw_value}")
    return webhook_type


def _build_webhook_url(base_url: str, path: str) -> str:
    normalized_base_url = base_url.strip()
    if not normalized_base_url.startswith(("http://", "https://")):
        normalized_base_url = f"https://{normalized_base_url}"
    return f"{normalized_base_url.rstrip('/')}/{path.lstrip('/')}"


def _parse_csv(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_resource_ids(raw_value: Optional[str]) -> Optional[List[str]]:
    resource_ids = _parse_csv(raw_value)
    return resource_ids or None


def _resolve_events(webhook_type: str, raw_events: Optional[str]) -> List[str]:
    parsed_events = _parse_csv(raw_events)

    if webhook_type == "message":
        if parsed_events and parsed_events != [MESSAGE_WEBHOOK_EVENT]:
            raise RuntimeError(
                "For webhook type 'message', only 'message.received' is supported.",
            )
        return [MESSAGE_WEBHOOK_EVENT]

    if not parsed_events:
        return list(DEFAULT_CALL_WEBHOOK_EVENTS)

    deduped_events: List[str] = []
    for event_name in parsed_events:
        if event_name not in deduped_events:
            deduped_events.append(event_name)
    return deduped_events


async def ensure_webhook(
    api_key: str,
    webhook_type: str,
    webhook_url: str,
    label: Optional[str] = None,
    user_id: Optional[str] = None,
    resource_ids: Optional[List[str]] = None,
    events: Optional[List[str]] = None,
    delete_existing: bool = False,
    delete_only: bool = False,
) -> dict:
    """
    Ensure a message or call webhook exists for the requested URL and events.
    """
    normalized_type = _normalize_webhook_type(webhook_type)
    async with OpenPhoneWebhookService(api_key=api_key) as service:
        deleted_ids: List[str] = []
        if delete_existing or delete_only:
            deleted_ids = await service.delete_webhooks_by_url(
                webhook_url=webhook_url,
                user_id=user_id,
                webhook_type=normalized_type,
            )
            logger.info(
                "Deleted %d existing %s webhook(s) for url=%s.",
                len(deleted_ids),
                normalized_type,
                webhook_url,
            )
            if delete_only:
                return {
                    "action": "delete_only",
                    "type": normalized_type,
                    "url": webhook_url,
                    "deletedIds": deleted_ids,
                    "deletedCount": len(deleted_ids),
                }

        if normalized_type == "message":
            webhook = await service.ensure_message_received_webhook(
                webhook_url=webhook_url,
                label=label,
                user_id=user_id,
                resource_ids=resource_ids,
            )
        else:
            webhook = await service.ensure_calls_webhook(
                webhook_url=webhook_url,
                events=events,
                label=label,
                user_id=user_id,
                resource_ids=resource_ids,
            )

    logger.info(
        "%s webhook ensure completed successfully (status=%s).",
        normalized_type,
        webhook.get("status"),
    )
    return webhook


def _build_parser(default_webhook_type: Optional[str] = None) -> argparse.ArgumentParser:
    normalized_default_type = (
        _normalize_webhook_type(default_webhook_type) if default_webhook_type else None
    )
    if normalized_default_type == "message":
        description = "Create/reuse OpenPhone webhook for message.received events."
    elif normalized_default_type == "calls":
        description = "Create/reuse OpenPhone webhook for call events."
    else:
        description = "Create/reuse OpenPhone webhook for message or call events."

    default_path_hint = (
        _DEFAULT_PATH_BY_TYPE[normalized_default_type]
        if normalized_default_type
        else "depends on --type"
    )
    if normalized_default_type == "calls":
        events_hint = (
            "Comma-separated call events "
            f"(default: {','.join(DEFAULT_CALL_WEBHOOK_EVENTS)})."
        )
    elif normalized_default_type == "message":
        events_hint = "Only message.received is supported."
    else:
        events_hint = (
            "Comma-separated webhook events. "
            "For message type, only message.received is supported."
        )

    parser = argparse.ArgumentParser(
        description=description,
    )
    if normalized_default_type:
        parser.set_defaults(webhook_type=normalized_default_type)
    else:
        parser.add_argument(
            "--type",
            type=str,
            required=True,
            choices=sorted(_WEBHOOK_TYPE_ALIASES.keys()),
            help="Webhook type: message/messages/sms or call/calls.",
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
        "--path",
        type=str,
        default=None,
        help=f"Endpoint path to append to base URL (default: {default_path_hint}).",
    )
    parser.add_argument(
        "--label",
        type=str,
        default=None,
        help="Webhook label (default: derived from path).",
    )
    parser.add_argument(
        "--events",
        type=str,
        default=None,
        help=events_hint,
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
    delete_group = parser.add_mutually_exclusive_group()
    delete_group.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete matching webhook(s) for this URL before create/reuse.",
    )
    delete_group.add_argument(
        "--delete-only",
        action="store_true",
        help="Delete matching webhook(s) for this URL and exit without creating.",
    )
    return parser


def run_cli(default_webhook_type: Optional[str] = None) -> dict:
    load_dotenv()
    parser = _build_parser(default_webhook_type=default_webhook_type)
    args = parser.parse_args()

    raw_webhook_type = (
        default_webhook_type
        or getattr(args, "webhook_type", None)
        or getattr(args, "type", None)
    )
    webhook_type = _normalize_webhook_type(raw_webhook_type)
    api_key = os.environ.get("OPENPHONE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENPHONE_API_KEY environment variable is not set. Add it to your .env file.",
        )

    if not args.base_url:
        raise RuntimeError(
            "Missing base URL. Set OPENPHONE_WEBHOOK_BASE_URL or pass --base-url.",
        )

    path = args.path or _DEFAULT_PATH_BY_TYPE[webhook_type]
    label = args.label or path.lstrip("/")
    events = None if args.delete_only else _resolve_events(webhook_type, args.events)
    resource_ids = _parse_resource_ids(args.resource_ids)
    webhook_url = _build_webhook_url(args.base_url, path)

    webhook = asyncio.run(
        ensure_webhook(
            api_key=api_key,
            webhook_type=webhook_type,
            webhook_url=webhook_url,
            label=label,
            user_id=args.user_id,
            resource_ids=resource_ids,
            events=events,
            delete_existing=args.delete_existing,
            delete_only=args.delete_only,
        )
    )

    if args.delete_only:
        logger.info(
            "%s webhook delete-only completed (deleted=%s, url=%s).",
            webhook_type,
            webhook.get("deletedCount"),
            webhook.get("url"),
        )
    else:
        logger.info(
            "%s webhook setup completed (id=%s, url=%s).",
            webhook_type,
            webhook.get("id"),
            webhook.get("url"),
        )
    print(json.dumps(webhook, indent=2, default=str))
    return webhook


if __name__ == "__main__":
    run_cli()
