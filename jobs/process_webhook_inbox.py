"""
Process unprocessed webhook_inbox rows into OpenPhone destination tables.

Example:
  python -m jobs.process_webhook_inbox --limit 100
"""

from __future__ import annotations

import argparse
import json
import logging

from dotenv import load_dotenv

from services.database import DatabaseConfigError, build_connection_factory_from_env
from services.op_webhook_inbox_processor_service import OpenPhoneWebhookInboxProcessorService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process webhook_inbox rows into OpenPhone destination tables.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of unprocessed rows to process in one run (default: 100).",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="openphone",
        help="webhook_inbox source filter (default: openphone).",
    )
    return parser


def run_cli() -> dict[str, int]:
    load_dotenv()
    parser = _build_parser()
    args = parser.parse_args()

    if args.limit <= 0:
        raise RuntimeError("--limit must be greater than zero.")

    try:
        connection_factory = build_connection_factory_from_env(require_config=True)
    except DatabaseConfigError as exc:
        raise RuntimeError(
            "Database config is required. Set DATABASE_URL (or OLJ_DB_PATH).",
        ) from exc

    processor = OpenPhoneWebhookInboxProcessorService(connection_factory=connection_factory)
    summary = processor.process_unprocessed(limit=args.limit, source=args.source)
    result = summary.to_dict()

    logger.info(
        "Webhook inbox processing completed (source=%s, scanned=%s, processed=%s, failed=%s, skipped=%s).",
        args.source,
        result["scanned"],
        result["processed"],
        result["failed"],
        result["skipped"],
    )
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    run_cli()
