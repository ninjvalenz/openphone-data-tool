"""
OpenPhone Data Consolidation Tool
===================================
Fetches users, phone numbers, conversations, calls (with transcripts),
and messages from the OpenPhone API and writes a single consolidated JSON file.

Uses asyncio + aiohttp for maximum concurrency across all levels:
  - All users processed in parallel
  - Conversations fetched in one batched call per user (all phone numbers at once)
  - Calls fetched in parallel per conversation per participant
  - All transcript fetches in parallel
  - Messages fetched in parallel across all phone numbers

Any items that fail to fetch are collected into a separate "failed_items.json"
file with the parameters needed to retry them individually.
"""

import os
import json
import asyncio
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Optional
from dotenv import load_dotenv

from services.openphone_service import OpenPhoneService, OpenPhoneApiError
from models.user import User
from models.phone_number import PhoneNumber
from models.conversation import Conversation
from models.call import Call
from models.message import Message
from models.transcript import Transcript
from models.consolidated import UserPhoneData, ConsolidatedPhoneData

# ------------------------------------------------------------------ #
#  Logging
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.environ.get("OPENPHONE_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENPHONE_API_KEY environment variable is not set. Add it to your .env file.")


# ------------------------------------------------------------------ #
#  Failed items collector
# ------------------------------------------------------------------ #
class FailedItemsCollector:
    """
    Thread-safe collector for items that failed to fetch.
    Each category stores the parameters needed to retry the fetch.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self.phone_numbers: List[dict] = []    # failed to fetch phone numbers for a user
        self.conversations: List[dict] = []    # failed to fetch conversations
        self.calls: List[dict] = []            # failed to fetch calls
        self.messages: List[dict] = []         # failed to fetch messages
        self.transcripts: List[dict] = []      # failed to fetch transcripts

    async def add(self, category: str, item: dict) -> None:
        async with self._lock:
            getattr(self, category).append(item)

    def has_failures(self) -> bool:
        return any([
            self.phone_numbers,
            self.conversations,
            self.calls,
            self.messages,
            self.transcripts,
        ])

    def to_dict(self) -> dict:
        categories = {
            "phone_numbers": {
                "items": self.phone_numbers,
                "description": "Failed to fetch phone numbers for these users. Retry with: get_phonenumber_by_user(userId)",
            },
            "conversations": {
                "items": self.conversations,
                "description": "Failed to fetch conversations. Retry with: get_all_conversations(phoneNumberIds)",
            },
            "calls": {
                "items": self.calls,
                "description": "Failed to fetch calls. Retry with: get_all_calls_by_phonenumber(phoneNumberId, participants)",
            },
            "messages": {
                "items": self.messages,
                "description": "Failed to fetch messages. Retry with: get_all_messages_by_phonenumber(phoneNumberId, phoneNumber)",
            },
            "transcripts": {
                "items": self.transcripts,
                "description": "Failed to fetch transcripts. Retry with: get_all_transcripts_by_call(callId)",
            },
        }

        result = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "totalFailed": sum(len(c["items"]) for c in categories.values()),
        }

        # Only include categories that actually have failures
        for name, cat in categories.items():
            if cat["items"]:
                result[name] = {
                    "count": len(cat["items"]),
                    "description": cat["description"],
                    "items": cat["items"],
                }

        return result


# ------------------------------------------------------------------ #
#  Fetch helpers
# ------------------------------------------------------------------ #
async def _fetch_calls_and_transcripts(
    service: OpenPhoneService,
    conversations: List[Conversation],
    failed: FailedItemsCollector,
) -> List[dict]:
    """
    For a list of conversations, fetch all calls (parallel per conversation,
    parallel per participant within each), then fetch all transcripts in parallel.

    Returns a list of call dicts (with transcripts attached).
    """
    # Gather calls from all conversations in parallel
    call_tasks = []
    call_task_meta = []  # track which conversation each task belongs to
    for conv in conversations:
        if not conv.participants:
            logger.info("  Skipping conversation %s — no participants", conv.id)
            continue
        call_tasks.append(
            service.get_all_calls_by_phonenumber(conv.phoneNumberId, conv.participants)
        )
        call_task_meta.append(conv)

    all_calls: List[Call] = []
    if call_tasks:
        call_results = await asyncio.gather(*call_tasks, return_exceptions=True)
        seen_ids: set = set()
        for conv, result in zip(call_task_meta, call_results):
            if isinstance(result, Exception):
                logger.warning(
                    "Failed to fetch calls for phoneNumberId=%s, participants=%s: %s — skipping.",
                    conv.phoneNumberId, conv.participants, result,
                )
                await failed.add("calls", {
                    "phoneNumberId": conv.phoneNumberId,
                    "participants": conv.participants,
                    "conversationId": conv.id,
                    "error": str(result),
                })
                continue
            for call in result:
                if call.id not in seen_ids:
                    seen_ids.add(call.id)
                    all_calls.append(call)

    # Fetch all transcripts in parallel
    if all_calls:
        transcript_results = await asyncio.gather(
            *[service.get_all_transcripts_by_call(call.id) for call in all_calls],
            return_exceptions=True,
        )
    else:
        transcript_results = []

    # Build call dicts with transcripts attached
    call_dicts: List[dict] = []
    for call, transcript_result in zip(all_calls, transcript_results):
        call_dict = call.to_dict()
        if isinstance(transcript_result, Exception):
            logger.warning(
                "Failed to fetch transcript for call %s: %s — skipping.",
                call.id, transcript_result,
            )
            await failed.add("transcripts", {
                "callId": call.id,
                "error": str(transcript_result),
            })
        elif transcript_result is not None:
            call_dict["transcript"] = transcript_result.to_dict()
        call_dicts.append(call_dict)

    return call_dicts


async def _fetch_all_messages(
    service: OpenPhoneService,
    phone_numbers: List[PhoneNumber],
    failed: FailedItemsCollector,
) -> List[dict]:
    """Fetch messages for all phone numbers in parallel."""
    if not phone_numbers:
        return []

    results = await asyncio.gather(
        *[
            service.get_all_messages_by_phonenumber(pn.id, pn.number or "")
            for pn in phone_numbers
        ],
        return_exceptions=True,
    )

    all_messages: List[dict] = []
    for pn, result in zip(phone_numbers, results):
        if isinstance(result, Exception):
            logger.warning(
                "Failed to fetch messages for phoneNumberId=%s, phoneNumber=%s: %s — skipping.",
                pn.id, pn.number, result,
            )
            await failed.add("messages", {
                "phoneNumberId": pn.id,
                "phoneNumber": pn.number or "",
                "userId": pn.userId,
                "error": str(result),
            })
            continue
        all_messages.extend(m.to_dict() for m in result)

    return all_messages


async def _process_user(
    service: OpenPhoneService,
    user: User,
    failed: FailedItemsCollector,
) -> dict:
    """
    Process a single user:
      1. Fetch phone numbers
      2. In parallel:
         a. Fetch ALL conversations in one batched call (all phone number IDs at once)
            -> then calls per conversation (parallel) -> transcripts (parallel)
         b. Fetch messages for all phone numbers (parallel)
      3. Build UserPhoneData dict.
    """
    logger.info("Processing user: %s %s (%s)", user.firstName, user.lastName, user.id)

    # Step 1 — Phone numbers for this user
    try:
        phone_numbers: List[PhoneNumber] = await service.get_phonenumber_by_user(user.id)
    except OpenPhoneApiError as exc:
        logger.warning(
            "Failed to fetch phone numbers for user %s: %s — skipping user.",
            user.id, exc,
        )
        await failed.add("phone_numbers", {
            "userId": user.id,
            "userName": f"{user.firstName} {user.lastName}",
            "error": str(exc),
        })
        return UserPhoneData(user=user.to_dict()).to_dict()

    phone_number_dicts = [pn.to_dict() for pn in phone_numbers]
    phone_number_ids = [pn.id for pn in phone_numbers]

    if not phone_number_ids:
        logger.info("  No phone numbers for user %s", user.id)
        return UserPhoneData(user=user.to_dict()).to_dict()

    # Step 2 — Conversations (batched) + calls/transcripts  AND  messages in parallel
    async def _conversations_and_calls():
        try:
            conversations = await service.get_all_conversations(phone_number_ids)
        except OpenPhoneApiError as exc:
            logger.warning(
                "Failed to fetch conversations for phoneNumberIds=%s: %s — skipping.",
                phone_number_ids, exc,
            )
            await failed.add("conversations", {
                "phoneNumberIds": phone_number_ids,
                "userId": user.id,
                "userName": f"{user.firstName} {user.lastName}",
                "error": str(exc),
            })
            return [], []

        conversation_dicts = [c.to_dict() for c in conversations]
        call_dicts = await _fetch_calls_and_transcripts(service, conversations, failed)
        return conversation_dicts, call_dicts

    (conversation_dicts, call_dicts), message_dicts = await asyncio.gather(
        _conversations_and_calls(),
        _fetch_all_messages(service, phone_numbers, failed),
    )

    user_phone_data = UserPhoneData(
        user=user.to_dict(),
        phoneNumbers=phone_number_dicts,
        conversations=conversation_dicts,
        calls=call_dicts,
        messages=message_dicts,
    )
    return user_phone_data.to_dict()


async def generate_phone_data_transactions(
    max_count: Optional[int] = None,
    output_path: str = "consolidated_phone_data.json",
    failed_path: str = "failed_items.json",
) -> str:
    """
    Main orchestrator:
      1. get_all_users_paginated
      2. Process ALL users in parallel:
         a. get_phonenumber_by_user
         b. One batched conversations call (all phone number IDs)
            -> calls (parallel per conversation, parallel per participant)
            -> transcripts (parallel)
         c. Messages (parallel across phone numbers)
      3. Consolidate into ConsolidatedPhoneData and stream JSON to file.
      4. Write any failed items to a separate JSON file.

    Returns the output file path.
    """
    failed = FailedItemsCollector()

    async with OpenPhoneService(api_key=API_KEY) as service:

        # Step 1 — Users (paginated, inherently sequential)
        logger.info("Step 1: Fetching users...")
        users: List[User] = await service.get_all_users_paginated(max_count=max_count)
        logger.info("Fetched %d users.", len(users))

        # Step 2 — Process all users in parallel
        user_data_list: List[dict] = await asyncio.gather(
            *[_process_user(service, user, failed) for user in users]
        )

        # Compute totals from the gathered results
        total_conversations = sum(len(ud["conversations"]) for ud in user_data_list)
        total_calls = sum(len(ud["calls"]) for ud in user_data_list)
        total_messages = sum(len(ud["messages"]) for ud in user_data_list)
        total_transcripts = sum(
            1 for ud in user_data_list
            for call in ud["calls"]
            if "transcript" in call
        )

        # Step 3 — Build top-level consolidated object and stream to file
        consolidated = ConsolidatedPhoneData(
            generatedAt=datetime.now(timezone.utc).isoformat(),
            totalUsers=len(users),
            totalConversations=total_conversations,
            totalCalls=total_calls,
            totalMessages=total_messages,
            totalTranscripts=total_transcripts,
            userData=user_data_list,
        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(consolidated.to_dict(), f, indent=2, default=str)

        logger.info(
            "Done! Wrote %d users, %d conversations, %d calls, %d messages, %d transcripts to %s",
            len(users),
            total_conversations,
            total_calls,
            total_messages,
            total_transcripts,
            output_path,
        )

        # Step 4 — Write failed items if any
        if failed.has_failures():
            failed_dict = failed.to_dict()
            with open(failed_path, "w", encoding="utf-8") as f:
                json.dump(failed_dict, f, indent=2, default=str)

            logger.warning(
                "⚠ %d items failed to fetch. Details saved to %s",
                failed_dict["totalFailed"],
                failed_path,
            )
            logger.warning(
                "  Breakdown: %d phone_numbers, %d conversations, %d calls, %d messages, %d transcripts",
                len(failed.phone_numbers),
                len(failed.conversations),
                len(failed.calls),
                len(failed.messages),
                len(failed.transcripts),
            )
        else:
            logger.info("All items fetched successfully — no failures.")

    return output_path


# ------------------------------------------------------------------ #
#  CLI entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenPhone Data Consolidation Tool")
    parser.add_argument(
        "--max-count",
        type=int,
        default=None,
        help="Maximum number of users to fetch (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="consolidated_phone_data.json",
        help="Output JSON file path (default: consolidated_phone_data.json)",
    )
    parser.add_argument(
        "--failed-output",
        type=str,
        default="failed_items.json",
        help="Output JSON file for failed items (default: failed_items.json)",
    )
    args = parser.parse_args()

    asyncio.run(
        generate_phone_data_transactions(
            max_count=args.max_count,
            output_path=args.output,
            failed_path=args.failed_output,
        )
    )
