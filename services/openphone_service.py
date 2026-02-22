import asyncio
import time
import aiohttp
import logging
from typing import List, Optional

from models.user import User
from models.phone_number import PhoneNumber
from models.conversation import Conversation
from models.call import Call
from models.message import Message
from models.transcript import Transcript

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openphone.com/v1"

# Retry config for rate limits (safety net if a 429 still slips through)
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2  # seconds

# OpenPhone API allows 10 requests/second per API key.
# We use 9 to leave a small margin and avoid ever hitting 429.
DEFAULT_REQUESTS_PER_SECOND = 9
DEFAULT_MAX_CONCURRENCY = 10


# ------------------------------------------------------------------ #
#  Token-bucket rate limiter
# ------------------------------------------------------------------ #
class TokenBucketRateLimiter:
    """
    Async token-bucket rate limiter.

    Allows up to `rate` requests per second with a burst capacity of
    `burst` tokens.  Each call to acquire() waits until a token is
    available, spacing requests evenly to stay under the API limit.
    """

    def __init__(self, rate: float, burst: Optional[int] = None):
        self.rate = rate                          # tokens added per second
        self.burst = burst or int(rate)           # max tokens in the bucket
        self._tokens = float(self.burst)          # start full
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.burst,
                    self._tokens + elapsed * self.rate,
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                # Calculate how long until the next token is available
                wait = (1.0 - self._tokens) / self.rate

            await asyncio.sleep(wait)


class OpenPhoneApiError(Exception):
    """Base exception for OpenPhone API errors."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[HTTP {status_code}] {message}")


class AuthenticationError(OpenPhoneApiError):
    pass


class RateLimitError(OpenPhoneApiError):
    pass


class OpenPhoneService:
    def __init__(
        self,
        api_key: str,
        requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ):
        self.api_key = api_key
        self._rate_limiter = TokenBucketRateLimiter(
            rate=requests_per_second,
            burst=int(requests_per_second),
        )
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": self.api_key,
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> dict:
        """
        Central HTTP helper with token-bucket rate limiting, auth-failure
        handling, server-error retry, and 429 back-off as a safety net.

        Flow:
          1. Acquire a token from the rate limiter (waits if needed)
          2. Acquire the concurrency semaphore
          3. Make the request
          4. On 429 or 5xx, back off and retry
        """
        url = f"{BASE_URL}/{endpoint}"
        retries = 0

        while True:
            # Wait for a rate-limit token before sending
            await self._rate_limiter.acquire()

            async with self._semaphore:
                try:
                    async with self.session.request(
                        method,
                        url,
                        params=params,
                        json=json_body,
                    ) as resp:
                        status = resp.status
                        content_type = resp.headers.get("Content-Type", "")

                        # Auth failures (401 / 403) — no retry
                        if status in (401, 403):
                            text = await resp.text()
                            raise AuthenticationError(status, f"Authentication failed: {text}")

                        # Rate limit (429) — safety-net back-off
                        if status == 429:
                            retries += 1
                            if retries > MAX_RETRIES:
                                raise RateLimitError(429, "Rate limit exceeded after max retries.")
                            wait = RETRY_BACKOFF_BASE ** retries
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    wait = int(retry_after)
                                except ValueError:
                                    pass
                            logger.warning("Rate-limited (429). Waiting %s s (attempt %s/%s)", wait, retries, MAX_RETRIES)
                            await asyncio.sleep(wait)
                            continue

                        # Server errors (500/502/503/504) — retry with back-off
                        if status in (500, 502, 503, 504):
                            retries += 1
                            if retries > MAX_RETRIES:
                                raise OpenPhoneApiError(status, f"Server error ({status}) persisted after {MAX_RETRIES} retries.")
                            wait = RETRY_BACKOFF_BASE ** retries
                            logger.warning(
                                "Server error (%s) on %s. Retrying in %s s (attempt %s/%s)",
                                status, endpoint, wait, retries, MAX_RETRIES,
                            )
                            await asyncio.sleep(wait)
                            continue

                        # Other client errors (4xx)
                        if status >= 400:
                            text = await resp.text()
                            raise OpenPhoneApiError(status, text)

                        # Success — but guard against non-JSON responses
                        if resp.content_length == 0:
                            return {}

                        if "application/json" not in content_type:
                            logger.warning(
                                "Unexpected content type '%s' from %s (HTTP %s). "
                                "The API may be experiencing issues.",
                                content_type, endpoint, status,
                            )
                            return {}

                        data = await resp.json()
                        return data

                except aiohttp.ClientError as exc:
                    raise OpenPhoneApiError(0, f"Network error: {exc}")

    # ------------------------------------------------------------------ #
    #  get_all_users_paginated
    # ------------------------------------------------------------------ #
    async def get_all_users_paginated(self, max_count: Optional[int] = None) -> List[User]:
        """
        Fetch all users, paginating via pageToken.
        If max_count is set, stop after collecting that many users.
        """
        users: List[User] = []
        page_token: Optional[str] = None

        while True:
            params: dict = {}
            if page_token:
                params["pageToken"] = page_token

            data = await self._request("GET", "users", params=params)
            items = data.get("data", [])

            if not items:
                logger.info("get_all_users_paginated: no more records.")
                break

            for item in items:
                users.append(User.from_dict(item))
                if max_count and len(users) >= max_count:
                    logger.info("get_all_users_paginated: reached maxCount=%s", max_count)
                    return users

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        logger.info("get_all_users_paginated: fetched %d users.", len(users))
        return users

    # ------------------------------------------------------------------ #
    #  get_phonenumber_by_user
    # ------------------------------------------------------------------ #
    async def get_phonenumber_by_user(self, user_id: str) -> List[PhoneNumber]:
        """
        Fetch phone numbers assigned to a specific user.
        """
        params = {"userId": user_id}
        data = await self._request("GET", "phone-numbers", params=params)
        items = data.get("data", [])

        if not items:
            logger.info("get_phonenumber_by_user: no phone numbers for user %s", user_id)
            return []

        phone_numbers: List[PhoneNumber] = []
        for item in items:
            pn = PhoneNumber.from_dict(item)
            pn.userId = user_id
            phone_numbers.append(pn)

        logger.info("get_phonenumber_by_user: found %d numbers for user %s", len(phone_numbers), user_id)
        return phone_numbers

    # ------------------------------------------------------------------ #
    #  get_all_conversations_by_phonenumber
    # ------------------------------------------------------------------ #
    async def get_all_conversations(self, phone_number_ids: List[str]) -> List[Conversation]:
        """
        Fetch all conversations for one or more phone numbers in a single
        paginated call.  The API accepts multiple phoneNumberId params.
        """
        conversations: List[Conversation] = []
        page_token: Optional[str] = None

        while True:
            params: dict = {"phoneNumberId": phone_number_ids}
            if page_token:
                params["pageToken"] = page_token

            data = await self._request("GET", "conversations", params=params)
            items = data.get("data", [])

            if not items:
                logger.info("get_all_conversations: no more records for %s", phone_number_ids)
                break

            for item in items:
                conversations.append(Conversation.from_dict(item))

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        logger.info("get_all_conversations: fetched %d conversations for %d phone numbers", len(conversations), len(phone_number_ids))
        return conversations

    # ------------------------------------------------------------------ #
    #  get_all_calls_by_phonenumber (parallel per participant)
    # ------------------------------------------------------------------ #
    async def _fetch_calls_for_participant(self, phone_number_id: str, participant: str) -> List[Call]:
        """Fetch all paginated calls for a single participant."""
        calls: List[Call] = []
        page_token: Optional[str] = None

        while True:
            params: dict = {
                "phoneNumberId": phone_number_id,
                "participants": [participant],
            }
            if page_token:
                params["pageToken"] = page_token

            data = await self._request("GET", "calls", params=params)
            items = data.get("data", [])

            if not items:
                break

            for item in items:
                calls.append(Call.from_dict(item))

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        return calls

    async def get_all_calls_by_phonenumber(self, phone_number_id: str, participants: List[str]) -> List[Call]:
        """
        Fetch all calls for a phone number — participants are fetched in parallel.
        Deduplicates by call ID.
        """
        # Launch all participant fetches concurrently
        results = await asyncio.gather(
            *[self._fetch_calls_for_participant(phone_number_id, p) for p in participants]
        )

        seen_ids: set = set()
        calls: List[Call] = []
        for participant_calls in results:
            for call in participant_calls:
                if call.id not in seen_ids:
                    seen_ids.add(call.id)
                    calls.append(call)

        logger.info("get_all_calls_by_phonenumber: fetched %d calls for %s", len(calls), phone_number_id)
        return calls

    # ------------------------------------------------------------------ #
    #  get_all_messages_by_phonenumber
    # ------------------------------------------------------------------ #
    async def get_all_messages_by_phonenumber(self, phone_number_id: str, phone_number: str) -> List[Message]:
        """
        Fetch all messages for a phone number, paginating via pageToken.
        """
        messages: List[Message] = []
        page_token: Optional[str] = None

        while True:
            params: dict = {
                "phoneNumberId": phone_number_id,
                "participants": [phone_number],
            }
            if page_token:
                params["pageToken"] = page_token

            data = await self._request("GET", "messages", params=params)
            items = data.get("data", [])

            if not items:
                logger.info("get_all_messages_by_phonenumber: no more records for %s", phone_number_id)
                break

            for item in items:
                messages.append(Message.from_dict(item))

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        logger.info("get_all_messages_by_phonenumber: fetched %d messages for %s", len(messages), phone_number_id)
        return messages

    # ------------------------------------------------------------------ #
    #  get_all_transcripts_by_call
    # ------------------------------------------------------------------ #
    async def get_all_transcripts_by_call(self, call_id: str) -> Optional[Transcript]:
        """
        Fetch the transcript for a specific call.
        The API uses a path parameter: GET /call-transcripts/{callId}
        Returns None if no transcript exists.
        """
        try:
            data = await self._request("GET", f"call-transcripts/{call_id}")
        except OpenPhoneApiError as exc:
            if exc.status_code == 404:
                logger.info("get_all_transcripts_by_call: no transcript for call %s", call_id)
                return None
            raise

        # The API returns the transcript directly (not wrapped in a "data" array)
        if not data:
            logger.info("get_all_transcripts_by_call: empty transcript for call %s", call_id)
            return None

        # Handle both direct object and "data" wrapper for safety
        transcript_data = data.get("data", data) if isinstance(data, dict) else data
        if isinstance(transcript_data, list):
            if not transcript_data:
                return None
            transcript_data = transcript_data[0]

        transcript = Transcript.from_dict(transcript_data)
        logger.info("get_all_transcripts_by_call: fetched transcript for call %s", call_id)
        return transcript
