"""
HTTP endpoint for receiving OpenPhone new-message webhook events.
"""

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv

from services.openphone_webhook_service import OpenPhoneWebhookService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

WEBHOOK_PATH = "/op_new_message"
SIGNATURE_HEADER = "openphone-signature"
DEFAULT_SIGNATURE_TOLERANCE_SECONDS = 300


def _parse_signature_timestamp(timestamp_raw: str) -> int:
    """
    Parse unix timestamp from signature header and normalize to seconds.
    Quo examples use millisecond precision.
    """
    timestamp_int = int(timestamp_raw)
    if timestamp_int > 1_000_000_000_000:
        return timestamp_int // 1000
    return timestamp_int


def _verify_signature(
    signature_header: str,
    raw_body: bytes,
    signing_key_bytes: bytes,
    tolerance_seconds: int,
) -> bool:
    """
    Verify OpenPhone/Quo signature from openphone-signature header.

    Header format:
      hmac;1;<timestamp>;<signature>
    Future versions may include multiple signatures separated by commas.
    """
    now_seconds = int(time.time())
    candidates = [value.strip() for value in signature_header.split(",") if value.strip()]

    for candidate in candidates:
        parts = candidate.split(";")
        if len(parts) != 4:
            continue

        scheme, version, timestamp_raw, provided_digest = parts
        if scheme != "hmac" or version != "1":
            continue

        try:
            timestamp_seconds = _parse_signature_timestamp(timestamp_raw)
        except ValueError:
            continue

        if tolerance_seconds > 0 and abs(now_seconds - timestamp_seconds) > tolerance_seconds:
            continue

        signed_data = timestamp_raw.encode("utf-8") + b"." + raw_body
        computed_digest = base64.b64encode(
            hmac.new(signing_key_bytes, signed_data, hashlib.sha256).digest()
        ).decode("utf-8")

        if hmac.compare_digest(provided_digest, computed_digest):
            return True

    return False


class OpenPhoneWebhookHandler(BaseHTTPRequestHandler):
    signing_key_bytes: bytes = b""
    signature_tolerance_seconds: int = DEFAULT_SIGNATURE_TOLERANCE_SECONDS

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        request_path = self.path.split("?", 1)[0]
        if request_path != WEBHOOK_PATH:
            self._send_json(404, {"message": "Not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        signature_header = self.headers.get(SIGNATURE_HEADER)
        if not signature_header:
            self._send_json(401, {"message": "Missing webhook signature"})
            return

        is_valid_signature = _verify_signature(
            signature_header=signature_header,
            raw_body=raw_body,
            signing_key_bytes=self.signing_key_bytes,
            tolerance_seconds=self.signature_tolerance_seconds,
        )
        if not is_valid_signature:
            self._send_json(401, {"message": "Invalid webhook signature"})
            return

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"message": "Invalid JSON"})
            return

        try:
            new_message = OpenPhoneWebhookService.parse_new_message_event(payload)
        except Exception as exc:
            logger.exception("Failed to parse webhook payload: %s", exc)
            self._send_json(400, {"message": "Invalid webhook payload"})
            return

        if new_message is None:
            logger.info("Ignored webhook type=%s", payload.get("type"))
            self._send_json(200, {"status": "ignored"})
            return

        logger.info("Mapped inbound webhook message: %s", new_message.to_dict())
        # TODO: Save new_message.to_dict() to the database.

        self._send_json(200, {"status": "ok"})

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)


def run_server() -> None:
    load_dotenv()
    host = os.environ.get("OPENPHONE_WEBHOOK_HOST", "0.0.0.0")
    port = int(os.environ.get("OPENPHONE_WEBHOOK_PORT", "8080"))
    signing_secret = os.environ.get("OPENPHONE_WEBHOOK_SIGNING_SECRET")
    if not signing_secret:
        raise RuntimeError(
            "OPENPHONE_WEBHOOK_SIGNING_SECRET is required for webhook signature verification.",
        )

    try:
        signing_key_bytes = base64.b64decode(signing_secret, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            "OPENPHONE_WEBHOOK_SIGNING_SECRET must be valid base64.",
        ) from exc
    if not signing_key_bytes:
        raise RuntimeError("OPENPHONE_WEBHOOK_SIGNING_SECRET cannot decode to empty bytes.")

    tolerance_raw = os.environ.get(
        "OPENPHONE_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS",
        str(DEFAULT_SIGNATURE_TOLERANCE_SECONDS),
    )
    try:
        tolerance_seconds = int(tolerance_raw)
    except ValueError as exc:
        raise RuntimeError(
            "OPENPHONE_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS must be an integer.",
        ) from exc
    if tolerance_seconds < 0:
        raise RuntimeError("OPENPHONE_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS must be >= 0.")

    OpenPhoneWebhookHandler.signing_key_bytes = signing_key_bytes
    OpenPhoneWebhookHandler.signature_tolerance_seconds = tolerance_seconds

    server = HTTPServer((host, port), OpenPhoneWebhookHandler)
    logger.info("Listening on http://%s:%s%s", host, port, WEBHOOK_PATH)
    server.serve_forever()


if __name__ == "__main__":
    run_server()
