"""
HTTP endpoint for receiving OpenPhone new-message webhook events.
"""

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv

from services.openphone_webhook_service import OpenPhoneWebhookService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

WEBHOOK_PATH = "/op_new_message"


class OpenPhoneWebhookHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != WEBHOOK_PATH:
            self._send_json(404, {"message": "Not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"message": "Invalid JSON"})
            return

        new_message = OpenPhoneWebhookService.parse_new_message_event(payload)
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

    server = HTTPServer((host, port), OpenPhoneWebhookHandler)
    logger.info("Listening on http://%s:%s%s", host, port, WEBHOOK_PATH)
    server.serve_forever()


if __name__ == "__main__":
    run_server()
