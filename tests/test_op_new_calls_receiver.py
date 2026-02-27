import base64
import hashlib
import hmac
import http.client
import json
import queue
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

from events import op_new_calls_receiver as receiver


class TestOpNewCallsReceiver(unittest.TestCase):
    def setUp(self) -> None:
        self._signing_key_bytes = b"unit-test-calls-signing-key"

        class TestWebhookHandler(receiver.OpenPhoneCallsWebhookHandler):
            pass

        self.handler_cls = TestWebhookHandler
        self.handler_cls.signing_key_bytes = self._signing_key_bytes
        self.handler_cls.signature_tolerance_seconds = 300
        self.handler_cls.event_queue = queue.Queue(maxsize=10)
        self.handler_cls.enqueue_timeout_seconds = 0.2

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.handler_cls)
        # Avoid hanging teardown if a request thread is still active.
        self.server.daemon_threads = True
        self.server.block_on_close = False
        self.port = int(self.server.server_address[1])
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        if self.server_thread.is_alive():
            shutdown_thread = threading.Thread(target=self.server.shutdown, daemon=True)
            shutdown_thread.start()
            shutdown_thread.join(timeout=2.0)
        self.server.server_close()
        self.server_thread.join(timeout=2.0)

    def _build_signature_header(self, raw_body: bytes) -> str:
        timestamp_raw = str(int(time.time() * 1000))
        signed_data = timestamp_raw.encode("utf-8") + b"." + raw_body
        digest = base64.b64encode(
            hmac.new(self._signing_key_bytes, signed_data, hashlib.sha256).digest()
        ).decode("utf-8")
        return f"hmac;1;{timestamp_raw};{digest}"

    def _post_json(self, path: str, payload: dict, signature_header: str | None) -> tuple[int, dict]:
        raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(raw_body)),
        }
        if signature_header is not None:
            headers[receiver.SIGNATURE_HEADER] = signature_header

        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request("POST", path, body=raw_body, headers=headers)
            response = conn.getresponse()
            response_body = response.read().decode("utf-8")
        finally:
            conn.close()

        parsed_body = json.loads(response_body) if response_body else {}
        return response.status, parsed_body

    def test_valid_signed_call_completed_event_is_queued(self) -> None:
        payload = {
            "type": "call.completed",
            "data": {
                "object": {
                    "id": "CA_TEST_1",
                    "phoneNumberId": "PN_TEST_1",
                    "conversationId": "CN_TEST_1",
                    "status": "completed",
                }
            },
        }
        raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        signature_header = self._build_signature_header(raw_body)

        status, body = self._post_json(
            path=receiver.WEBHOOK_PATH,
            payload=payload,
            signature_header=signature_header,
        )

        self.assertEqual(status, 200)
        self.assertEqual(body.get("status"), "queued")

        queued_event = self.handler_cls.event_queue.get_nowait()
        self.assertEqual(queued_event.call_id, "CA_TEST_1")
        self.assertEqual(queued_event.phone_number_id, "PN_TEST_1")
        self.assertEqual(queued_event.payload.get("type"), "call.completed")

    def test_non_call_completed_event_is_ignored(self) -> None:
        payload = {
            "type": "call.ringing",
            "data": {"object": {"id": "CA_TEST_2"}},
        }
        raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        signature_header = self._build_signature_header(raw_body)

        status, body = self._post_json(
            path=receiver.WEBHOOK_PATH,
            payload=payload,
            signature_header=signature_header,
        )

        self.assertEqual(status, 200)
        self.assertEqual(body.get("status"), "ignored")
        self.assertTrue(self.handler_cls.event_queue.empty())

    def test_invalid_signature_is_rejected(self) -> None:
        payload = {
            "type": "call.completed",
            "data": {"object": {"id": "CA_TEST_3"}},
        }
        status, body = self._post_json(
            path=receiver.WEBHOOK_PATH,
            payload=payload,
            signature_header="hmac;1;1234567890;invalid_signature",
        )

        self.assertEqual(status, 401)
        self.assertEqual(body.get("message"), "Invalid webhook signature")
        self.assertTrue(self.handler_cls.event_queue.empty())


if __name__ == "__main__":
    unittest.main()
