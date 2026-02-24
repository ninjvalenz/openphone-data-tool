"""
Shared constants for OpenPhone webhook endpoints.
"""

NEW_MESSAGE_WEBHOOK_PATH = "/op_new_message"
NEW_CALLS_WEBHOOK_PATH = "/op_new_calls"

DEFAULT_CALL_WEBHOOK_EVENTS = (
    "call.ringing",
    "call.completed",
    "call.recording.completed",
)
