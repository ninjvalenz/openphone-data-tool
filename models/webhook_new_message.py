from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class WebhookNewMessage:
    """
    Model for OpenPhone message.received webhook payloads.

    Keeps webhook metadata plus the nested message object fields.
    """

    id: str
    event: Optional[str] = None
    apiVersion: Optional[str] = None
    eventCreatedAt: Optional[str] = None
    type: Optional[str] = None
    object: Optional[str] = None
    from_number: Optional[str] = None
    to: List[str] = field(default_factory=list)
    direction: Optional[str] = None
    text: Optional[str] = None
    status: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    userId: Optional[str] = None
    phoneNumberId: Optional[str] = None
    conversationId: Optional[str] = None
    contactIds: List[str] = field(default_factory=list)
    media: Optional[list] = None

    @classmethod
    def from_dict(cls, data: dict) -> "WebhookNewMessage":
        payload_data = data.get("data") or {}
        obj = payload_data.get("object") or {}

        to_value = obj.get("to")
        if isinstance(to_value, list):
            to_list = to_value
        elif to_value is None:
            to_list = []
        else:
            to_list = [to_value]

        contact_ids = obj.get("contactIds")
        if not isinstance(contact_ids, list):
            contact_ids = []

        return cls(
            id=obj.get("id", ""),
            event=data.get("event"),
            apiVersion=data.get("apiVersion"),
            eventCreatedAt=data.get("createdAt"),
            type=data.get("type"),
            object=obj.get("object"),
            from_number=obj.get("from"),
            to=to_list,
            direction=obj.get("direction"),
            text=obj.get("text") or obj.get("body"),
            status=obj.get("status"),
            createdAt=obj.get("createdAt"),
            updatedAt=obj.get("updatedAt"),
            userId=obj.get("userId"),
            phoneNumberId=obj.get("phoneNumberId"),
            conversationId=obj.get("conversationId"),
            contactIds=contact_ids,
            media=obj.get("media"),
        )

    def to_dict(self) -> dict:
        result = {
            "id": self.id,
            "event": self.event,
            "apiVersion": self.apiVersion,
            "eventCreatedAt": self.eventCreatedAt,
            "type": self.type,
            "object": self.object,
            "from_number": self.from_number,
            "to": self.to,
            "direction": self.direction,
            "text": self.text,
            "status": self.status,
            "createdAt": self.createdAt,
            "updatedAt": self.updatedAt,
            "userId": self.userId,
            "phoneNumberId": self.phoneNumberId,
            "conversationId": self.conversationId,
            "contactIds": self.contactIds,
            "media": self.media,
        }
        return {k: v for k, v in result.items() if v is not None and v != []}
