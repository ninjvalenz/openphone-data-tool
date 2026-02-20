from dataclasses import dataclass
from typing import Optional


@dataclass
class Message:
    id: str
    phoneNumberId: Optional[str] = None
    from_number: Optional[str] = None
    to: Optional[str] = None
    body: Optional[str] = None
    direction: Optional[str] = None
    status: Optional[str] = None
    createdAt: Optional[str] = None
    userId: Optional[str] = None
    conversationId: Optional[str] = None
    media: Optional[list] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        return cls(
            id=data.get("id", ""),
            phoneNumberId=data.get("phoneNumberId"),
            from_number=data.get("from"),
            to=data.get("to"),
            body=data.get("body"),
            direction=data.get("direction"),
            status=data.get("status"),
            createdAt=data.get("createdAt"),
            userId=data.get("userId"),
            conversationId=data.get("conversationId"),
            media=data.get("media"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
