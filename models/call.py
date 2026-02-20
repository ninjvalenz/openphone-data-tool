from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class Call:
    id: str
    phoneNumberId: Optional[str] = None
    from_number: Optional[str] = None
    to: Optional[str] = None
    direction: Optional[str] = None
    status: Optional[str] = None
    duration: Optional[int] = None
    createdAt: Optional[str] = None
    answeredAt: Optional[str] = None
    completedAt: Optional[str] = None
    userId: Optional[str] = None
    conversationId: Optional[str] = None
    transcript: Optional[dict] = None  # will hold Transcript object as dict

    @classmethod
    def from_dict(cls, data: dict) -> "Call":
        return cls(
            id=data.get("id", ""),
            phoneNumberId=data.get("phoneNumberId"),
            from_number=data.get("from"),
            to=data.get("to"),
            direction=data.get("direction"),
            status=data.get("status"),
            duration=data.get("duration"),
            createdAt=data.get("createdAt"),
            answeredAt=data.get("answeredAt"),
            completedAt=data.get("completedAt"),
            userId=data.get("userId"),
            conversationId=data.get("conversationId"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
