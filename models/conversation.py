from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class Conversation:
    id: str
    phoneNumberId: Optional[str] = None
    participants: List[str] = field(default_factory=list)
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    lastActivityAt: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Conversation":
        return cls(
            id=data.get("id", ""),
            phoneNumberId=data.get("phoneNumberId"),
            participants=data.get("participants", []),
            createdAt=data.get("createdAt"),
            updatedAt=data.get("updatedAt"),
            lastActivityAt=data.get("lastActivityAt"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
