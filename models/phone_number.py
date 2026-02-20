from dataclasses import dataclass
from typing import Optional


@dataclass
class PhoneNumber:
    id: str
    number: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    userId: Optional[str] = None
    createdAt: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "PhoneNumber":
        # The API returns phone numbers with a list of userIds
        user_ids = data.get("userIds", [])
        return cls(
            id=data.get("id", ""),
            number=data.get("number"),
            name=data.get("name"),
            type=data.get("type"),
            userId=user_ids[0] if user_ids else data.get("userId"),
            createdAt=data.get("createdAt"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
