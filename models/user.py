from dataclasses import dataclass, field
from typing import Optional


@dataclass
class User:
    id: str
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    createdAt: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "User":
        return cls(
            id=data.get("id", ""),
            firstName=data.get("firstName"),
            lastName=data.get("lastName"),
            email=data.get("email"),
            role=data.get("role"),
            createdAt=data.get("createdAt"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
