from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

from .user import User
from .phone_number import PhoneNumber
from .call import Call
from .message import Message


@dataclass
class UserPhoneData:
    """Consolidated data for a single user: their info, phone numbers, conversations, calls, and messages."""

    user: dict = field(default_factory=dict)
    phoneNumbers: List[dict] = field(default_factory=list)
    conversations: List[dict] = field(default_factory=list)
    calls: List[dict] = field(default_factory=list)
    messages: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "user": self.user,
            "phoneNumbers": self.phoneNumbers,
            "conversations": self.conversations,
            "calls": self.calls,
            "messages": self.messages,
        }


@dataclass
class ConsolidatedPhoneData:
    """Top-level consolidated data containing all users and their phone data."""

    generatedAt: str = ""
    totalUsers: int = 0
    totalConversations: int = 0
    totalCalls: int = 0
    totalMessages: int = 0
    totalTranscripts: int = 0
    userData: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "generatedAt": self.generatedAt,
            "totalUsers": self.totalUsers,
            "totalConversations": self.totalConversations,
            "totalCalls": self.totalCalls,
            "totalMessages": self.totalMessages,
            "totalTranscripts": self.totalTranscripts,
            "userData": self.userData,
        }
