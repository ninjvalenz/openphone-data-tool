from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class TranscriptDialogue:
    content: Optional[str] = None
    start: Optional[float] = None
    end: Optional[float] = None
    identifier: Optional[str] = None
    userId: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "TranscriptDialogue":
        return cls(
            content=data.get("content"),
            start=data.get("start"),
            end=data.get("end"),
            identifier=data.get("identifier"),
            userId=data.get("userId"),
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Transcript:
    callId: str
    status: Optional[str] = None
    createdAt: Optional[str] = None
    duration: Optional[float] = None
    dialogue: List[TranscriptDialogue] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> "Transcript":
        dialogue_data = data.get("dialogue") or []
        return cls(
            callId=data.get("callId", ""),
            status=data.get("status"),
            createdAt=data.get("createdAt"),
            duration=data.get("duration"),
            dialogue=[TranscriptDialogue.from_dict(d) for d in dialogue_data],
        )

    def to_dict(self) -> dict:
        result = {k: v for k, v in self.__dict__.items() if v is not None and k != "dialogue"}
        result["dialogue"] = [d.to_dict() for d in self.dialogue]
        return result
