"""
Feishu integration schema definitions.
"""

from typing import Any
from pydantic import BaseModel, Field


class FeishuDigestSender(BaseModel):
    """Feishu event sender metadata."""

    open_id: str | None = None
    user_id: str | None = None


class FeishuDigestMessage(BaseModel):
    """
    Normalized message receive event payload forwarded by feishu-dispatcher.
    """

    event_id: str | None = None
    message_id: str
    chat_id: str
    chat_type: str | None = None
    message_type: str = "text"
    text: str
    sender: FeishuDigestSender | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @property
    def sender_open_id(self) -> str | None:
        """Helper to get sender open_id."""
        return self.sender.open_id if self.sender else None

    @property
    def sender_user_id(self) -> str | None:
        """Helper to get sender user_id."""
        return self.sender.user_id if self.sender else None


class FeishuDigestProcessResult(BaseModel):
    """
    Result of processing a Feishu digest message.
    """

    status: str
    code: str | None = None
    entry_id: str | None = None
    text: str | None = None
    outbox_file: str | None = None
