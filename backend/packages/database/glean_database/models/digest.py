"""
Digest-related database models.
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, generate_uuid


class DigestRun(Base, TimestampMixin):
    """
    Tracks daily digest generation runs for users.
    """

    __tablename__ = "digest_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=generate_uuid)
    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="processing", nullable=False
    )  # processing / sent / failed / partial_failed
    target_channel: Mapped[str] = mapped_column(
        String(20), default="feishu", nullable=False
    )  # feishu
    feishu_chat_id: Mapped[str | None] = mapped_column(String(100))
    message_id: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(Text)

    # Relationships
    user = relationship("User")
    items = relationship("DigestItem", back_populates="run", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "window_start",
            "window_end",
            "target_channel",
            "feishu_chat_id",
            name="uq_digest_run_window",
        ),
    )


class DigestItem(Base, TimestampMixin):
    """
    Stores individual articles (entries) sent in a digest run.
    """

    __tablename__ = "digest_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=generate_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("digest_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    folder_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("folders.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    category_name: Mapped[str] = mapped_column(String(100), nullable=False)
    entry_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("entries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    code: Mapped[str] = mapped_column(String(20), nullable=False)  # N01, N02...
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    title_zh: Mapped[str | None] = mapped_column(String(1000))
    summary_zh: Mapped[str | None] = mapped_column(Text)
    fulltext_zh: Mapped[str | None] = mapped_column(Text)
    fulltext_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    feishu_message_id: Mapped[str | None] = mapped_column(String(100))

    # Relationships
    run = relationship("DigestRun", back_populates="items")
    user = relationship("User")
    folder = relationship("Folder")
    entry = relationship("Entry")

    __table_args__ = (
        UniqueConstraint("run_id", "code", name="uq_digest_item_code"),
        UniqueConstraint("run_id", "entry_id", name="uq_digest_item_entry"),
        Index("idx_digest_item_user_entry", "user_id", "entry_id"),
    )
