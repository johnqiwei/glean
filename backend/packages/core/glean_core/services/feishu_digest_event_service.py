"""
Feishu Digest Event Service.

Handles message processing, validation, short-code resolution,
and lazy translation for Feishu event messages.
"""

import json
import os
import re
import tempfile
from datetime import UTC, datetime
from typing import Any
from arq.connections import ArqRedis
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from glean_core import get_logger
from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.schemas.feishu import FeishuDigestMessage, FeishuDigestProcessResult
from glean_core.services.article_language_service import ArticleLanguageService
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import DigestItem, DigestRun, Entry

logger = get_logger(__name__)

CODE_PATTERN = re.compile(r"\b[nN]\d{2}\b")
MENTION_TEXT_PATTERN = re.compile(r"^\s*@\S+")


class FeishuDigestEventService:
    """
    Handles processing of Feishu incoming messages.
    """

    def __init__(self, session: AsyncSession, redis: ArqRedis) -> None:
        self.db = session
        self.redis = redis
        self.config_service = TypedConfigService(session)

    async def process_message(
        self,
        msg: FeishuDigestMessage,
        write_to_outbox: bool = False,
        outbox_dir: str | None = None,
    ) -> FeishuDigestProcessResult:
        """
        Processes a Feishu digest short-code message.
        """
        # 1. Deduplication via Redis
        redis_dedup_key = f"feishu:processed_msg:{msg.message_id}"
        was_set = await self.redis.set(redis_dedup_key, "1", ex=600, nx=True)
        if not was_set:
            logger.info("Message deduplicated by Redis", extra={"message_id": msg.message_id})
            return FeishuDigestProcessResult(status="deduplicated")

        # Load configurations
        feishu_config = await self.config_service.get(FeishuConfig)
        digest_config = await self.config_service.get(DigestConfig)

        # 2. Filter by target chat
        if msg.chat_id != feishu_config.news_chat_id:
            logger.info(
                "Message chat_id mismatch, ignored",
                extra={"message_chat_id": msg.chat_id, "configured_chat_id": feishu_config.news_chat_id},
            )
            return FeishuDigestProcessResult(status="ignored")

        # 3. Filter by allowed users
        open_id = msg.sender_open_id
        user_id = msg.sender_user_id

        if feishu_config.allowed_user_ids:
            is_allowed = False
            if (open_id and open_id in feishu_config.allowed_user_ids) or (
                user_id and user_id in feishu_config.allowed_user_ids
            ):
                is_allowed = True

            if not is_allowed:
                logger.info(
                    "Sender not in allowed users list",
                    extra={"open_id": open_id, "user_id": user_id},
                )
                return FeishuDigestProcessResult(status="user_not_allowed")

        # 4. Check mention requirement
        if feishu_config.require_mention:
            mentions_bot = False
            # Check mentions from raw metadata
            raw_mentions = msg.raw.get("event", {}).get("message", {}).get("mentions", [])
            if isinstance(raw_mentions, list) and raw_mentions:
                mentions_bot = True
            elif MENTION_TEXT_PATTERN.search(msg.text) is not None:
                mentions_bot = True

            if not mentions_bot:
                logger.info("Mention required but not found in message")
                return FeishuDigestProcessResult(status="mention_required")

        # 5. Parse code (e.g. n01)
        match = CODE_PATTERN.search(msg.text)
        if not match:
            logger.info("No valid article code found in message text", extra={"text": msg.text})
            return FeishuDigestProcessResult(status="no_code_found")

        code = match.group(0).lower()

        # 6. Find the latest digest run sent to this chat
        run_stmt = (
            select(DigestRun)
            .where(
                DigestRun.feishu_chat_id == msg.chat_id,
                DigestRun.status.in_(("sent", "partial_failed")),
            )
            .order_by(desc(DigestRun.created_at))
            .limit(1)
        )

        run_res = await self.db.execute(run_stmt)
        latest_run = run_res.scalar_one_or_none()

        if not latest_run:
            reply_text = "没有找到发送至该群的日报记录，请确认群ID配置是否正确。"
            outbox_file = await self._handle_reply(
                msg.message_id, code, reply_text, write_to_outbox, outbox_dir
            )
            return FeishuDigestProcessResult(
                status="run_not_found", text=reply_text, outbox_file=outbox_file
            )

        # 7. Find digest item
        item_stmt = select(DigestItem).where(
            DigestItem.run_id == latest_run.id,
            DigestItem.code == code,
        )
        item_res = await self.db.execute(item_stmt)
        digest_item = item_res.scalar_one_or_none()

        if not digest_item:
            reply_text = f"没有找到编号 {code} 对应的文章，请确认该编号来自最近的日报。"
            outbox_file = await self._handle_reply(
                msg.message_id, code, reply_text, write_to_outbox, outbox_dir
            )
            return FeishuDigestProcessResult(
                status="item_not_found", code=code, text=reply_text, outbox_file=outbox_file
            )

        # 8. Lazy translate fulltext to Chinese
        if not digest_item.fulltext_zh:
            # Load entry
            entry_stmt = select(Entry).where(Entry.id == digest_item.entry_id)
            entry_res = await self.db.execute(entry_stmt)
            entry = entry_res.scalar_one_or_none()

            if not entry:
                reply_text = "抱歉，该文章的源正文不存在，请尝试点击日报中的原文链接阅读。"
                outbox_file = await self._handle_reply(
                    msg.message_id, code, reply_text, write_to_outbox, outbox_dir
                )
                return FeishuDigestProcessResult(
                    status="entry_not_found",
                    code=code,
                    entry_id=digest_item.entry_id,
                    text=reply_text,
                    outbox_file=outbox_file,
                )

            # Translate content
            lang_service = ArticleLanguageService(digest_config)
            try:
                fulltext_zh = await lang_service.translate_fulltext_to_zh(entry)
                digest_item.fulltext_zh = fulltext_zh
                digest_item.fulltext_generated_at = datetime.now(UTC)
                await self.db.commit()
            except Exception as trans_err:
                logger.exception("Failed to translate entry fulltext", extra={"entry_id": entry.id})
                reply_text = f"翻译正文失败，请稍后重试。错误信息: {trans_err}"
                outbox_file = await self._handle_reply(
                    msg.message_id, code, reply_text, write_to_outbox, outbox_dir
                )
                return FeishuDigestProcessResult(
                    status="translation_failed",
                    code=code,
                    entry_id=digest_item.entry_id,
                    text=reply_text,
                    outbox_file=outbox_file,
                )

        fulltext_zh = digest_item.fulltext_zh or "（未生成有效翻译正文）"
        header_text = f"【{digest_item.title_zh or '正文'}】\n\n"
        full_message = header_text + fulltext_zh

        outbox_file = await self._handle_reply(
            msg.message_id, code, full_message, write_to_outbox, outbox_dir
        )

        return FeishuDigestProcessResult(
            status="processed",
            code=code,
            entry_id=digest_item.entry_id,
            text=full_message,
            outbox_file=outbox_file,
        )

    async def _handle_reply(
        self,
        message_id: str,
        code: str | None,
        text: str,
        write_to_outbox: bool,
        outbox_dir: str | None,
    ) -> str | None:
        """
        Handles the response, optionally writing it to the outbox directory.
        """
        if not write_to_outbox:
            return None

        if not outbox_dir:
            outbox_dir = "/workspace/ocworkspace/feishu_outbox"

        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d_%H%M%S")
        filename = f"daily_news_reply_{timestamp}_{message_id}_{code or 'error'}.txt"

        # Ensure outbox directory exists
        os.makedirs(outbox_dir, exist_ok=True)

        # Write to temporary file in the same directory first for atomic rename
        temp_fd, temp_path = tempfile.mkstemp(suffix=".tmp", prefix=f".{filename}_", dir=outbox_dir)
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write(text)
            final_path = os.path.join(outbox_dir, filename)
            os.replace(temp_path, final_path)
            logger.info("Wrote reply file atomically", extra={"path": final_path})
            return final_path
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            logger.exception("Failed to write atomic reply file", extra={"filename": filename})
            raise e
