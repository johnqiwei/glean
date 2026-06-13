"""
Feishu Digest Event Service.

Handles message processing, validation, short-code resolution,
and lazy translation for Feishu event messages.
"""

import os
import re
import tempfile
from contextlib import suppress
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from arq.connections import ArqRedis
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from glean_core import get_logger
from glean_core.redis_keys import RedisKeys
from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.schemas.feishu import FeishuDigestMessage, FeishuDigestProcessResult
from glean_core.services.article_language_service import ArticleLanguageService
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import DigestItem, DigestRun, Entry, UserEntry

logger = get_logger(__name__)

CODE_PATTERN = re.compile(r"\b(?:(?P<date>\d{4})[\s_-]*)?(?P<code>[nN]\d{2})\b")
MENTION_TEXT_PATTERN = re.compile(r"^\s*@\S+")
SAFE_FILENAME_PART_PATTERN = re.compile(r"[^A-Za-z0-9_-]+")


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
                extra={
                    "message_chat_id": msg.chat_id,
                    "configured_chat_id": feishu_config.news_chat_id,
                },
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
            if (isinstance(raw_mentions, list) and raw_mentions) or (
                MENTION_TEXT_PATTERN.search(msg.text) is not None
            ):
                mentions_bot = True

            if not mentions_bot:
                logger.info("Mention required but not found in message")
                return FeishuDigestProcessResult(status="mention_required")

        # 5. Parse code (e.g. n01, 0612n01, 0612 n01)
        parsed_code = self._parse_digest_code(msg.text, digest_config.timezone)
        if not parsed_code:
            logger.info("No valid article code found in message text", extra={"text": msg.text})
            return FeishuDigestProcessResult(status="no_code_found")

        code, target_date = parsed_code

        # 6. Find digest run. Plain n01 uses latest run; MMDD+n01 uses that local date.
        latest_run = await self._find_digest_run(
            chat_id=msg.chat_id,
            target_date=target_date,
            timezone_name=digest_config.timezone,
        )

        if not latest_run:
            if target_date:
                reply_text = f"没有找到 {target_date.strftime('%m%d')} 发送至该群的日报记录。"
            else:
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
        await self._mark_entry_liked(digest_item.user_id, digest_item.entry_id)

        return FeishuDigestProcessResult(
            status="processed",
            code=code,
            entry_id=digest_item.entry_id,
            text=full_message,
            outbox_file=outbox_file,
        )

    async def _find_digest_run(
        self,
        chat_id: str,
        target_date: date | None,
        timezone_name: str,
    ) -> DigestRun | None:
        stmt = (
            select(DigestRun)
            .where(
                DigestRun.feishu_chat_id == chat_id,
                DigestRun.status.in_(("sent", "partial_failed")),
            )
            .order_by(desc(DigestRun.created_at))
        )

        if target_date is None:
            stmt = stmt.limit(1)
        else:
            timezone = self._get_timezone(timezone_name)
            start_at = datetime.combine(target_date, time.min, tzinfo=timezone).astimezone(UTC)
            end_at = datetime.combine(
                target_date + timedelta(days=1), time.min, tzinfo=timezone
            ).astimezone(UTC)
            stmt = stmt.where(
                DigestRun.created_at >= start_at, DigestRun.created_at < end_at
            ).limit(1)

        run_res = await self.db.execute(stmt)
        return run_res.scalar_one_or_none()

    async def _mark_entry_liked(self, user_id: str, entry_id: str) -> None:
        stmt = select(UserEntry).where(UserEntry.user_id == user_id, UserEntry.entry_id == entry_id)
        result = await self.db.execute(stmt)
        user_entry = result.scalar_one_or_none()
        old_is_liked = user_entry.is_liked if user_entry else None

        if not user_entry:
            user_entry = UserEntry(user_id=user_id, entry_id=entry_id)
            self.db.add(user_entry)

        now = datetime.now(UTC)
        user_entry.is_liked = True
        user_entry.liked_at = now
        await self.db.commit()

        if old_is_liked is True:
            return

        try:
            debounce_key = RedisKeys.pref_update_debounce(user_id, entry_id, "like")
            was_set = await self.redis.set(
                debounce_key,
                "1",
                ex=RedisKeys.PREF_UPDATE_DEBOUNCE_TTL,
                nx=True,
            )
            if was_set:
                await self.redis.enqueue_job(
                    "update_user_preference",
                    user_id=user_id,
                    entry_id=entry_id,
                    signal_type="like",
                )
        except Exception as err:
            logger.warning(
                "Failed to queue preference update after Feishu like", extra={"error": str(err)}
            )

    @classmethod
    def _parse_digest_code(cls, text: str, timezone_name: str) -> tuple[str, date | None] | None:
        match = CODE_PATTERN.search(text)
        if not match:
            return None

        code = match.group("code").lower()
        date_prefix = match.group("date")
        if not date_prefix:
            return code, None

        timezone = cls._get_timezone(timezone_name)
        now = datetime.now(timezone).date()
        month = int(date_prefix[:2])
        day = int(date_prefix[2:])
        try:
            target_date = date(now.year, month, day)
        except ValueError:
            return None

        if target_date > now:
            try:
                target_date = date(now.year - 1, month, day)
            except ValueError:
                return None
        return code, target_date

    @staticmethod
    def _get_timezone(timezone_name: str) -> ZoneInfo:
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            logger.warning(
                "Invalid digest timezone, falling back to UTC", extra={"timezone": timezone_name}
            )
            return ZoneInfo("UTC")

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
        safe_message_id = self._safe_filename_part(message_id)
        safe_code = self._safe_filename_part(code or "error")
        filename = f"daily_news_reply_{timestamp}_{safe_message_id}_{safe_code}.txt"

        # Ensure outbox directory exists
        os.makedirs(outbox_dir, exist_ok=True)

        # Write to temporary file in the same directory first for atomic rename
        temp_fd, temp_path = tempfile.mkstemp(suffix=".tmp", prefix=f".{filename}_", dir=outbox_dir)
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.chmod(temp_path, 0o666)
            final_path = os.path.join(outbox_dir, filename)
            os.replace(temp_path, final_path)
            logger.info("Wrote reply file atomically", extra={"path": final_path})
            return final_path
        except Exception:
            if os.path.exists(temp_path):
                with suppress(OSError):
                    os.remove(temp_path)
            logger.exception("Failed to write atomic reply file", extra={"filename": filename})
            raise

    @staticmethod
    def _safe_filename_part(value: str) -> str:
        """
        Keep dispatcher outbox filenames inside the target directory.
        """
        safe_value = SAFE_FILENAME_PART_PATTERN.sub("_", value).strip("_")
        return safe_value or "unknown"
