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

CODE_PATTERN = re.compile(
    r"(?<!\w)(?:(?P<date>\d{4})[\s_-]*)?(?:(?:news|n)\s*|#)\s*0*(?P<number>\d{1,3})\b",
    re.IGNORECASE,
)
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
                reply_text = "抱歉，你没有权限获取日报详情。"
                outbox_file = await self._handle_reply(
                    msg.message_id, None, reply_text, write_to_outbox, outbox_dir
                )
                return FeishuDigestProcessResult(
                    status="user_not_allowed", text=reply_text, outbox_file=outbox_file
                )

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

        # 5. Parse codes (e.g. n01, n01 n02, #1 #2, 0612n01)
        parsed_codes = self._parse_digest_codes(msg.text, digest_config.timezone)
        if not parsed_codes:
            logger.info("No valid article code found in message text", extra={"text": msg.text})
            reply_text = "没有识别到文章编号。请按日报里的格式发送，例如：@Glean n01，或 @Glean #1。"
            outbox_file = await self._handle_reply(
                msg.message_id, None, reply_text, write_to_outbox, outbox_dir
            )
            return FeishuDigestProcessResult(
                status="no_code_found", text=reply_text, outbox_file=outbox_file
            )

        run_cache: dict[date | None, DigestRun | None] = {}
        lang_service = ArticleLanguageService(digest_config)
        reply_parts: list[str] = []
        success_codes: list[str] = []
        success_entry_ids: list[str] = []
        error_statuses: list[str] = []

        for code, target_date in parsed_codes:
            if target_date not in run_cache:
                run_cache[target_date] = await self._find_digest_run(
                    chat_id=msg.chat_id,
                    target_date=target_date,
                    timezone_name=digest_config.timezone,
                )
            latest_run = run_cache[target_date]

            if not latest_run:
                if target_date:
                    reply_parts.append(f"【{code}】没有找到 {target_date.strftime('%m%d')} 发送至该群的日报记录。")
                else:
                    reply_parts.append(f"【{code}】没有找到发送至该群的日报记录，请确认群ID配置是否正确。")
                error_statuses.append("run_not_found")
                continue

            item_stmt = select(DigestItem).where(
                DigestItem.run_id == latest_run.id,
                DigestItem.code == code,
            )
            item_res = await self.db.execute(item_stmt)
            digest_item = item_res.scalar_one_or_none()

            if not digest_item:
                reply_parts.append(f"【{code}】没有找到对应的文章，请确认该编号来自该日报。")
                error_statuses.append("item_not_found")
                continue

            if not digest_item.fulltext_zh:
                entry_stmt = select(Entry).where(Entry.id == digest_item.entry_id)
                entry_res = await self.db.execute(entry_stmt)
                entry = entry_res.scalar_one_or_none()

                if not entry:
                    reply_parts.append(f"【{code}】抱歉，该文章的源正文不存在，请尝试点击日报中的原文链接阅读。")
                    error_statuses.append("entry_not_found")
                    continue

                try:
                    fulltext_zh = await lang_service.translate_fulltext_to_zh(entry)
                    digest_item.fulltext_zh = fulltext_zh
                    digest_item.fulltext_generated_at = datetime.now(UTC)
                    await self.db.commit()
                except Exception as trans_err:
                    logger.exception("Failed to translate entry fulltext", extra={"entry_id": entry.id})
                    reply_parts.append(f"【{code}】翻译正文失败，请稍后重试。错误信息: {trans_err}")
                    error_statuses.append("translation_failed")
                    continue

            fulltext_zh = digest_item.fulltext_zh or "（未生成有效翻译正文）"
            reply_parts.append(f"【{code} · {digest_item.title_zh or '正文'}】\n\n{fulltext_zh}")
            success_codes.append(code)
            success_entry_ids.append(digest_item.entry_id)
            await self._mark_entry_liked(digest_item.user_id, digest_item.entry_id)

        full_message = "\n\n---\n\n".join(reply_parts)
        requested_codes = [code for code, _ in parsed_codes]
        outbox_file = await self._handle_reply(
            msg.message_id,
            "_".join(success_codes or requested_codes),
            full_message,
            write_to_outbox,
            outbox_dir,
        )

        if success_codes:
            status = "processed" if not error_statuses else "partial_processed"
        else:
            status = error_statuses[0] if error_statuses else "item_not_found"

        return FeishuDigestProcessResult(
            status=status,
            code=",".join(requested_codes),
            entry_id=",".join(success_entry_ids) if success_entry_ids else None,
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
        codes = cls._parse_digest_codes(text, timezone_name)
        return codes[0] if codes else None

    @classmethod
    def _parse_digest_codes(cls, text: str, timezone_name: str) -> list[tuple[str, date | None]]:
        parsed_codes: list[tuple[str, date | None]] = []
        seen: set[tuple[str, date | None]] = set()

        for match in CODE_PATTERN.finditer(text):
            code = f"n{int(match.group('number')):02d}"
            date_prefix = match.group("date")
            if not date_prefix:
                parsed = (code, None)
                if parsed not in seen:
                    seen.add(parsed)
                    parsed_codes.append(parsed)
                continue

            timezone = cls._get_timezone(timezone_name)
            now = datetime.now(timezone).date()
            month = int(date_prefix[:2])
            day = int(date_prefix[2:])
            try:
                target_date = date(now.year, month, day)
            except ValueError:
                continue

            if target_date > now:
                try:
                    target_date = date(now.year - 1, month, day)
                except ValueError:
                    continue

            parsed = (code, target_date)
            if parsed not in seen:
                seen.add(parsed)
                parsed_codes.append(parsed)

        return parsed_codes

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
