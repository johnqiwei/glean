"""
Feishu integrations router.

Handles webhook event callbacks from Feishu (Lark), including challenge verification,
event decryption, event signature verification, and short-code article retrieval.
"""

import hmac
import json
import re
from datetime import UTC, datetime
from typing import Annotated, Any

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.services.article_language_service import ArticleLanguageService
from glean_core.services.feishu_bot_client import FeishuBotClient
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import DigestItem, DigestRun, Entry
from glean_database.session import get_session

from ..dependencies import get_redis_pool

router = APIRouter()

# Regular expression to match N01, N02... code pattern (case insensitive)
CODE_PATTERN = re.compile(r"\b[nN]\d{2}\b")
MENTION_TEXT_PATTERN = re.compile(r"^\s*@\S+")


def _payload_token(payload: dict[str, Any]) -> str | None:
    header = payload.get("header")
    if isinstance(header, dict) and header.get("token"):
        return str(header["token"])
    token = payload.get("token")
    return str(token) if token else None


def _verify_verification_token(payload: dict[str, Any], config: FeishuConfig) -> None:
    if not config.verification_token:
        return
    token = _payload_token(payload)
    if token is None or not hmac.compare_digest(token, config.verification_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Feishu verification token mismatch",
        )


def _message_mentions_bot(message: dict[str, Any], text_content: str) -> bool:
    mentions = message.get("mentions")
    if isinstance(mentions, list) and mentions:
        return True
    return MENTION_TEXT_PATTERN.search(text_content) is not None


@router.post("/events")
async def handle_feishu_events(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[ArqRedis, Depends(get_redis_pool)],
    x_lark_signature: Annotated[str | None, Header()] = None,
    x_lark_request_timestamp: Annotated[str | None, Header()] = None,
    x_lark_request_nonce: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """
    Feishu Event Subscription Endpoint.

    Verifies URL challenges, authenticates signature headers, decrypts payloads
    when configured, and responds to message callbacks.
    """
    body_bytes = await request.body()

    # Parse initial JSON payload
    try:
        payload = json.loads(body_bytes)
    except json.JSONDecodeError as err:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from err

    # Load configurations
    config_service = TypedConfigService(session)
    feishu_config = await config_service.get(FeishuConfig)
    digest_config = await config_service.get(DigestConfig)

    if not feishu_config.enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Feishu integration is disabled",
        )
    if not feishu_config.verification_token and not feishu_config.encrypt_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Feishu callback authentication is not configured",
        )

    feishu_client = FeishuBotClient(feishu_config)

    # 1. Handle Encryption if configured
    is_encrypted = "encrypt" in payload
    if is_encrypted:
        if not feishu_config.encrypt_key:
            raise HTTPException(
                status_code=400,
                detail="Encryption key not configured on server",
            )
        try:
            payload = feishu_client.decrypt_payload(payload["encrypt"])
        except Exception as decrypt_err:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to decrypt payload: {decrypt_err}",
            ) from decrypt_err

    _verify_verification_token(payload, feishu_config)

    # 2. Handle URL Verification Challenge
    if payload.get("type") == "url_verification":
        challenge = payload.get("challenge")
        if not challenge:
            raise HTTPException(status_code=400, detail="Challenge code missing")
        return {"challenge": challenge}

    # 3. Verify Signature if encrypt_key is present (use raw body_bytes for signing verification)
    if feishu_config.encrypt_key:
        if not x_lark_signature or not x_lark_request_timestamp or not x_lark_request_nonce:
            raise HTTPException(
                status_code=401,
                detail="Missing signature headers",
            )
        is_valid = feishu_client.verify_signature(
            timestamp=x_lark_request_timestamp,
            nonce=x_lark_request_nonce,
            signature=x_lark_signature,
            body=body_bytes,
        )
        if not is_valid:
            raise HTTPException(
                status_code=401,
                detail="Signature verification failed",
            )

    # 4. Handle Event Callbacks
    event_header = payload.get("header", {})
    event_type = event_header.get("event_type")

    # We only process message receive events
    if event_type == "im.message.receive_v1":
        event_data = payload.get("event", {})
        message = event_data.get("message", {})
        message_id = message.get("message_id")

        if not message_id:
            return {"status": "ignored"}

        # 4a. Deduplication via Redis
        redis_dedup_key = f"feishu:processed_msg:{message_id}"
        # Set with 10-minute expiration if not exists
        was_set = await redis.set(redis_dedup_key, "1", ex=600, nx=True)
        if not was_set:
            return {"status": "deduplicated"}

        # 4b. Extrac chat, sender and text content
        chat_id = message.get("chat_id")
        sender = event_data.get("sender", {})
        sender_id_data = sender.get("sender_id", {})
        open_id = sender_id_data.get("open_id")
        user_id = sender_id_data.get("user_id")
        content_str = message.get("content")

        if not chat_id or not content_str:
            return {"status": "ignored"}

        # Parse message content (Feishu wraps stringified JSON)
        try:
            content_data = json.loads(content_str)
            text_content = content_data.get("text", "").strip()
        except json.JSONDecodeError:
            return {"status": "ignored"}

        # 4c. Filter by target chat and allowed users
        # Check if chat matches news_chat_id
        if chat_id != feishu_config.news_chat_id:
            return {"status": "ignored"}

        # Check allowed user ids (open_id or user_id)
        if feishu_config.allowed_user_ids:
            is_allowed = False
            if (open_id and open_id in feishu_config.allowed_user_ids) or (
                user_id and user_id in feishu_config.allowed_user_ids
            ):
                is_allowed = True

            if not is_allowed:
                return {"status": "user_not_allowed"}

        if feishu_config.require_mention and not _message_mentions_bot(message, text_content):
            return {"status": "mention_required"}

        # 4d. Parse code (e.g. N01)
        match = CODE_PATTERN.search(text_content)
        if not match:
            return {"status": "no_code_found"}

        code = match.group(0).upper()

        # 4e. Find the latest digest run sent to this chat
        run_stmt = select(DigestRun).where(
            DigestRun.feishu_chat_id == chat_id,
            DigestRun.status.in_(("sent", "partial_failed")),
        ).order_by(desc(DigestRun.created_at)).limit(1)

        run_res = await session.execute(run_stmt)
        latest_run = run_res.scalar_one_or_none()

        if not latest_run:
            await feishu_client.reply_text_message(
                message_id,
                "没有找到发送至该群的日报记录，请确认群ID配置是否正确。"
            )
            return {"status": "run_not_found"}

        # Find digest item
        item_stmt = select(DigestItem).where(
            DigestItem.run_id == latest_run.id,
            DigestItem.code == code,
        )
        item_res = await session.execute(item_stmt)
        digest_item = item_res.scalar_one_or_none()

        if not digest_item:
            await feishu_client.reply_text_message(
                message_id,
                f"没有找到编号 {code} 对应的文章，请确认该编号来自最近的日报。"
            )
            return {"status": "item_not_found"}

        # 4f. Lazy translate fulltext to Chinese
        if not digest_item.fulltext_zh:
            # Load entry
            entry_stmt = select(Entry).where(Entry.id == digest_item.entry_id)
            entry_res = await session.execute(entry_stmt)
            entry = entry_res.scalar_one_or_none()

            if not entry:
                await feishu_client.reply_text_message(
                    message_id,
                    "抱歉，该文章的源正文不存在，请尝试点击日报中的原文链接阅读。"
                )
                return {"status": "entry_not_found"}

            # Translate content
            lang_service = ArticleLanguageService(digest_config)
            try:
                fulltext_zh = await lang_service.translate_fulltext_to_zh(entry)
                digest_item.fulltext_zh = fulltext_zh
                digest_item.fulltext_generated_at = datetime.now(UTC)
                await session.commit()
            except Exception as trans_err:
                await feishu_client.reply_text_message(
                    message_id,
                    f"翻译正文失败，请稍后重试。错误信息: {trans_err}"
                )
                return {"status": "translation_failed"}

        # 4g. Send reply messages (split if too long)
        fulltext_zh = digest_item.fulltext_zh or "（未生成有效翻译正文）"
        header_text = f"【{digest_item.title_zh or '正文'}】\n\n"
        full_message = header_text + fulltext_zh

        # Feishu has a limit of 10000 characters per message, chunk to 9000
        chunk_size = 9000
        message_chunks = [full_message[i:i + chunk_size] for i in range(0, len(full_message), chunk_size)]

        for chunk in message_chunks:
            await feishu_client.reply_text_message(message_id, chunk)

        return {"status": "processed", "code": code, "entry_id": digest_item.entry_id}

    return {"status": "unsupported_event"}
