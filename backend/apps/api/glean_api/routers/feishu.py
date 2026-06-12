"""
Feishu integrations router.

Handles webhook event callbacks from Feishu (Lark), including challenge verification,
event decryption, event signature verification, and short-code article retrieval.
Also provides a secure internal endpoint for local dispatcher delivery.
"""

import hmac
import json
import re
from typing import Annotated, Any

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from glean_api.config import settings
from glean_core.schemas.config import FeishuConfig
from glean_core.schemas.feishu import FeishuDigestMessage
from glean_core.services.feishu_bot_client import FeishuBotClient
from glean_core.services.feishu_digest_event_service import FeishuDigestEventService
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.session import get_session

from ..dependencies import get_redis_pool

router = APIRouter()
internal_router = APIRouter()

security = HTTPBearer(auto_error=False)


async def verify_internal_token(
    credentials: HTTPAuthorizationCredentials | None = Security(security)
) -> None:
    """Verify the bearer token for internal callback requests."""
    if not credentials:
        if not settings.feishu_dispatcher_callback_token and settings.debug:
            return
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization credentials",
        )
    token = credentials.credentials
    expected_token = settings.feishu_dispatcher_callback_token
    if not expected_token:
        if settings.debug:
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Internal callback token not configured",
        )
    if not hmac.compare_digest(token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid internal callback token",
        )


MENTION_TEXT_PATTERN = re.compile(r"^\s*@\S+")


def _message_mentions_bot(message: dict[str, Any], text_content: str) -> bool:
    mentions = message.get("mentions")
    if isinstance(mentions, list) and mentions:
        return True
    return MENTION_TEXT_PATTERN.search(text_content) is not None


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
    Feishu Event Subscription Endpoint (Webhook mode).

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

    # 3. Verify Signature if encrypt_key is present
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

        # Extract chat, sender and text content
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

        # Build normalized payload schema
        msg_obj = FeishuDigestMessage(
            event_id=event_header.get("event_id"),
            message_id=message_id,
            chat_id=chat_id,
            chat_type=message.get("chat_type"),
            message_type=message.get("message_type", "text"),
            text=text_content,
            sender={"open_id": open_id, "user_id": user_id},
            raw=payload,
        )

        event_service = FeishuDigestEventService(session, redis)
        result = await event_service.process_message(msg_obj, write_to_outbox=False)

        # If webhook path successfully generated reply text, reply directly
        if result.text:
            # Feishu has a limit of 10000 characters per message, chunk to 9000
            chunk_size = 9000
            message_chunks = [result.text[i : i + chunk_size] for i in range(0, len(result.text), chunk_size)]

            for chunk in message_chunks:
                await feishu_client.reply_text_message(message_id, chunk)

        return {"status": result.status, "code": result.code, "entry_id": result.entry_id}

    return {"status": "unsupported_event"}


@internal_router.post("/messages")
async def handle_internal_dispatcher_messages(
    msg: FeishuDigestMessage,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[ArqRedis, Depends(get_redis_pool)],
    _: Annotated[None, Depends(verify_internal_token)] = None,
) -> dict[str, Any]:
    """
    Internal callback endpoint called by feishu-dispatcher.
    """
    event_service = FeishuDigestEventService(session, redis)
    result = await event_service.process_message(
        msg,
        write_to_outbox=True,
        outbox_dir=settings.feishu_dispatcher_outbox_dir,
    )
    return {
        "status": result.status,
        "code": result.code,
        "entry_id": result.entry_id,
        "outbox_file": result.outbox_file,
    }
