"""
Integration tests for Feishu Webhook API endpoints.
"""

import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import DigestItem, DigestRun, Entry, Feed, Subscription, User


@pytest.fixture
async def setup_feishu_config(db_session):
    """Enable Feishu and Digest configuration for testing."""
    config_service = TypedConfigService(db_session)
    await config_service.update(
        FeishuConfig,
        enabled=True,
        app_id="cli_test_app",
        app_secret="test_secret",
        encrypt_key="test_encrypt_key",
        news_chat_id="chat_test_123",
        allowed_user_ids=["usr_test_123"]
    )
    await config_service.update(
        DigestConfig,
        enabled=True,
        user_ids=["user_test_123"]
    )


@pytest.mark.asyncio
async def test_feishu_challenge(client: AsyncClient, setup_feishu_config):
    """Test Feishu URL verification challenge."""
    payload = {
        "type": "url_verification",
        "challenge": "test-feishu-challenge-code"
    }

    response = await client.post(
        "/api/integrations/feishu/events",
        json=payload
    )

    assert response.status_code == 200
    assert response.json() == {"challenge": "test-feishu-challenge-code"}


@pytest.mark.asyncio
async def test_feishu_message_receive_lazy_translate(client: AsyncClient, db_session, setup_feishu_config, monkeypatch):
    """Test receiving an @bot N01 event, doing lazy translation, and replying."""
    # 1. Setup user, feed, entry, digest_run, and digest_item
    user = User(
        id="user_test_123",
        email="feishu.user@example.com",
        name="Feishu User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    feed = Feed(url="https://feed.example.com", title="Feishu Feed")
    db_session.add(feed)
    await db_session.commit()
    await db_session.refresh(feed)

    entry = Entry(
        feed_id=feed.id,
        url="https://article.example.com/1",
        title="Breaking Tech News",
        content="<p>This is the full article text in English that needs translation.</p>",
        embedding_status="done"
    )
    db_session.add(entry)
    await db_session.commit()
    await db_session.refresh(entry)

    sub = Subscription(user_id=user.id, feed_id=feed.id, folder_id=None)
    db_session.add(sub)
    await db_session.commit()

    run = DigestRun(
        user_id=user.id,
        window_start=datetime.now() - timedelta(hours=24),
        window_end=datetime.now(),
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123"
    )
    db_session.add(run)
    await db_session.commit()
    await db_session.refresh(run)

    item = DigestItem(
        run_id=run.id,
        user_id=user.id,
        folder_id=None,
        category_name="Unclassified",
        entry_id=entry.id,
        code="n01",
        rank=1,
        score=95.0,
        title_zh="打破性的科技新闻",
        summary_zh="关于科技的简短摘要。",
        fulltext_zh=None,  # Empty to trigger lazy translation
    )
    db_session.add(item)
    await db_session.commit()

    # 2. Mock Feishu client reply and signature verify methods
    mock_reply = AsyncMock(return_value="msg_reply_123")
    monkeypatch.setattr(
        "glean_core.services.feishu_bot_client.FeishuBotClient.reply_text_message",
        mock_reply
    )
    monkeypatch.setattr(
        "glean_core.services.feishu_bot_client.FeishuBotClient.verify_signature",
        lambda *args, **kwargs: True
    )

    # Mock LLM calls inside ArticleLanguageService
    mock_llm_call = AsyncMock(return_value="这是翻译后的完整中文内容。")
    monkeypatch.setattr(
        "glean_core.services.article_language_service.ArticleLanguageService._call_llm",
        mock_llm_call
    )

    # 3. Simulate Feishu message event payload
    event_payload = {
        "schema": "2.0",
        "header": {
            "event_id": "event_123456",
            "event_type": "im.message.receive_v1",
            "create_time": "1610000000",
            "token": "verification_token_123"
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "usr_test_123",
                    "user_id": "usr_test_123"
                },
                "sender_type": "user"
            },
            "message": {
                "message_id": "om_message_123",
                "chat_id": "chat_test_123",
                "content": json.dumps({"text": "@Glean N01"}),
                "message_type": "text"
            }
        }
    }

    # Custom headers for signature verify placeholder
    headers = {
        "X-Lark-Signature": "fake-sig",
        "X-Lark-Request-Timestamp": "1610000000",
        "X-Lark-Request-Nonce": "fake-nonce",
    }

    # 4. Post request to webhook
    response = await client.post(
        "/api/integrations/feishu/events",
        json=event_payload,
        headers=headers
    )

    assert response.status_code == 200
    assert response.json()["status"] == "processed"
    assert response.json()["code"] == "n01"

    # 5. Check if lazy translation was triggered and database was updated
    await db_session.refresh(item)
    assert item.fulltext_zh == "这是翻译后的完整中文内容。"
    assert item.fulltext_generated_at is not None

    # Check if reply_text_message was invoked
    assert mock_reply.called
    assert "这是翻译后的完整中文内容。" in mock_reply.call_args[0][1]


@pytest.mark.asyncio
async def test_internal_feishu_callback_unauthorized(client: AsyncClient, setup_feishu_config, monkeypatch):
    """Test accessing the internal callback endpoint without a valid token."""
    from glean_api.config import settings
    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")

    payload = {
        "message_id": "om_message_456",
        "chat_id": "chat_test_123",
        "text": "@Glean n01",
    }

    # Missing credentials
    response = await client.post("/api/internal/feishu/messages", json=payload)
    assert response.status_code == 401

    # Invalid token
    headers = {"Authorization": "Bearer wrong_token"}
    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_internal_feishu_callback_success(
    client: AsyncClient, db_session, setup_feishu_config, monkeypatch, tmp_path
):
    """Test successful processing through the internal endpoint writing outbox files."""
    import os

    # 1. Setup user, feed, entry, digest_run, and digest_item
    user = User(
        id="user_test_123",
        email="feishu.user@example.com",
        name="Feishu User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()

    feed = Feed(url="https://feed.example.com", title="Feishu Feed")
    db_session.add(feed)
    await db_session.commit()

    entry = Entry(
        feed_id=feed.id,
        url="https://article.example.com/1",
        title="Breaking Tech News",
        content="<p>This is the full article text in English that needs translation.</p>",
        embedding_status="done"
    )
    db_session.add(entry)
    await db_session.commit()

    run = DigestRun(
        user_id=user.id,
        window_start=datetime.now() - timedelta(hours=24),
        window_end=datetime.now(),
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123"
    )
    db_session.add(run)
    await db_session.commit()

    item = DigestItem(
        run_id=run.id,
        user_id=user.id,
        folder_id=None,
        category_name="Unclassified",
        entry_id=entry.id,
        code="n01",
        rank=1,
        score=95.0,
        title_zh="打破性的科技新闻",
        summary_zh="关于科技的简短摘要。",
        fulltext_zh=None,
    )
    db_session.add(item)
    await db_session.commit()

    # Mock settings and LLM call
    from glean_api.config import settings
    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")
    monkeypatch.setattr(settings, "feishu_dispatcher_outbox_dir", str(tmp_path))

    mock_llm_call = AsyncMock(return_value="这是翻译后的完整中文内容。")
    monkeypatch.setattr(
        "glean_core.services.article_language_service.ArticleLanguageService._call_llm",
        mock_llm_call
    )

    payload = {
        "message_id": "om_message_789",
        "chat_id": "chat_test_123",
        "text": "@Glean n01",
        "sender": {
            "open_id": "usr_test_123",
            "user_id": "usr_test_123"
        }
    }
    headers = {"Authorization": "Bearer test_internal_token"}

    # Post message
    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["code"] == "n01"
    assert data["outbox_file"] is not None

    # Check that outbox file exists and contains correct translation text
    outbox_file = data["outbox_file"]
    assert os.path.exists(outbox_file)
    with open(outbox_file, "r", encoding="utf-8") as f:
        content = f.read()
        assert "这是翻译后的完整中文内容。" in content

    # Test deduplication
    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == "deduplicated"
