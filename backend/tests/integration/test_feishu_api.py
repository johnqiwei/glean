"""
Integration tests for Feishu Webhook API endpoints.
"""

import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import DigestItem, DigestRun, Entry, Feed, Subscription, User, UserEntry


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
        allowed_user_ids=["usr_test_123"],
    )
    await config_service.update(DigestConfig, enabled=True, user_ids=["user_test_123"])


@pytest.mark.asyncio
async def test_feishu_challenge(client: AsyncClient, setup_feishu_config):
    """Test Feishu URL verification challenge."""
    payload = {"type": "url_verification", "challenge": "test-feishu-challenge-code"}

    response = await client.post("/api/integrations/feishu/events", json=payload)

    assert response.status_code == 200
    assert response.json() == {"challenge": "test-feishu-challenge-code"}


@pytest.mark.asyncio
async def test_feishu_message_receive_lazy_translate(
    client: AsyncClient, db_session, setup_feishu_config, monkeypatch
):
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
        embedding_status="done",
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
        feishu_chat_id="chat_test_123",
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
        "glean_core.services.feishu_bot_client.FeishuBotClient.reply_text_message", mock_reply
    )
    monkeypatch.setattr(
        "glean_core.services.feishu_bot_client.FeishuBotClient.verify_signature",
        lambda *args, **kwargs: True,
    )

    # Mock LLM calls inside ArticleLanguageService
    mock_llm_call = AsyncMock(return_value="这是翻译后的完整中文内容。")
    monkeypatch.setattr(
        "glean_core.services.article_language_service.ArticleLanguageService._call_llm",
        mock_llm_call,
    )

    # 3. Simulate Feishu message event payload
    event_payload = {
        "schema": "2.0",
        "header": {
            "event_id": "event_123456",
            "event_type": "im.message.receive_v1",
            "create_time": "1610000000",
            "token": "verification_token_123",
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
                "sender_type": "user",
            },
            "message": {
                "message_id": "om_message_123",
                "chat_id": "chat_test_123",
                "content": json.dumps({"text": "@Glean N01"}),
                "message_type": "text",
            },
        },
    }

    # Custom headers for signature verify placeholder
    headers = {
        "X-Lark-Signature": "fake-sig",
        "X-Lark-Request-Timestamp": "1610000000",
        "X-Lark-Request-Nonce": "fake-nonce",
    }

    # 4. Post request to webhook
    response = await client.post(
        "/api/integrations/feishu/events", json=event_payload, headers=headers
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
async def test_internal_feishu_callback_unauthorized(
    client: AsyncClient, setup_feishu_config, monkeypatch
):
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
        embedding_status="done",
    )
    db_session.add(entry)
    await db_session.commit()

    run = DigestRun(
        user_id=user.id,
        window_start=datetime.now() - timedelta(hours=24),
        window_end=datetime.now(),
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123",
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
        mock_llm_call,
    )

    payload = {
        "message_id": "om_message_789/../../escape",
        "chat_id": "chat_test_123",
        "text": "@Glean n01",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
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
    assert os.path.dirname(outbox_file) == str(tmp_path)
    assert ".." not in os.path.basename(outbox_file)
    assert "/" not in os.path.basename(outbox_file)
    with open(outbox_file, encoding="utf-8") as f:
        content = f.read()
        assert "这是翻译后的完整中文内容。" in content

    liked_result = await db_session.execute(
        select(UserEntry).where(UserEntry.user_id == user.id, UserEntry.entry_id == entry.id)
    )
    liked_state = liked_result.scalar_one()
    assert liked_state.is_liked is True
    assert liked_state.liked_at is not None

    # Test deduplication
    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == "deduplicated"


@pytest.mark.asyncio
async def test_internal_feishu_callback_accepts_hash_number_code(
    client: AsyncClient, db_session, setup_feishu_config, monkeypatch, tmp_path
):
    """A user can request details as @bot #1, not only @bot n01."""
    user = User(
        id="user_test_123",
        email="feishu.user@example.com",
        name="Feishu User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()

    feed = Feed(url="https://feed.example.com/hash", title="Feishu Hash Feed")
    db_session.add(feed)
    await db_session.commit()

    entry = Entry(
        feed_id=feed.id,
        url="https://article.example.com/hash",
        title="Hash Code News",
        content="<p>hash code</p>",
        embedding_status="done",
    )
    db_session.add(entry)
    await db_session.commit()

    run = DigestRun(
        user_id=user.id,
        window_start=datetime.now() - timedelta(hours=24),
        window_end=datetime.now(),
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123",
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
        title_zh="编号新闻",
        summary_zh="编号摘要",
        fulltext_zh="编号正文",
    )
    db_session.add(item)
    await db_session.commit()

    from glean_api.config import settings

    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")
    monkeypatch.setattr(settings, "feishu_dispatcher_outbox_dir", str(tmp_path))

    payload = {
        "message_id": "om_message_hash_code",
        "chat_id": "chat_test_123",
        "text": "@Glean #1",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
    }
    headers = {"Authorization": "Bearer test_internal_token"}

    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["code"] == "n01"
    with open(data["outbox_file"], encoding="utf-8") as f:
        assert "编号正文" in f.read()


@pytest.mark.asyncio
async def test_internal_feishu_callback_accepts_multiple_codes(
    client: AsyncClient, db_session, setup_feishu_config, monkeypatch, tmp_path
):
    """A single mention can request multiple article details."""
    user = User(
        id="user_test_123",
        email="feishu.user@example.com",
        name="Feishu User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()

    feed = Feed(url="https://feed.example.com/multiple", title="Feishu Multiple Feed")
    db_session.add(feed)
    await db_session.commit()

    entry_one = Entry(
        feed_id=feed.id,
        url="https://article.example.com/multiple-1",
        title="First News",
        content="<p>first</p>",
        embedding_status="done",
    )
    entry_two = Entry(
        feed_id=feed.id,
        url="https://article.example.com/multiple-2",
        title="Second News",
        content="<p>second</p>",
        embedding_status="done",
    )
    db_session.add_all([entry_one, entry_two])
    await db_session.commit()

    run = DigestRun(
        user_id=user.id,
        window_start=datetime.now() - timedelta(hours=24),
        window_end=datetime.now(),
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123",
    )
    db_session.add(run)
    await db_session.commit()

    db_session.add_all(
        [
            DigestItem(
                run_id=run.id,
                user_id=user.id,
                folder_id=None,
                category_name="Unclassified",
                entry_id=entry_one.id,
                code="n01",
                rank=1,
                score=95.0,
                title_zh="第一条新闻",
                summary_zh="第一条摘要",
                fulltext_zh="第一条正文",
            ),
            DigestItem(
                run_id=run.id,
                user_id=user.id,
                folder_id=None,
                category_name="Unclassified",
                entry_id=entry_two.id,
                code="n02",
                rank=2,
                score=90.0,
                title_zh="第二条新闻",
                summary_zh="第二条摘要",
                fulltext_zh="第二条正文",
            ),
        ]
    )
    await db_session.commit()

    from glean_api.config import settings

    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")
    monkeypatch.setattr(settings, "feishu_dispatcher_outbox_dir", str(tmp_path))

    payload = {
        "message_id": "om_message_multiple_codes",
        "chat_id": "chat_test_123",
        "text": "@Glean n01 n02",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
    }
    headers = {"Authorization": "Bearer test_internal_token"}

    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["code"] == "n01,n02"
    assert entry_one.id in data["entry_id"]
    assert entry_two.id in data["entry_id"]
    with open(data["outbox_file"], encoding="utf-8") as f:
        content = f.read()
        assert "【n01 · 第一条新闻】" in content
        assert "第一条正文" in content
        assert "【n02 · 第二条新闻】" in content
        assert "第二条正文" in content
        assert "\n\n---\n\n" in content

    liked_result = await db_session.execute(
        select(UserEntry).where(
            UserEntry.user_id == user.id,
            UserEntry.entry_id.in_([entry_one.id, entry_two.id]),
        )
    )
    liked_states = liked_result.scalars().all()
    assert len(liked_states) == 2
    assert all(state.is_liked is True for state in liked_states)


@pytest.mark.asyncio
async def test_internal_feishu_callback_no_code_writes_help_reply(
    client: AsyncClient, setup_feishu_config, monkeypatch, tmp_path
):
    """Mentioned messages that do not contain a code should not fail silently."""
    from glean_api.config import settings

    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")
    monkeypatch.setattr(settings, "feishu_dispatcher_outbox_dir", str(tmp_path))

    payload = {
        "message_id": "om_message_no_code",
        "chat_id": "chat_test_123",
        "text": "@Glean detail please",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
    }
    headers = {"Authorization": "Bearer test_internal_token"}

    response = await client.post("/api/internal/feishu/messages", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "no_code_found"
    assert data["outbox_file"] is not None
    with open(data["outbox_file"], encoding="utf-8") as f:
        assert "@Glean n01" in f.read()


@pytest.mark.asyncio
async def test_internal_feishu_callback_date_prefixed_code_selects_historical_run(
    client: AsyncClient, db_session, setup_feishu_config, monkeypatch, tmp_path
):
    """MMDD+nXX resolves that day's run while bare nXX still resolves latest run."""
    user = User(
        id="user_test_123",
        email="feishu.user@example.com",
        name="Feishu User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()

    feed = Feed(url="https://feed.example.com/date", title="Feishu Date Feed")
    db_session.add(feed)
    await db_session.commit()

    today_entry = Entry(
        feed_id=feed.id,
        url="https://article.example.com/today",
        title="Today News",
        content="<p>today</p>",
        embedding_status="done",
    )
    yesterday_entry = Entry(
        feed_id=feed.id,
        url="https://article.example.com/yesterday",
        title="Yesterday News",
        content="<p>yesterday</p>",
        embedding_status="done",
    )
    db_session.add_all([today_entry, yesterday_entry])
    await db_session.commit()

    timezone = ZoneInfo("Asia/Singapore")
    today = datetime.now(timezone)
    yesterday = today - timedelta(days=1)
    today_created_at = datetime(today.year, today.month, today.day, 8, 0, tzinfo=timezone)
    yesterday_created_at = datetime(
        yesterday.year, yesterday.month, yesterday.day, 8, 0, tzinfo=timezone
    )

    today_run = DigestRun(
        user_id=user.id,
        window_start=today_created_at - timedelta(hours=24),
        window_end=today_created_at,
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123",
        created_at=today_created_at.astimezone(UTC),
    )
    yesterday_run = DigestRun(
        user_id=user.id,
        window_start=yesterday_created_at - timedelta(hours=24),
        window_end=yesterday_created_at,
        status="sent",
        target_channel="feishu",
        feishu_chat_id="chat_test_123",
        created_at=yesterday_created_at.astimezone(UTC),
    )
    db_session.add_all([today_run, yesterday_run])
    await db_session.commit()

    db_session.add_all(
        [
            DigestItem(
                run_id=today_run.id,
                user_id=user.id,
                folder_id=None,
                category_name="Unclassified",
                entry_id=today_entry.id,
                code="n01",
                rank=1,
                score=95.0,
                title_zh="今日新闻",
                summary_zh="今日摘要",
                fulltext_zh="今日正文",
            ),
            DigestItem(
                run_id=yesterday_run.id,
                user_id=user.id,
                folder_id=None,
                category_name="Unclassified",
                entry_id=yesterday_entry.id,
                code="n01",
                rank=1,
                score=90.0,
                title_zh="昨日新闻",
                summary_zh="昨日摘要",
                fulltext_zh="昨日正文",
            ),
        ]
    )
    await db_session.commit()

    from glean_api.config import settings

    monkeypatch.setattr(settings, "feishu_dispatcher_callback_token", "test_internal_token")
    monkeypatch.setattr(settings, "feishu_dispatcher_outbox_dir", str(tmp_path))
    headers = {"Authorization": "Bearer test_internal_token"}

    date_prefixed_payload = {
        "message_id": "om_message_yesterday",
        "chat_id": "chat_test_123",
        "text": f"@Glean {yesterday_created_at.strftime('%m%d')} n01",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
    }
    response = await client.post(
        "/api/internal/feishu/messages", json=date_prefixed_payload, headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["entry_id"] == yesterday_entry.id
    with open(data["outbox_file"], encoding="utf-8") as f:
        assert "昨日正文" in f.read()

    bare_payload = {
        "message_id": "om_message_today",
        "chat_id": "chat_test_123",
        "text": "@Glean n01",
        "sender": {"open_id": "usr_test_123", "user_id": "usr_test_123"},
    }
    response = await client.post(
        "/api/internal/feishu/messages", json=bare_payload, headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["entry_id"] == today_entry.id
    with open(data["outbox_file"], encoding="utf-8") as f:
        assert "今日正文" in f.read()

    liked_result = await db_session.execute(
        select(UserEntry).where(
            UserEntry.user_id == user.id, UserEntry.entry_id == yesterday_entry.id
        )
    )
    assert liked_result.scalar_one().is_liked is True
