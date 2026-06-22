"""
Unit tests for Daily Digest Service and Article Language Service.
"""

from unittest.mock import AsyncMock

import pytest

from glean_core.schemas.config import DigestConfig
from glean_core.services.article_language_service import (
    ArticleLanguageService,
    clean_html_to_text,
    filter_detail_source_text,
)
from glean_core.services.daily_digest_service import DailyDigestService
from glean_database.models import Entry, Feed, Folder, Subscription, User


def test_clean_html_to_text():
    """Verify HTML tags stripping and link transformation."""
    raw_html = (
        "<h1>Hello World</h1>"
        "<p>This is a paragraph with <a href='https://example.com'>a link</a> and a second sentence.</p>"
        "<script>console.log('strip me')</script>"
        "<div>Some extra text in a div.</div>"
    )
    cleaned = clean_html_to_text(raw_html)
    assert "Hello World" in cleaned
    assert "a link (https://example.com)" in cleaned
    assert "console.log" not in cleaned
    assert "Some extra text in a div." in cleaned


def test_filter_detail_source_text_removes_trailing_newsletter_sections():
    """Verify unrelated newsletter roundup sections are not sent for detail translation."""
    source_text = "\n\n".join(
        [
            "Main article headline",
            "This paragraph belongs to the requested n13 article.",
            "AINews网站",
            "Subscribe and visit our site for more AI news.",
            "AI Twitter Recap",
            "A separate social recap that should not be returned.",
        ]
    )

    filtered = filter_detail_source_text(source_text, title="Main article headline")

    assert "This paragraph belongs to the requested n13 article." in filtered
    assert "AINews网站" not in filtered
    assert "AI Twitter Recap" not in filtered


@pytest.mark.asyncio
async def test_translate_fulltext_filters_unrelated_sections_before_llm(monkeypatch):
    """Verify the Feishu detail translation prompt receives only the requested article body."""
    config = DigestConfig(
        llm_api_key="fake-key",
        llm_base_url="https://api.deepseek.com",
        llm_model="deepseek-v4-flash",
    )
    service = ArticleLanguageService(config)

    mock_call = AsyncMock(return_value="正文翻译")
    monkeypatch.setattr(service, "_call_llm", mock_call)

    entry = Entry(
        title="Requested Article",
        summary=None,
        content=(
            "<article><h1>Requested Article</h1>"
            "<p>Requested article body.</p>"
            "<h2>AI Twitter Recap</h2>"
            "<p>Unrelated recap content.</p></article>"
        ),
    )

    result = await service.translate_fulltext_to_zh(entry)

    assert result == "正文翻译"
    user_prompt = mock_call.call_args.args[1]
    assert "Requested article body." in user_prompt
    assert "AI Twitter Recap" not in user_prompt
    assert "Unrelated recap content." not in user_prompt


@pytest.mark.asyncio
async def test_article_language_service_summarize(monkeypatch):
    """Verify that summarize falls back to title if API call fails or is mocked."""
    config = DigestConfig(
        llm_api_key="fake-key",
        llm_base_url="https://api.deepseek.com",
        llm_model="deepseek-v4-flash"
    )
    service = ArticleLanguageService(config)

    # Mock LLM API call
    mock_call = AsyncMock(return_value="这是测试摘要内容，长度足够长以满足摘要要求。")
    monkeypatch.setattr(service, "_call_llm", mock_call)

    entry = Entry(
        title="Test Title",
        summary="<p>Some test summary content to be summarized.</p>",
        content="<p>Full content text.</p>"
    )

    summary = await service.summarize_to_zh(entry)
    assert summary == "这是测试摘要内容，长度足够长以满足摘要要求。"
    assert mock_call.called


@pytest.mark.asyncio
async def test_daily_digest_service_classification(db_session):
    """Verify categorization logic in DailyDigestService."""
    # 1. Create a user
    user = User(
        email="digest.test@example.com",
        name="Digest User",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    # 2. Create folders: Root Folder "AI" and Child Folder "Study"
    root_folder = Folder(
        name="AI",
        type="feed",
        user_id=user.id,
        parent_id=None,
    )
    db_session.add(root_folder)
    await db_session.commit()
    await db_session.refresh(root_folder)

    child_folder = Folder(
        name="Study",
        type="feed",
        user_id=user.id,
        parent_id=root_folder.id,
    )
    db_session.add(child_folder)
    await db_session.commit()
    await db_session.refresh(child_folder)

    # 3. Create feeds and subscriptions
    feed1 = Feed(url="https://feed1.example.com", title="AI News")
    feed2 = Feed(url="https://feed2.example.com", title="Study Guide")
    feed3 = Feed(url="https://feed3.example.com", title="Unclassified News")
    db_session.add_all([feed1, feed2, feed3])
    await db_session.commit()
    await db_session.refresh(feed1)
    await db_session.refresh(feed2)
    await db_session.refresh(feed3)

    # Sub1 in root_folder, Sub2 in child_folder, Sub3 unclassified
    sub1 = Subscription(user_id=user.id, feed_id=feed1.id, folder_id=root_folder.id)
    sub2 = Subscription(user_id=user.id, feed_id=feed2.id, folder_id=child_folder.id)
    sub3 = Subscription(user_id=user.id, feed_id=feed3.id, folder_id=None)
    db_session.add_all([sub1, sub2, sub3])
    await db_session.commit()

    # 4. Invoke classification
    digest_service = DailyDigestService(db_session)
    categories = await digest_service.load_top_level_feed_categories(user.id)

    # Expect:
    # "AI" category has feeds [feed1.id, feed2.id]
    # "Unclassified" category has feeds [feed3.id]
    assert "AI" in categories
    assert feed1.id in categories["AI"]
    assert feed2.id in categories["AI"]

    assert "Unclassified" in categories
    assert feed3.id in categories["Unclassified"]
