"""
Daily Digest Service.

Orchestrates candidate retrieval, real-time preference scoring,
summarization, database recording, and dispatching to Feishu.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from glean_core import get_logger
from glean_core.schemas.config import DigestConfig, FeishuConfig
from glean_core.services.article_language_service import ArticleLanguageService
from glean_core.services.feishu_bot_client import FeishuBotClient
from glean_core.services.typed_config_service import TypedConfigService
from glean_database.models import (
    DigestItem,
    DigestRun,
    Entry,
    Feed,
    Folder,
    Subscription,
    UserEntry,
)

logger = get_logger(__name__)


class DailyDigestService:
    """
    Orchestrates daily news digests.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.db = session
        self.config_service = TypedConfigService(session)

    async def _get_folder_tree_ids(self, folder_id: str, user_id: str) -> list[str]:
        """
        Recursively get all child folder IDs including the parent folder itself.
        """
        result_ids = [folder_id]

        async def get_children(parent_id: str) -> list[str]:
            stmt = select(Folder.id).where(
                Folder.parent_id == parent_id,
                Folder.user_id == user_id,
                Folder.type == "feed",
            )
            result = await self.db.execute(stmt)
            child_ids = [str(row[0]) for row in result.all()]

            all_ids = child_ids.copy()
            for child_id in child_ids:
                all_ids.extend(await get_children(child_id))
            return all_ids

        result_ids.extend(await get_children(folder_id))
        return result_ids

    async def load_top_level_feed_categories(self, user_id: str) -> dict[str, list[str]]:
        """
        Maps top-level category folder names to their descendant feed IDs.
        Unclassified subscriptions are mapped to "Unclassified".
        """
        # Find root folders
        stmt = select(Folder).where(
            Folder.user_id == user_id,
            Folder.parent_id.is_(None),
            Folder.type == "feed",
        )
        result = await self.db.execute(stmt)
        root_folders = result.scalars().all()

        category_to_feeds: dict[str, list[str]] = {}

        # 1. Folder-grouped subscriptions
        for folder in root_folders:
            folder_ids = await self._get_folder_tree_ids(folder.id, user_id)
            sub_stmt = select(Subscription.feed_id).where(
                Subscription.user_id == user_id,
                Subscription.folder_id.in_(folder_ids),
            )
            sub_res = await self.db.execute(sub_stmt)
            feed_ids = [str(row[0]) for row in sub_res.all()]
            if feed_ids:
                category_to_feeds[folder.name] = feed_ids

        # 2. Unclassified subscriptions
        unclass_stmt = select(Subscription.feed_id).where(
            Subscription.user_id == user_id,
            Subscription.folder_id.is_(None),
        )
        unclass_res = await self.db.execute(unclass_stmt)
        unclass_feed_ids = [str(row[0]) for row in unclass_res.all()]
        if unclass_feed_ids:
            category_to_feeds["Unclassified"] = unclass_feed_ids

        return category_to_feeds

    async def generate_digest_for_user(
        self,
        user_id: str,
        score_service: Any,
    ) -> DigestRun | None:
        """
        Generate and send a daily digest for a specific user.
        """
        # Load configs
        digest_config = await self.config_service.get(DigestConfig)
        feishu_config = await self.config_service.get(FeishuConfig)

        if not digest_config.enabled or not feishu_config.enabled:
            logger.info(
                "Digest or Feishu config is disabled",
                extra={"digest_enabled": digest_config.enabled, "feishu_enabled": feishu_config.enabled},
            )
            return None

        chat_id = feishu_config.news_chat_id
        if not chat_id:
            logger.error("Feishu news_chat_id is not configured, aborting digest run")
            return None

        # Determine time window
        now = datetime.now(UTC)
        window_start = now - timedelta(hours=digest_config.lookback_hours)
        window_end = now

        # Create DigestRun
        run = DigestRun(
            user_id=user_id,
            window_start=window_start,
            window_end=window_end,
            status="processing",
            target_channel="feishu",
            feishu_chat_id=chat_id,
        )
        self.db.add(run)
        await self.db.commit()
        await self.db.refresh(run)

        try:
            # Load categories & feeds mapping
            category_to_feeds = await self.load_top_level_feed_categories(user_id)
            if not category_to_feeds:
                logger.info("No subscribed feeds found for user", extra={"user_id": user_id})
                run.status = "sent"
                await self.db.commit()
                return run

            # Subquery to exclude previously sent entries within the lookback window
            lookback_limit = now - timedelta(days=digest_config.duplicate_lookback_days)
            sent_subq = select(DigestItem.entry_id).where(
                DigestItem.user_id == user_id,
                DigestItem.created_at >= lookback_limit,
            )

            # Helper objects
            lang_service = ArticleLanguageService(digest_config)
            feishu_client = FeishuBotClient(feishu_config)

            sent_items_to_save: list[DigestItem] = []
            dispatch_failures = 0
            code_counter = 1

            for category_name, feed_ids in category_to_feeds.items():
                logger.info(
                    "Processing candidates for category",
                    extra={"category": category_name, "feeds_count": len(feed_ids)},
                )

                # Fetch entry candidates
                stmt = select(Entry, Feed.title.label("feed_title")).join(Feed, Entry.feed_id == Feed.id).outerjoin(
                    UserEntry,
                    (Entry.id == UserEntry.entry_id) & (UserEntry.user_id == user_id),
                ).where(
                    Entry.feed_id.in_(feed_ids),
                    Entry.embedding_status == "done",
                    ~Entry.id.in_(sent_subq),
                )

                # Filter by publication window
                published_col = func.coalesce(Entry.published_at, Entry.created_at)
                stmt = stmt.where(published_col >= window_start, published_col < window_end)

                # Filter out read entries
                if digest_config.exclude_read_entries:
                    stmt = stmt.where((UserEntry.is_read.is_(False)) | (UserEntry.is_read.is_(None)))

                # Limit initial candidates for scoring
                stmt = stmt.order_by(published_col.desc()).limit(digest_config.candidate_limit_per_category)

                res = await self.db.execute(stmt)
                rows = res.all()
                if not rows:
                    continue

                entries = [row[0] for row in rows]
                feed_title_map = {str(row[0].id): str(row[1]) for row in rows}

                # Score candidates in batch
                scores: dict[str, float] = {}
                if score_service:
                    try:
                        scores_result = await score_service.batch_calculate_scores(user_id, entries)
                        for entry_id, score_val in scores_result.items():
                            if isinstance(score_val, dict):
                                scores[entry_id] = float(score_val.get("score", 50.0))
                            else:
                                scores[entry_id] = float(score_val)
                    except Exception as score_err:
                        logger.error("Batch scoring failed, using default fallback score", extra={"error": str(score_err)})
                        scores = {entry.id: 50.0 for entry in entries}
                else:
                    scores = {entry.id: 50.0 for entry in entries}

                # Sort candidates by preference score descending, then published_at descending
                scored_entries = []
                for entry in entries:
                    score = scores.get(entry.id, 50.0)
                    if digest_config.min_score is None or score >= digest_config.min_score:
                        scored_entries.append((entry, score))

                scored_entries.sort(key=lambda x: (x[1], x[0].published_at or x[0].created_at), reverse=True)

                # Pick top N
                selected = scored_entries[:digest_config.top_per_category]
                if not selected:
                    continue

                # Process summaries, translations and prepare DigestItems
                category_items: list[DigestItem] = []
                for rank, (entry, score) in enumerate(selected, start=1):
                    code = f"N{code_counter:02d}"
                    code_counter += 1

                    # Generate Chinese title and summary
                    logger.info("Summarizing article", extra={"entry_id": entry.id, "code": code})
                    summary_zh = await lang_service.summarize_to_zh(entry)

                    # We also translate the title using DeepSeek for a fully Chinese report
                    title_zh = entry.title
                    try:
                        title_prompt = f"请将以下文章英文标题翻译为中文，保持简洁专业，直接输出翻译结果，不要有任何其他解释：\n\n{entry.title}"
                        translated_title = await lang_service._call_llm(
                            "你是一个专业的翻译官，只需将文章标题翻译为中文。",
                            title_prompt,
                        )
                        if translated_title:
                            title_zh = translated_title
                    except Exception:
                        logger.warning("Title translation failed, using original", extra={"entry_id": entry.id})

                    # Find folder ID if any
                    sub_stmt = select(Subscription.folder_id).where(
                        Subscription.user_id == user_id,
                        Subscription.feed_id == entry.feed_id,
                    )
                    sub_res = await self.db.execute(sub_stmt)
                    folder_id = sub_res.scalar_one_or_none()

                    item = DigestItem(
                        run_id=run.id,
                        user_id=user_id,
                        folder_id=folder_id,
                        category_name=category_name,
                        entry_id=entry.id,
                        code=code,
                        rank=rank,
                        score=score,
                        title_zh=title_zh,
                        summary_zh=summary_zh,
                    )
                    category_items.append(item)

                # Send category message to Feishu (one Feishu message per category)
                if category_items:
                    # Construct Feishu post elements
                    post_content: list[list[dict[str, Any]]] = []

                    for item in category_items:
                        # Find original entry object to link it
                        orig_entry = next(entry for entry, _ in selected if entry.id == item.entry_id)
                        feed_title = feed_title_map.get(item.entry_id, "RSS Feed")
                        pub_time_str = ""
                        if orig_entry.published_at:
                            pub_time_str = orig_entry.published_at.strftime("%H:%M")
                        else:
                            pub_time_str = orig_entry.created_at.strftime("%H:%M")

                        post_content.append([
                            {"tag": "text", "text": f"{item.code}  "},
                            {"tag": "a", "text": item.title_zh, "href": orig_entry.url},
                        ])
                        post_content.append([
                            {"tag": "text", "text": f"评分：{item.score}  |  来源：{feed_title}  |  发布时间：{pub_time_str}"}
                        ])
                        post_content.append([
                            {"tag": "text", "text": f"摘要：{item.summary_zh}"}
                        ])
                        post_content.append([
                            {"tag": "text", "text": "回复 "},
                            {"tag": "text", "text": f"@{feishu_config.app_id or 'Glean'} {item.code}", "style": ["bold"]},
                            {"tag": "text", "text": " 获取中文原文\n\n"}
                        ])

                    # Dispatch to Feishu
                    title_text = f"【Glean Daily • {category_name} • {now.strftime('%Y-%m-%d')}】"
                    try:
                        feishu_msg_id = await feishu_client.send_rich_text_message(
                            chat_id=chat_id,
                            title=title_text,
                            content=post_content,
                        )
                        for item in category_items:
                            item.feishu_message_id = feishu_msg_id
                        sent_items_to_save.extend(category_items)
                    except Exception as dispatch_err:
                        dispatch_failures += 1
                        logger.error(
                            "Failed to dispatch Feishu message for category",
                            extra={"category": category_name, "error": str(dispatch_err)},
                        )

            # Persist items to database
            if sent_items_to_save:
                self.db.add_all(sent_items_to_save)
                run.status = "partial_failed" if dispatch_failures else "sent"
            else:
                run.status = "failed" if dispatch_failures else "sent"
                if dispatch_failures:
                    run.error_message = "All Feishu dispatch attempts failed"
                else:
                    logger.info("No items qualified for this digest run")

            await self.db.commit()
            return run

        except Exception as run_err:
            logger.exception("Digest run failed", extra={"run_id": run.id})
            run.status = "failed"
            run.error_message = str(run_err)
            await self.db.commit()
            return run
