"""
Daily News Digest background tasks.
"""

import zoneinfo
from datetime import datetime
from typing import Any

from glean_core import get_logger
from glean_core.schemas.config import DigestConfig, EmbeddingConfig, VectorizationStatus
from glean_core.services import DailyDigestService, SimpleScoreService, TypedConfigService
from glean_database.session import get_session_context

logger = get_logger(__name__)


async def _resolve_score_service(ctx: dict[str, Any], session: Any) -> Any:
    """
    Resolve ScoreService (using Milvus) or fallback to SimpleScoreService.
    """
    config_service = TypedConfigService(session)
    config = await config_service.get(EmbeddingConfig)

    milvus_client = ctx.get("milvus_client")
    if milvus_client and config.enabled and config.status in (
        VectorizationStatus.IDLE,
        VectorizationStatus.REBUILDING,
    ):
        try:
            from glean_vector.services.score_service import ScoreService
            return ScoreService(db_session=session, milvus_client=milvus_client)
        except Exception as e:
            logger.warning("Failed to initialize vector ScoreService, falling back to SimpleScoreService", extra={"error": str(e)})
            return SimpleScoreService(session)
    else:
        return SimpleScoreService(session)


async def generate_daily_digest(ctx: dict[str, Any], user_id: str | None = None) -> dict[str, Any]:
    """
    Worker task to generate and send daily digests.
    """
    logger.info("Starting generate_daily_digest task", extra={"user_id": user_id})

    async with get_session_context() as session:
        config_service = TypedConfigService(session)
        digest_config = await config_service.get(DigestConfig)

        if not digest_config.enabled:
            logger.info("Daily digest features are disabled in config")
            return {"status": "disabled"}

        # Determine target user IDs
        target_users = [user_id] if user_id else digest_config.user_ids
        if not target_users:
            logger.info("No target users configured for daily digest")
            return {"status": "no_users"}

        digest_service = DailyDigestService(session)
        score_service = await _resolve_score_service(ctx, session)

        runs_created = 0
        for uid in target_users:
            logger.info("Triggering digest run for user", extra={"user_id": uid})
            try:
                run = await digest_service.generate_digest_for_user(uid, score_service)
                if run:
                    runs_created += 1
            except Exception:
                logger.exception("Failed to generate digest for user", extra={"user_id": uid})

        return {"status": "success", "runs_created": runs_created}


async def scheduled_daily_digest(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Scheduled task to check and trigger daily digests.
    Runs every hour.
    """
    logger.info("Running scheduled_daily_digest check")

    async with get_session_context() as session:
        config_service = TypedConfigService(session)
        digest_config = await config_service.get(DigestConfig)

        if not digest_config.enabled:
            return {"status": "disabled"}

        # Parse local time for the configured timezone
        try:
            tz = zoneinfo.ZoneInfo(digest_config.timezone)
        except Exception as tz_err:
            logger.error("Invalid timezone configured, falling back to UTC", extra={"timezone": digest_config.timezone, "error": str(tz_err)})
            tz = zoneinfo.ZoneInfo("UTC")

        local_now = datetime.now(tz)

        # Check if the current hour matches the configured hour
        if local_now.hour != digest_config.daily_hour:
            logger.debug(
                "Skipping digest run: current hour does not match configured daily hour",
                extra={"current_hour": local_now.hour, "daily_hour": digest_config.daily_hour},
            )
            return {"status": "wrong_hour"}

        # Use Redis lock to make sure we only execute once per calendar day per user
        redis = ctx["redis"]
        local_date_str = local_now.strftime("%Y-%m-%d")

        triggered_users = []
        for uid in digest_config.user_ids:
            lock_key = f"digest:daily:{uid}:{local_date_str}"
            # Lock expires in 25 hours (90000 seconds) to safely cover the day
            was_locked = await redis.set(lock_key, "1", ex=90000, nx=True)
            if was_locked:
                logger.info("Acquired daily lock, queueing digest job", extra={"user_id": uid, "date": local_date_str})
                await redis.enqueue_job("generate_daily_digest", user_id=uid)
                triggered_users.append(uid)
            else:
                logger.info("Daily digest already run for user today", extra={"user_id": uid, "date": local_date_str})

        return {"status": "checked", "triggered_users": triggered_users}
