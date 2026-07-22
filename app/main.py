from contextlib import asynccontextmanager
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.api.routes import router
from app.bootstrap.application import build_application_state
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.storage_retention import cleanup_storage, maybe_run_startup_cleanup
from app.infrastructure.persistence.database_maintenance import run_database_maintenance


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings)
    state = build_application_state(settings)
    for name, value in state.items():
        setattr(app.state, name, value)
    app.state.startup_storage_cleanup = maybe_run_startup_cleanup(settings)
    app.state.startup_database_maintenance = run_database_maintenance(settings, dry_run=False)

    scheduler = None
    ai_worker_task = None
    if settings.ai_worker_enabled:
        ai_worker_task = asyncio.create_task(state["ai_research_worker"].run(), name="ai-research-worker")
        app.state.ai_worker_task = ai_worker_task
    if settings.enable_scheduler:
        scheduler = AsyncIOScheduler(timezone=settings.timezone)
        scheduler.add_job(state["macro_service"].latest, "interval", minutes=30, id="macro_latest")
        scheduler.add_job(state["event_service"].upcoming, "interval", minutes=15, id="events_upcoming")
        scheduler.add_job(
            lambda: cleanup_storage(settings, category="all", dry_run=False),
            "interval",
            hours=max(settings.storage_cleanup_interval_hours, 1),
            id="storage_retention_cleanup",
            max_instances=1,
            coalesce=True,
        )
        if settings.research_scheduler_enabled:
            pre_hour, pre_minute = (int(item) for item in settings.research_premarket_time.split(":"))
            post_hour, post_minute = (int(item) for item in settings.research_postmarket_time.split(":"))
            if settings.research_premarket_enabled:
                scheduler.add_job(
                    lambda: state["research_scheduler"].evaluate("premarket"), "cron",
                    hour=pre_hour, minute=pre_minute, id="research_premarket", max_instances=1, coalesce=True,
                )
            if settings.research_session_enabled:
                scheduler.add_job(
                    lambda: state["research_scheduler"].evaluate("session"), "interval",
                    minutes=settings.research_session_interval_minutes, id="research_session",
                    max_instances=1, coalesce=True,
                )
            if settings.research_postmarket_enabled:
                scheduler.add_job(
                    lambda: state["research_scheduler"].evaluate("postmarket"), "cron",
                    hour=post_hour, minute=post_minute, id="research_postmarket", max_instances=1, coalesce=True,
                )
            if settings.research_event_triggers_enabled:
                for trigger in (
                    "pre_event", "post_release", "speech_outcome", "earnings_post_release",
                    "temporary_source_retry",
                ):
                    scheduler.add_job(
                        lambda selected=trigger: state["research_scheduler"].evaluate(selected),
                        "interval", minutes=settings.research_session_interval_minutes,
                        id=f"research_{trigger}", max_instances=1, coalesce=True,
                    )
            if settings.research_news_enabled:
                scheduler.add_job(
                    lambda: state["research_scheduler"].evaluate("news_refresh"), "interval",
                    minutes=settings.ai_run_window_news_minutes, id="research_news_refresh",
                    max_instances=1, coalesce=True,
                )
        scheduler.add_job(
            lambda: run_database_maintenance(settings, dry_run=False),
            "interval",
            hours=max(settings.storage_cleanup_interval_hours, 1),
            id="database_retention_cleanup",
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        app.state.scheduler = scheduler

    yield

    if scheduler:
        scheduler.shutdown(wait=False)
    if ai_worker_task:
        state["ai_research_worker"].stop()
        try:
            await asyncio.wait_for(
                ai_worker_task,
                timeout=settings.ai_worker_shutdown_timeout_seconds,
            )
        except TimeoutError:
            ai_worker_task.cancel()
            await asyncio.gather(ai_worker_task, return_exceptions=True)


app = FastAPI(
    title="AI-MARKET-DATA-SERVICE",
    version="0.1.0",
    description="Normalized macro and economic event data service for AI-TRADER.",
    lifespan=lifespan,
)
app.include_router(router)
