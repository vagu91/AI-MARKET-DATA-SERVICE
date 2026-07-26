from contextlib import asynccontextmanager
import asyncio
import logging
import uuid

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.api.routes import router
from app.bootstrap.application import build_application_state
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.storage_retention import cleanup_storage, maybe_run_startup_cleanup
from app.infrastructure.persistence.database_maintenance import run_database_maintenance
from app.services.execution_context import ExecutionContext


logger = logging.getLogger(__name__)
EVENT_CALENDAR_CATCHUP_ERROR_BACKOFF_MAX_SECONDS = 30


async def run_lifecycle_due_scan(state):
    scheduler = state["research_scheduler"]
    execution_context = ExecutionContext.explicit_ai(
        correlation_id="apscheduler-lifecycle-due-scanner",
        request_origin="research_scheduler",
        allow_live_providers=True,
    )
    return await asyncio.to_thread(
        scheduler.scan_due_items,
        owner="apscheduler-lifecycle-due-scanner",
        resolver=state["lifecycle_due_resolver"].resolve,
        ai_enqueue=lambda items: scheduler.enqueue_due_residuals(
            items,
            execution_context=execution_context,
        ),
        execution_context=execution_context,
    )


async def run_startup_lifecycle_catchup(
    state,
    *,
    correlation_id: str = "startup-lifecycle-catch-up",
):
    scheduler = state["research_scheduler"]
    execution_context = (
        ExecutionContext.provider_only(
            correlation_id=correlation_id,
            allow_live_providers=True,
        )
        if scheduler.settings.event_calendar_catchup_enabled
        else ExecutionContext.explicit_ai(
            correlation_id=correlation_id,
            request_origin="recovery",
            allow_live_providers=True,
        )
    )
    return await asyncio.to_thread(
        scheduler.startup_catch_up,
        resolver=state["lifecycle_due_resolver"].resolve,
        ai_enqueue=lambda items: scheduler.enqueue_due_residuals(
            items,
            execution_context=execution_context,
        ),
        execution_context=execution_context,
    )


async def run_event_calendar_catchup_loop(state):
    settings = state["settings"]
    scheduler = state["research_scheduler"]
    interval_seconds = max(
        int(settings.lifecycle_due_scanner_interval_seconds),
        1,
    )
    consecutive_failures = 0
    while settings.event_calendar_catchup_enabled:
        await asyncio.sleep(interval_seconds)
        correlation_id = f"event-calendar-catch-up-{uuid.uuid4()}"
        try:
            result = await run_startup_lifecycle_catchup(
                state,
                correlation_id=correlation_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            consecutive_failures += 1
            retry_delay_seconds = min(
                2 ** min(consecutive_failures - 1, 5),
                EVENT_CALENDAR_CATCHUP_ERROR_BACKOFF_MAX_SECONDS,
            )
            catch_up_status = "UNKNOWN"
            try:
                catch_up_status = (
                    scheduler.record_event_calendar_catchup_error(
                        correlation_id=correlation_id,
                        error=exc,
                        retry_delay_seconds=retry_delay_seconds,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "event calendar catch-up error telemetry failed; "
                    "correlation_id=%s original_error_type=%s",
                    correlation_id,
                    type(exc).__name__,
                )
            logger.exception(
                "event calendar catch-up tick failed; "
                "correlation_id=%s error_type=%s catch_up_status=%s "
                "retry_delay_seconds=%s",
                correlation_id,
                type(exc).__name__,
                catch_up_status,
                retry_delay_seconds,
            )
            await asyncio.sleep(retry_delay_seconds)
            continue
        consecutive_failures = 0
        runtime_status = str(result.get("status") or "UNKNOWN")
        checkpoint_status = str(
            result.get("catch_up_completion_status")
            or runtime_status
        )
        logger.info(
            "event calendar catch-up tick finished; "
            "correlation_id=%s status=%s checkpoint_status=%s",
            correlation_id,
            runtime_status,
            checkpoint_status,
        )


def run_research_scheduler_evaluation(state, trigger_name: str):
    scheduler = state["research_scheduler"]
    execution_context = ExecutionContext.explicit_ai(
        correlation_id=f"apscheduler-research-{trigger_name}",
        request_origin="research_scheduler",
        allow_live_providers=True,
    )
    return scheduler.evaluate(
        trigger_name,
        execution_context=execution_context,
    )


def run_market_context_sync_refresh(state):
    return state["market_context_sync_refresh_worker"].run_once(
        owner="apscheduler-market-context-sync-refresh",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings)
    state = build_application_state(settings)
    for name, value in state.items():
        setattr(app.state, name, value)
    app.state.startup_storage_cleanup = maybe_run_startup_cleanup(settings)
    app.state.startup_database_maintenance = run_database_maintenance(settings, dry_run=False)
    app.state.startup_lifecycle_catchup = (
        await run_startup_lifecycle_catchup(state)
    )

    scheduler = None
    ai_worker_task = None
    event_calendar_catchup_task = None
    if settings.event_calendar_catchup_enabled:
        event_calendar_catchup_task = asyncio.create_task(
            run_event_calendar_catchup_loop(state),
            name="event-calendar-provider-catch-up",
        )
        app.state.event_calendar_catchup_task = event_calendar_catchup_task
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
                    run_research_scheduler_evaluation, "cron",
                    args=[state, "premarket"],
                    hour=pre_hour, minute=pre_minute, id="research_premarket", max_instances=1, coalesce=True,
                )
            if settings.research_session_enabled:
                scheduler.add_job(
                    run_research_scheduler_evaluation, "interval",
                    args=[state, "session"],
                    minutes=settings.research_session_interval_minutes, id="research_session",
                    max_instances=1, coalesce=True,
                )
            if settings.research_postmarket_enabled:
                scheduler.add_job(
                    run_research_scheduler_evaluation, "cron",
                    args=[state, "postmarket"],
                    hour=post_hour, minute=post_minute, id="research_postmarket", max_instances=1, coalesce=True,
                )
            if settings.research_event_triggers_enabled:
                for trigger in (
                    "pre_event", "post_release", "speech_outcome", "earnings_post_release",
                    "temporary_source_retry",
                ):
                    scheduler.add_job(
                        run_research_scheduler_evaluation,
                        "interval", minutes=settings.research_session_interval_minutes,
                        args=[state, trigger],
                        id=f"research_{trigger}", max_instances=1, coalesce=True,
                    )
            if settings.research_news_enabled:
                scheduler.add_job(
                    run_research_scheduler_evaluation, "interval",
                    args=[state, "news_refresh"],
                    minutes=settings.ai_run_window_news_minutes, id="research_news_refresh",
                    max_instances=1, coalesce=True,
                )
            if settings.lifecycle_due_scanner_enabled:
                scheduler.add_job(
                    run_lifecycle_due_scan,
                    "interval",
                    args=[state],
                    seconds=settings.lifecycle_due_scanner_interval_seconds,
                    id="lifecycle_due_scanner",
                    max_instances=1,
                    coalesce=True,
                )
        scheduler.add_job(
            lambda: run_database_maintenance(settings, dry_run=False),
            "interval",
            hours=max(settings.storage_cleanup_interval_hours, 1),
            id="database_retention_cleanup",
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            run_market_context_sync_refresh,
            "interval",
            args=[state],
            seconds=max(settings.lifecycle_due_scanner_interval_seconds, 1),
            id="market_context_sync_refresh",
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
    if event_calendar_catchup_task:
        event_calendar_catchup_task.cancel()
        await asyncio.gather(
            event_calendar_catchup_task,
            return_exceptions=True,
        )


app = FastAPI(
    title="AI-MARKET-DATA-SERVICE",
    version="0.1.0",
    description="Normalized macro and economic event data service for AI-TRADER.",
    lifespan=lifespan,
)
app.include_router(router)
