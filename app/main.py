from contextlib import asynccontextmanager

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


app = FastAPI(
    title="AI-MARKET-DATA-SERVICE",
    version="0.1.0",
    description="Normalized macro and economic event data service for AI-TRADER.",
    lifespan=lifespan,
)
app.include_router(router)
