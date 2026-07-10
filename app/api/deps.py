from fastapi import Request

from app.services.event_window_service import EventWindowService
from app.services.event_service import EventService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.macro_service import MacroService
from app.services.nasdaq_data_service import NasdaqDataService


def get_macro_service(request: Request) -> MacroService:
    return request.app.state.macro_service


def get_event_service(request: Request) -> EventService:
    return request.app.state.event_service


def get_event_window_service(request: Request) -> EventWindowService:
    return request.app.state.event_window_service


def get_nasdaq_data_service(request: Request) -> NasdaqDataService:
    return request.app.state.nasdaq_data_service


def get_enrichment_orchestrator(request: Request) -> EnrichmentOrchestrator:
    return request.app.state.enrichment_orchestrator
