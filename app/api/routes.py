from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.analysis.service import AnalysisService
from app.api.dependencies import (
    get_analysis_service,
    get_ingestion_service,
    get_provider_status_service,
    get_registry,
    get_repository,
)
from app.api.schemas import GapAnalysisRequest, ProviderStatusResponse, RunCreateRequest
from app.core.exceptions import RecordNotFound
from app.ingestion.service import IngestionService
from app.services.provider_status import ProviderStatusService
from app.services.provider_registry import ProviderRegistry
from app.storage.repository import InMemoryRepository


api_router = APIRouter()


@api_router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@api_router.get("/providers")
def list_providers(registry: ProviderRegistry = Depends(get_registry)) -> list[dict[str, object]]:
    return [provider.describe() for provider in registry.list()]


@api_router.get("/providers/{provider}/status", response_model=ProviderStatusResponse)
def provider_status(
    provider: str,
    registry: ProviderRegistry = Depends(get_registry),
    provider_status_service: ProviderStatusService = Depends(get_provider_status_service),
) -> ProviderStatusResponse:
    adapter = registry.get(provider)
    return ProviderStatusResponse(**provider_status_service.build_status(adapter))


@api_router.post("/runs")
def create_runs(
    payload: RunCreateRequest,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    providers = payload.providers or service.registry.names()
    runs = [service.run_provider(name, mode=payload.mode, dry_run=payload.dry_run) for name in providers]
    return {"runs": runs}


@api_router.post("/runs/{provider}")
def create_provider_run(
    provider: str,
    payload: RunCreateRequest,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    run = service.run_provider(provider, mode=payload.mode, dry_run=payload.dry_run)
    return {"run": run}


@api_router.get("/runs")
def list_runs(
    provider: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    status: str | None = Query(default=None),
    repository: InMemoryRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_runs(provider=provider, mode=mode, status=status)


@api_router.get("/runs/{run_id}")
def get_run(run_id: int, repository: InMemoryRepository = Depends(get_repository)) -> dict[str, object]:
    run = repository.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@api_router.post("/reprocess/{record_id}")
def reprocess_record(
    record_id: int,
    service: IngestionService = Depends(get_ingestion_service),
) -> dict[str, object]:
    try:
        return service.reprocess_raw_payload(record_id)
    except RecordNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@api_router.get("/facilities")
def list_facilities(
    provider: str | None = Query(default=None),
    category: str | None = Query(default=None),
    city: str | None = Query(default=None),
    verified: bool | None = Query(default=None),
    repository: InMemoryRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_facilities(
        provider=provider,
        category=category,
        city=city,
        verified=verified,
    )


@api_router.get("/facilities/{facility_id}")
def get_facility(
    facility_id: int, repository: InMemoryRepository = Depends(get_repository)
) -> dict[str, object]:
    facility = repository.get_facility(facility_id)
    if not facility:
        raise HTTPException(status_code=404, detail="facility not found")
    return facility


@api_router.get("/gaps")
def list_gaps(
    region: str | None = Query(default=None),
    category: str | None = Query(default=None),
    stale_only: bool = Query(default=False),
    repository: InMemoryRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_gaps(region=region, category=category, stale_only=stale_only)


@api_router.post("/analysis/gaps")
def run_gap_analysis(
    payload: GapAnalysisRequest,
    service: AnalysisService = Depends(get_analysis_service),
) -> dict[str, object]:
    findings = service.run_gap_analysis(
        region=payload.region,
        category=payload.category,
        stale_only=payload.stale_only,
    )
    return {"findings": findings}
