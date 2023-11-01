from functools import lru_cache

from fastapi import Header, HTTPException

from app.analysis.service import AnalysisService
from app.core.config import get_settings
from app.ingestion.service import IngestionService
from app.routing.publisher import build_publisher
from app.services.baserow import build_baserow_client
from app.services.curation import CurationService
from app.services.export_service import ExportService
from app.services.facility_view import FacilityViewService
from app.services.provider_status import ProviderStatusService
from app.services.provider_registry import ProviderRegistry, build_provider_registry
from app.storage.db import init_db
from app.storage.raw_archive import build_archive_backend
from app.storage.repository import InMemoryRepository, Repository, SQLRepository


def require_api_key(authorization: str | None = Header(default=None)) -> None:
    key = get_settings().api_key
    if not key:
        return
    if authorization != f"Bearer {key}":
        raise HTTPException(status_code=401, detail="missing or invalid API key")


@lru_cache()
def get_repository() -> Repository:
    settings = get_settings()
    if settings.repository_backend == "memory":
        return InMemoryRepository()
    init_db()
    return SQLRepository()


@lru_cache()
def get_registry() -> ProviderRegistry:
    return build_provider_registry()


@lru_cache()
def get_ingestion_service() -> IngestionService:
    return IngestionService(
        repository=get_repository(),
        registry=get_registry(),
        archive=build_archive_backend(get_repository()),
    )


@lru_cache()
def get_analysis_service() -> AnalysisService:
    return AnalysisService(repository=get_repository(), publisher=build_publisher())


@lru_cache()
def get_provider_status_service() -> ProviderStatusService:
    return ProviderStatusService(repository=get_repository())


@lru_cache()
def get_facility_view_service() -> FacilityViewService:
    return FacilityViewService(repository=get_repository())


@lru_cache()
def get_curation_service() -> CurationService:
    return CurationService(
        repository=get_repository(),
        facility_view_service=get_facility_view_service(),
        baserow_client=build_baserow_client(),
    )


@lru_cache()
def get_export_service() -> ExportService:
    return ExportService(
        repository=get_repository(),
        facility_view_service=get_facility_view_service(),
    )
