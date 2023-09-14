from functools import lru_cache

from app.analysis.service import AnalysisService
from app.ingestion.service import IngestionService
from app.routing.publisher import build_publisher
from app.services.provider_status import ProviderStatusService
from app.services.provider_registry import ProviderRegistry, build_provider_registry
from app.storage.raw_archive import build_archive_backend
from app.storage.repository import InMemoryRepository


@lru_cache()
def get_repository() -> InMemoryRepository:
    return InMemoryRepository()


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
