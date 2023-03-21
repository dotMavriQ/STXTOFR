from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from app.normalization.models import NormalizationIssue, NormalizedFacility


@dataclass(frozen=True)
class ProviderMetadata:
    provider_name: str
    source_type: str
    base_url: str
    category: str
    trust_rank: int = 100


@dataclass(frozen=True)
class RateLimitPolicy:
    requests_per_minute: int
    burst_size: int = 1


@dataclass(frozen=True)
class FetchResult:
    provider_name: str
    fetched_at: datetime
    request_url: str
    status_code: int
    payload: Any
    request_headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RunContext:
    mode: str = "full"
    checkpoint: str | None = None
    dry_run: bool = False


class ProviderAdapter(ABC):
    @abstractmethod
    def fetch(self, run_context: RunContext) -> FetchResult:
        raise NotImplementedError

    @abstractmethod
    def normalize(
        self, raw_payload: Any, fetched_at: datetime
    ) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        raise NotImplementedError

    @abstractmethod
    def get_source_metadata(self) -> ProviderMetadata:
        raise NotImplementedError

    @abstractmethod
    def supports_incremental(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_rate_limit_policy(self) -> RateLimitPolicy:
        raise NotImplementedError

    def describe(self) -> dict[str, object]:
        metadata = self.get_source_metadata()
        return {
            "provider": metadata.provider_name,
            "source_type": metadata.source_type,
            "base_url": metadata.base_url,
            "category": metadata.category,
            "supports_incremental": self.supports_incremental(),
            "rate_limit": self.get_rate_limit_policy().requests_per_minute,
        }

    def iter_records(self, payload: Any) -> Iterable[Any]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if "records" in payload:
                return payload["records"]
            return payload.values()
        return []

