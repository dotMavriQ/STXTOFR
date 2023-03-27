from __future__ import annotations

from datetime import datetime
from typing import Any

from app.normalization.models import NormalizationIssue, NormalizedFacility
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.circlek.mapper import map_circlek_station


class CircleKAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "site_id": "1001",
                    "name": "Circle K Arlandastad",
                    "street": "Cederströms Slinga 21B",
                    "city": "Arlandastad",
                    "postal_code": "195 61",
                    "region": "Stockholms Län",
                    "country_code": "SE",
                    "latitude": 59.60697,
                    "longitude": 17.896358,
                    "phones": ["+46859511410", "+46859511411"],
                    "fuels": ["diesel", "hvo100", "bensin 95"],
                    "services": ["toalett", "truckdiesel"],
                    "opening_hours": "Mon-Fri 06:00-22:00",
                    "source_url": "https://www.circlek.se/station-search",
                }
            ]
        }
        return FetchResult(
            provider_name="circlek",
            fetched_at=datetime.utcnow(),
            request_url="https://www.circlek.se/station-search",
            status_code=200,
            payload=payload,
            request_headers={"x-requested-with": "XMLHttpRequest"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        facilities = [map_circlek_station(record, fetched_at=fetched_at) for record in raw_payload.get("records", [])]
        return facilities, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="circlek",
            source_type="api",
            base_url="https://www.circlek.se/station-search",
            category="fuel_station",
            trust_rank=20,
        )

    def supports_incremental(self) -> bool:
        return False

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=10, burst_size=1)

