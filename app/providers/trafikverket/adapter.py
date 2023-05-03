from __future__ import annotations

from datetime import datetime
from typing import Any

from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class TrafikverketParkingAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "id": "tvp-1",
                    "name": "Aavajoki",
                    "latitude": 65.81801,
                    "longitude": 23.8300419,
                    "city": "Haparanda",
                    "country_code": "SE",
                    "description": "Tillgänglig i södergående färdriktning.",
                    "parking_features": ["playground", "toilet", "dumping_station"],
                    "services": ["rest_area"],
                    "source_url": "https://api.trafikinfo.trafikverket.se/v2/data.json",
                }
            ]
        }
        return FetchResult(
            provider_name="trafikverket",
            fetched_at=datetime.utcnow(),
            request_url="https://api.trafikinfo.trafikverket.se/v2/data.json",
            status_code=200,
            payload=payload,
            request_headers={"content-type": "text/xml"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            rows.append(
                NormalizedFacility(
                    provider_name="trafikverket",
                    provider_record_id=str(record["id"]),
                    source_type="api",
                    source_url=str(record.get("source_url")),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="trafikverket"),
                    facility_name=str(record["name"]),
                    facility_brand="Trafikverket",
                    category="parking",
                    subcategories=["rest_area"],
                    latitude=float(record["latitude"]),
                    longitude=float(record["longitude"]),
                    formatted_address=str(record.get("description") or "") or None,
                    city=str(record.get("city") or "") or None,
                    country_code=str(record.get("country_code") or "").lower() or None,
                    services=list(record.get("services", [])),
                    parking_features=list(record.get("parking_features", [])),
                    heavy_vehicle_relevance=True,
                    confidence_score=0.93,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["trafikverket", record.get("id"), record.get("latitude"), record.get("longitude")]),
                    verified_status="unverified",
                    notes=str(record.get("description") or "") or None,
                )
            )
        return rows, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="trafikverket",
            source_type="api",
            base_url="https://api.trafikinfo.trafikverket.se/v2/data.json",
            category="parking",
            trust_rank=10,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=30, burst_size=3)

