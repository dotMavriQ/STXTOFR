from __future__ import annotations

from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class TRBAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "id": "trb-1",
                    "name": "TRB Helsingborg",
                    "city": "Helsingborg",
                    "latitude": 56.05,
                    "longitude": 12.70,
                    "description": "<p>Truck stop with card access</p>",
                    "phone": "+4642123999",
                    "source_url": "https://trb.se/wp-admin/admin-ajax.php",
                }
            ]
        }
        return FetchResult(
            provider_name="trb",
            fetched_at=datetime.utcnow(),
            request_url="https://trb.se/wp-admin/admin-ajax.php",
            status_code=200,
            payload=payload,
            request_headers={"x-requested-with": "XMLHttpRequest"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            description = BeautifulSoup(str(record.get("description") or ""), "lxml").get_text(" ", strip=True)
            rows.append(
                NormalizedFacility(
                    provider_name="trb",
                    provider_record_id=str(record["id"]),
                    source_type="feed",
                    source_url=str(record.get("source_url")),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="trb"),
                    facility_name=str(record["name"]),
                    facility_brand="TRB",
                    category="fuel_station",
                    subcategories=["truck_stop"],
                    latitude=float(record["latitude"]),
                    longitude=float(record["longitude"]),
                    city=str(record.get("city") or "") or None,
                    country_code="se",
                    phone=str(record.get("phone") or "") or None,
                    services=["truck_card_access"],
                    heavy_vehicle_relevance=True,
                    confidence_score=0.72,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["trb", record.get("id")]),
                    verified_status="unverified",
                    notes=description or None,
                )
            )
        return rows, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="trb",
            source_type="hybrid",
            base_url="https://trb.se/wp-admin/admin-ajax.php",
            category="fuel_station",
            trust_rank=45,
        )

    def supports_incremental(self) -> bool:
        return False

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=8, burst_size=1)

