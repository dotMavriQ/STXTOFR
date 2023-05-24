from __future__ import annotations

from datetime import datetime
from typing import Any

from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class EspressoHouseAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "id": "eh-1",
                    "name": "Espresso House Klarabergsgatan",
                    "address": "Klarabergsgatan 50",
                    "city": "Stockholm",
                    "postal_code": "111 21",
                    "country_code": "SE",
                    "latitude": 59.3324,
                    "longitude": 18.0586,
                    "phone": "+46812345678",
                    "opening_hours": "Mon-Fri 06:30-20:00",
                    "services": ["coffee", "food"],
                    "source_url": "https://myespressohouse.com/beproud/api/CoffeeShop/v2",
                }
            ]
        }
        return FetchResult(
            provider_name="espresso_house",
            fetched_at=datetime.utcnow(),
            request_url="https://myespressohouse.com/beproud/api/CoffeeShop/v2",
            status_code=200,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            rows.append(
                NormalizedFacility(
                    provider_name="espresso_house",
                    provider_record_id=str(record["id"]),
                    source_type="api",
                    source_url=str(record.get("source_url")),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="espresso_house"),
                    facility_name=str(record["name"]),
                    facility_brand="Espresso House",
                    category="coffee_shop",
                    latitude=float(record["latitude"]),
                    longitude=float(record["longitude"]),
                    formatted_address=", ".join(
                        value for value in [record.get("address"), record.get("city"), record.get("postal_code")] if value
                    ),
                    street=str(record.get("address") or "") or None,
                    city=str(record.get("city") or "") or None,
                    postal_code=str(record.get("postal_code") or "") or None,
                    country_code=str(record.get("country_code") or "").lower() or None,
                    phone=str(record.get("phone") or "") or None,
                    opening_hours=str(record.get("opening_hours") or "") or None,
                    services=list(record.get("services", [])),
                    heavy_vehicle_relevance=False,
                    confidence_score=0.9,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["espresso_house", record.get("id")]),
                    verified_status="unverified",
                )
            )
        return rows, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="espresso_house",
            source_type="api",
            base_url="https://myespressohouse.com/beproud/api/CoffeeShop/v2",
            category="coffee_shop",
            trust_rank=35,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=20, burst_size=2)

