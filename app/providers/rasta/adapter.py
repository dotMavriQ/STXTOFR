from __future__ import annotations

from datetime import datetime
from typing import Any

from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash
from app.providers.rasta.parser import clean_hours, extract_services


class RastaAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "slug": "arboga",
                    "name": "Rasta Arboga",
                    "city": "Arboga",
                    "description": "Vid E20 i rondellen Ekbacken, strax utanför Arboga ligger Rasta Arboga.",
                    "hours": "Måndag - Fredag 06:00 - 22:00",
                    "services_html": '<ul id="ikoner"><li class="restaurang dusch"></li><li class="bransle"></li></ul>',
                }
            ]
        }
        return FetchResult(
            provider_name="rasta",
            fetched_at=datetime.utcnow(),
            request_url="https://www.rasta.se/anlaggningar",
            status_code=200,
            payload=payload,
            request_headers={"user-agent": "STXTOFR"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        facilities: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            facilities.append(
                NormalizedFacility(
                    provider_name="rasta",
                    provider_record_id=str(record["slug"]),
                    source_type="scrape",
                    source_url=f"https://www.rasta.se/anlaggningar/{record['slug']}",
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="rasta"),
                    facility_name=str(record["name"]),
                    facility_brand="Rasta",
                    category="roadside_rest",
                    subcategories=["truck_stop"],
                    formatted_address=None,
                    street=None,
                    city=str(record.get("city") or "") or None,
                    region=None,
                    postal_code=None,
                    country_code="se",
                    phone=None,
                    opening_hours=clean_hours(str(record.get("hours") or "")),
                    services=extract_services(str(record.get("services_html") or "")),
                    fuel_types=[],
                    heavy_vehicle_relevance=True,
                    electric_charging_relevance=False,
                    confidence_score=0.55,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["rasta", record.get("slug"), record.get("city")]),
                    verified_status="unverified",
                    notes=str(record.get("description") or "") or None,
                )
            )
        return facilities, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="rasta",
            source_type="scrape",
            base_url="https://www.rasta.se/anlaggningar",
            category="roadside_rest",
            trust_rank=40,
        )

    def supports_incremental(self) -> bool:
        return False

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=6, burst_size=1)

