from __future__ import annotations

from datetime import datetime
from typing import Any

from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class PreemAdapter(ProviderAdapter):
    def fetch(self, run_context: RunContext) -> FetchResult:
        payload = {
            "records": [
                {
                    "id": "preem-1",
                    "name": "Preem Falkoping Rastvagen",
                    "address": "Rastvägen 4",
                    "city": "Falköping",
                    "postal_code": "521 40",
                    "country_code": "SE",
                    "latitude": 58.173,
                    "longitude": 13.55,
                    "phone": "+4651512345",
                    "services": ["truck_parking", "shower"],
                    "fuel_types": ["diesel", "hvo100", "adblue"],
                    "opening_hours": "Mon-Sun 00:00-24:00",
                    "source_url": "https://www.preem.se/api/Stations/AllStations",
                }
            ]
        }
        return FetchResult(
            provider_name="preem",
            fetched_at=datetime.utcnow(),
            request_url="https://www.preem.se/api/Stations/AllStations",
            status_code=200,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            rows.append(
                NormalizedFacility(
                    provider_name="preem",
                    provider_record_id=str(record["id"]),
                    source_type="api",
                    source_url=str(record.get("source_url")),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="preem"),
                    facility_name=str(record["name"]),
                    facility_brand="Preem",
                    category="fuel_station",
                    subcategories=["truck_stop"],
                    latitude=float(record["latitude"]),
                    longitude=float(record["longitude"]),
                    formatted_address=", ".join(
                        value for value in [record.get("address"), record.get("city"), record.get("postal_code")] if value
                    ),
                    street=str(record.get("address") or "") or None,
                    city=str(record.get("city") or "") or None,
                    region=None,
                    postal_code=str(record.get("postal_code") or "") or None,
                    country_code=str(record.get("country_code") or "").lower() or None,
                    phone=str(record.get("phone") or "") or None,
                    opening_hours=str(record.get("opening_hours") or "") or None,
                    services=list(record.get("services", [])),
                    fuel_types=list(record.get("fuel_types", [])),
                    heavy_vehicle_relevance=True,
                    confidence_score=0.96,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["preem", record.get("id"), record.get("latitude"), record.get("longitude")]),
                    verified_status="unverified",
                )
            )
        return rows, []

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="preem",
            source_type="api",
            base_url="https://www.preem.se/api/Stations/AllStations",
            category="fuel_station",
            trust_rank=15,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=15, burst_size=2)

