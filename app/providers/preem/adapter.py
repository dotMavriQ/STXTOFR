from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from app.core.exceptions import ProviderFetchError
from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class PreemAdapter(ProviderAdapter):
    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        list_response = self.http.get("https://www.preem.se/page-data/stationer/page-data.json")
        list_payload = list_response.json()
        nodes = list_payload.get("result", {}).get("data", {}).get("allDatoCmsStationPage", {}).get("nodes", [])
        slugs = [
            str(node.get("page", {}).get("slug") or "")
            for node in nodes
            if isinstance(node, dict) and str(node.get("page", {}).get("slug") or "").startswith("/stationer/")
        ]
        with ThreadPoolExecutor(max_workers=8) as executor:
            records = [record for record in executor.map(self._fetch_station_detail, slugs) if record]
        payload = {"records": records}
        return FetchResult(
            provider_name="preem",
            fetched_at=utc_now(),
            request_url="https://www.preem.se/page-data/stationer/page-data.json",
            status_code=list_response.status_code,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        for record in raw_payload.get("records", []):
            coordinates = normalize_coordinates(
                provider_name="preem",
                record_id=str(record["id"]),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
            )
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
                    latitude=coordinates.latitude,
                    longitude=coordinates.longitude,
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
                    confidence_score=max(0.0, 0.96 + coordinates.confidence_adjustment),
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["preem", record.get("id"), coordinates.latitude, coordinates.longitude]),
                    verified_status="unverified",
                    notes="; ".join(coordinates.notes) or None,
                )
            )
            issues.extend(coordinates.issues)
        return rows, issues

    def _fetch_station_detail(self, slug: str) -> dict[str, object] | None:
        page_data_url = f"https://www.preem.se/page-data{slug.rstrip('/')}/page-data.json"
        try:
            response = self.http.get(page_data_url)
            payload = response.json()
        except ProviderFetchError:
            return None
        station = payload.get("result", {}).get("data", {}).get("datoCmsStationPage")
        if not isinstance(station, dict):
            return None
        opening_hours = self._format_opening_hours(station.get("openingHours"))
        services = [
            key
            for key in ("carWashAutomatic", "carWashSelfService", "foodAndBeverages", "trailerRental")
            if station.get(key)
        ]
        if station.get("adaptedFor24mVehicles"):
            services.append("adapted_for_24m_vehicles")
        if station.get("saifaConnected"):
            services.append("saifa_connected")
        return {
            "id": station.get("stationCode"),
            "name": station.get("stationName"),
            "address": station.get("streetAddress"),
            "city": station.get("city"),
            "postal_code": station.get("postalCode"),
            "country_code": "SE",
            "latitude": station.get("latitude"),
            "longitude": station.get("longitude"),
            "phone": station.get("phoneNumber"),
            "services": services,
            "fuel_types": [entry.get("name") for entry in station.get("fuelTypes", []) if isinstance(entry, dict) and entry.get("name")],
            "opening_hours": opening_hours,
            "source_url": f"https://www.preem.se{slug}",
        }

    @staticmethod
    def _format_opening_hours(values: Any) -> str | None:
        if not isinstance(values, dict):
            return None
        weekday = PreemAdapter._build_day_hours(values.get("openingHourWeekday"), values.get("closeHourWeekday"))
        saturday = PreemAdapter._build_day_hours(values.get("openingHourSaturday"), values.get("closeHourSaturday"))
        sunday = PreemAdapter._build_day_hours(values.get("openingHourSunday"), values.get("closeHourSunday"))
        parts = []
        if weekday:
            parts.append(f"Mon-Fri {weekday}")
        if saturday:
            parts.append(f"Sat {saturday}")
        if sunday:
            parts.append(f"Sun {sunday}")
        return "; ".join(parts) or None

    @staticmethod
    def _build_day_hours(open_from: Any, open_to: Any) -> str | None:
        open_value = str(open_from or "").strip()
        close_value = str(open_to or "").strip()
        if not open_value or not close_value:
            return None
        return f"{open_value[:5]}-{close_value[:5]}"

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="preem",
            source_type="api",
            base_url="https://www.preem.se/page-data/stationer/page-data.json",
            category="fuel_station",
            trust_rank=15,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=15, burst_size=2)
