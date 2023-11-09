from __future__ import annotations

from datetime import datetime
from typing import Any

from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class IDSAdapter(ProviderAdapter):
    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        response = self.http.get("https://ids.q8.com/en/get/stations.json")
        payload = response.json()
        return FetchResult(
            provider_name="ids",
            fetched_at=utc_now(),
            request_url="https://ids.q8.com/en/get/stations.json",
            status_code=response.status_code,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        stations = raw_payload.get("Stations", {}).get("Station", [])
        for record in stations:
            if str(record.get("Country") or "").lower() != "sweden":
                continue
            services = self._as_list(record.get("Services", {}).get("Service"))
            street, postal_code, city = self._split_address_parts(
                str(record.get("Address_line_1") or ""),
                str(record.get("Address_line_2") or ""),
            )
            coordinates = normalize_coordinates(
                provider_name="ids",
                record_id=str(record["StationId"]),
                latitude=record.get("XCoordinate"),
                longitude=record.get("YCoordinate"),
            )
            operational_notes = self._build_operational_notes(record)
            opening_hours = self._extract_opening_hours(record)
            note_parts = [*operational_notes, *coordinates.notes]
            rows.append(
                NormalizedFacility(
                    provider_name="ids",
                    provider_record_id=str(record["StationId"]),
                    source_type="feed",
                    source_url=f"https://ids.q8.com{record.get('NodeURL')}" if record.get("NodeURL") else None,
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="ids"),
                    facility_name=str(record["Name"]),
                    facility_brand="IDS",
                    category="fuel_station",
                    subcategories=["truck_stop"],
                    latitude=coordinates.latitude,
                    longitude=coordinates.longitude,
                    formatted_address=", ".join(
                        value for value in [street, city, postal_code] if value
                    ),
                    street=street,
                    city=city,
                    region=None,
                    postal_code=postal_code,
                    country_code="se",
                    phone=str(record.get("Phone") or "") or None,
                    opening_hours=opening_hours,
                    services=[service for service in services if service not in {"diesel", "adblue", "hvo100"}],
                    fuel_types=[service for service in services if service in {"diesel", "adblue", "hvo100", "cng", "lng", "cbg", "lbg", "lbg50"}],
                    heavy_vehicle_relevance=True,
                    confidence_score=max(0.0, 0.9 + coordinates.confidence_adjustment),
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["ids", record.get("StationId"), coordinates.latitude, coordinates.longitude]),
                    verified_status="unverified",
                    notes="; ".join(value for value in note_parts if value) or None,
                )
            )
            issues.extend(coordinates.issues)
        return rows, issues

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).lower() for item in value]
        return [str(value).lower()]

    @staticmethod
    def _split_address_parts(address_1: str, address_2: str) -> tuple[str | None, str | None, str | None]:
        street = address_1 or None
        if not address_2:
            return street, None, None
        cleaned = address_2.strip()
        parts = cleaned.split()
        if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
            return street, f"{parts[0]} {parts[1]}", " ".join(parts[2:])
        if len(parts) >= 2 and parts[0].replace("-", "").isdigit():
            return street, parts[0], " ".join(parts[1:])
        return street, None, cleaned

    @staticmethod
    def _extract_opening_hours(record: dict[str, Any]) -> str | None:
        candidate_keys = (
            "OpeningHours",
            "Openinghours",
            "OpeningHour",
            "OpeningHourText",
            "OpeningHoursText",
            "OpeningTime",
            "OpeningTimes",
            "Hours",
        )
        for key in candidate_keys:
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, list):
                parts = [str(item).strip() for item in value if str(item).strip()]
                if parts:
                    return "; ".join(parts)
            if isinstance(value, dict):
                parts = [f"{name}: {str(item).strip()}" for name, item in value.items() if str(item).strip()]
                if parts:
                    return "; ".join(parts)
        return None

    @staticmethod
    def _build_operational_notes(record: dict[str, Any]) -> list[str]:
        notes: list[str] = []
        state = str(record.get("State") or "").strip()
        if state:
            notes.append(f"state: {state.lower()}")
        if record.get("IsActive") is False:
            notes.append("inactive")
        lift = record.get("PossibilityToLift")
        if isinstance(lift, bool):
            notes.append(f"lift_available: {'yes' if lift else 'no'}")
        maintenance_from = str(record.get("MaintenanceFrom") or "").strip()
        maintenance_until = str(record.get("MaintenanceUntil") or "").strip()
        if maintenance_from and maintenance_until:
            notes.append(f"maintenance_window: {maintenance_from} to {maintenance_until}")
        elif maintenance_from:
            notes.append(f"maintenance_from: {maintenance_from}")
        elif maintenance_until:
            notes.append(f"maintenance_until: {maintenance_until}")
        return notes

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="ids",
            source_type="hybrid",
            base_url="https://ids.q8.com/en/get/stations.json",
            category="fuel_station",
            trust_rank=25,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=12, burst_size=2)
