from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.core.config import get_settings
from app.core.exceptions import ProviderFetchError
from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class TrafikverketParkingAdapter(ProviderAdapter):
    PAGE_URL = "https://www.trafikverket.se/trafikinformation/vag/"
    OBJECT_TYPE = "Parking"
    SCHEMA_VERSION = "1.4"

    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()
        self.settings = get_settings()

    def fetch(self, run_context: RunContext) -> FetchResult:
        api_key = self.settings.trafikverket_api_key or self._extract_public_api_key()
        payload_xml = self._build_query(api_key)
        response = self.http.post(
            self.settings.trafikverket_api_url,
            data=payload_xml.encode("utf-8"),
            headers={
                "content-type": "text/xml",
                "accept": "application/json",
                "user-agent": self._user_agent(),
                "referer": self.PAGE_URL,
                "origin": "https://www.trafikverket.se",
            },
        )
        payload = response.json()
        records = self._extract_records(payload)
        return FetchResult(
            provider_name="trafikverket",
            fetched_at=utc_now(),
            request_url=self.settings.trafikverket_api_url,
            status_code=response.status_code,
            payload={"records": records, "source_url": self.PAGE_URL},
            request_headers={"content-type": "text/xml"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        for record in raw_payload.get("records", []):
            coordinates = normalize_coordinates(
                provider_name="trafikverket",
                record_id=str(record["id"]),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
            )
            services = self._dedupe_strings(
                [*record.get("equipment", []), *record.get("facilities", [])]
            )
            parking_features = self._dedupe_strings(
                [
                    *record.get("usage_scenarios", []),
                    *record.get("vehicle_characteristics", []),
                    *record.get("access_points", []),
                    *([f"open_status:{record['open_status']}"] if record.get("open_status") else []),
                    *([f"operation_status:{record['operation_status']}"] if record.get("operation_status") else []),
                    *([f"icon:{record['icon_id']}"] if record.get("icon_id") else []),
                ]
            )
            note_parts = [
                record.get("description"),
                record.get("location_description"),
                record.get("distance_to_nearest_city"),
                *coordinates.notes,
            ]
            rows.append(
                NormalizedFacility(
                    provider_name="trafikverket",
                    provider_record_id=str(record["id"]),
                    source_type="api",
                    source_url=str(record.get("source_url") or raw_payload.get("source_url") or self.PAGE_URL),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="trafikverket"),
                    facility_name=str(record["name"]),
                    facility_brand="Trafikverket",
                    category="parking",
                    subcategories=["rest_area"],
                    latitude=coordinates.latitude,
                    longitude=coordinates.longitude,
                    formatted_address=self._build_formatted_address(record),
                    street=None,
                    city=None,
                    region=None,
                    postal_code=None,
                    country_code="se",
                    phone=str(record.get("operator_phone") or "") or None,
                    opening_hours=str(record.get("open_status") or "") or None,
                    services=services,
                    parking_features=parking_features,
                    heavy_vehicle_relevance=(
                        "truckparking" in {value.replace("_", "").replace(" ", "") for value in record.get("usage_scenarios", [])}
                        or any("lorry" in value for value in record.get("vehicle_characteristics", []))
                    ),
                    electric_charging_relevance="electricchargingstation" in {
                        value.replace("_", "").replace(" ", "") for value in record.get("equipment", [])
                    },
                    confidence_score=max(0.0, 0.93 + coordinates.confidence_adjustment),
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(
                        ["trafikverket", record.get("id"), coordinates.latitude, coordinates.longitude]
                    ),
                    verified_status="unverified",
                    notes="; ".join(value for value in note_parts if value) or None,
                )
            )
            issues.extend(coordinates.issues)
        return rows, issues

    def _extract_public_api_key(self) -> str:
        response = self.http.get(
            self.PAGE_URL,
            headers={"user-agent": self._user_agent()},
        )
        match = re.search(r'apikey="([0-9a-f]{32})"', response.text)
        if match is None:
            raise ProviderFetchError("trafikverket map page did not expose a trafikinfo api key")
        return match.group(1)

    @classmethod
    def _build_query(cls, api_key: str) -> str:
        return f"""
<REQUEST>
  <LOGIN authenticationkey="{api_key}" />
  <QUERY objecttype="{cls.OBJECT_TYPE}" schemaversion="{cls.SCHEMA_VERSION}">
    <FILTER>
      <AND>
        <EQ name="Deleted" value="false" />
        <EQ name="IconId" value="restArea" />
      </AND>
    </FILTER>
  </QUERY>
</REQUEST>
""".strip()

    @classmethod
    def _extract_records(cls, payload: dict[str, Any]) -> list[dict[str, Any]]:
        results = payload.get("RESPONSE", {}).get("RESULT", [])
        records: list[dict[str, Any]] = []
        for result in results:
            for record in result.get("Parking", []):
                flattened = cls._flatten_record(record)
                if flattened is not None:
                    records.append(flattened)
        return records

    @classmethod
    def _flatten_record(cls, record: dict[str, Any]) -> dict[str, Any] | None:
        record_id = record.get("Id")
        name = record.get("Name")
        if not record_id or not name:
            return None
        latitude, longitude = cls._parse_wgs84_point(record.get("Geometry", {}).get("WGS84"))
        operator = record.get("Operator") or {}
        return {
            "id": str(record_id),
            "name": str(name),
            "latitude": latitude,
            "longitude": longitude,
            "description": cls._clean_text(record.get("Description")),
            "location_description": cls._clean_text(record.get("LocationDescription")),
            "distance_to_nearest_city": cls._clean_text(record.get("DistanceToNearestCity")),
            "icon_id": cls._clean_text(record.get("IconId")),
            "open_status": cls._clean_text(record.get("OpenStatus")),
            "operation_status": cls._clean_text(record.get("OperationStatus")),
            "usage_scenarios": [str(value).strip().lower() for value in record.get("UsageSenario", []) if str(value).strip()],
            "equipment": cls._flatten_equipment(record.get("Equipment", [])),
            "facilities": cls._flatten_facilities(record.get("Facilities", [])),
            "vehicle_characteristics": cls._flatten_vehicle_characteristics(record.get("VehicleCharacteristics", [])),
            "access_points": cls._flatten_access_points(record.get("ParkingAccess", [])),
            "operator_name": cls._clean_text(operator.get("Name")),
            "operator_phone": cls._clean_text(operator.get("ContactTelephoneNumber")),
            "operator_email": cls._clean_text(operator.get("ContactEmail")),
            "source_url": cls.PAGE_URL,
        }

    @staticmethod
    def _parse_wgs84_point(value: Any) -> tuple[float | None, float | None]:
        if not value:
            return None, None
        match = re.search(r"POINT\s*\(\s*([0-9.+-]+)\s+([0-9.+-]+)\s*\)", str(value))
        if match is None:
            return None, None
        longitude = float(match.group(1))
        latitude = float(match.group(2))
        return latitude, longitude

    @staticmethod
    def _flatten_equipment(entries: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []
        for entry in entries:
            item_type = str(entry.get("Type") or "").strip()
            accessibility = str(entry.get("Accessibility") or "").strip()
            if item_type:
                values.append(item_type.lower())
            if accessibility:
                values.append(accessibility.lower())
        return values

    @staticmethod
    def _flatten_facilities(entries: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []
        for entry in entries:
            item_type = str(entry.get("Type") or "").strip()
            accessibility = str(entry.get("Accessibility") or "").strip()
            if item_type:
                values.append(item_type.lower())
            if accessibility:
                values.append(accessibility.lower())
        return values

    @staticmethod
    def _flatten_vehicle_characteristics(entries: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []
        for entry in entries:
            vehicle_type = str(entry.get("VehicleType") or "").strip()
            spaces = entry.get("NumberOfSpaces")
            if vehicle_type:
                if spaces is None:
                    values.append(vehicle_type.lower())
                else:
                    values.append(f"{vehicle_type.lower()}:{spaces}")
        return values

    @staticmethod
    def _flatten_access_points(entries: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []
        for entry in entries:
            wgs84 = str(entry.get("WGS84") or "").strip()
            if wgs84:
                values.append(f"access:{wgs84}")
        return values

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _dedupe_strings(values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            item = str(value).strip().lower()
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    @staticmethod
    def _build_formatted_address(record: dict[str, Any]) -> str | None:
        values = [
            str(record.get("location_description") or "").strip(),
            str(record.get("distance_to_nearest_city") or "").strip(),
        ]
        parts = [value for value in values if value]
        return ", ".join(parts) or None

    @staticmethod
    def _user_agent() -> str:
        return (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="trafikverket",
            source_type="api",
            base_url=self.settings.trafikverket_api_url,
            category="parking",
            trust_rank=10,
        )

    def supports_incremental(self) -> bool:
        return True

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=30, burst_size=3)
