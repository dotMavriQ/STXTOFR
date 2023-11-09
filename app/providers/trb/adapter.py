from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from app.core.exceptions import ProviderFetchError
from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class TRBAdapter(ProviderAdapter):
    STATION_PAGE_URL = "https://trb.se/hitta-tankstation/"
    WIDGET_JSON_URL_TEMPLATE = "https://cdn.storelocatorwidgets.com/json/{uid}-Swedish"

    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        page_response = self.http.get(
            self.STATION_PAGE_URL,
            headers=self._page_headers(),
        )
        uid = self._extract_widget_uid(page_response.text)
        request_headers = {
            "user-agent": self._page_headers()["user-agent"],
            "referer": self.STATION_PAGE_URL,
        }
        request_url = self.WIDGET_JSON_URL_TEMPLATE.format(uid=uid)
        params = {"callback": "slw"}
        status_code: int | None = None
        try:
            api_response = self.http.get(
                request_url,
                headers=self._widget_json_headers(),
                params=params,
                timeout=10,
            )
            raw_payload = api_response.text
            status_code = api_response.status_code
        except ProviderFetchError as exc:
            raw_payload = self._fetch_widget_payload_with_browser(uid=uid)
            if raw_payload is None:
                raise exc
            status_code = 200
            request_headers["x-trb-fetch-mode"] = "browser-fallback"

        payload = self._decode_widget_payload(raw_payload)
        records = self._extract_records(payload, uid=uid)
        if not records:
            raise ProviderFetchError("trb widget bootstrap feed did not return station records")
        return FetchResult(
            provider_name="trb",
            fetched_at=utc_now(),
            request_url=f"{request_url}?callback=slw",
            status_code=status_code or 200,
            payload={
                "uid": uid,
                "source_url": self.STATION_PAGE_URL,
                "widget_json_url": request_url,
                "records": records,
            },
            request_headers=request_headers,
        )

    def _fetch_widget_payload_with_browser(self, uid: str) -> str | None:
        return None

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        for record in raw_payload.get("records", []):
            description = BeautifulSoup(str(record.get("description") or ""), "html.parser").get_text(" ", strip=True)
            coordinates = normalize_coordinates(
                provider_name="trb",
                record_id=str(record["id"]),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
            )
            rows.append(
                NormalizedFacility(
                    provider_name="trb",
                    provider_record_id=str(record["id"]),
                    source_type="feed",
                    source_url=str(record.get("source_url") or raw_payload.get("source_url") or self.STATION_PAGE_URL),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="trb"),
                    facility_name=str(record["name"]),
                    facility_brand="TRB",
                    category="fuel_station",
                    subcategories=["truck_stop"],
                    latitude=coordinates.latitude,
                    longitude=coordinates.longitude,
                    formatted_address=self._format_address(record),
                    street=str(record.get("address") or "") or None,
                    city=str(record.get("city") or "") or None,
                    region=str(record.get("region") or "") or None,
                    postal_code=str(record.get("postal_code") or "") or None,
                    country_code="se",
                    phone=str(record.get("phone") or "") or None,
                    opening_hours=str(record.get("opening_hours") or "") or None,
                    services=self._dedupe_strings(record.get("services")),
                    fuel_types=self._dedupe_strings(record.get("fuels")),
                    heavy_vehicle_relevance=True,
                    confidence_score=max(0.0, 0.72 + coordinates.confidence_adjustment),
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["trb", record.get("id"), coordinates.latitude, coordinates.longitude]),
                    verified_status="unverified",
                    notes="; ".join(value for value in [description or None, *coordinates.notes] if value) or None,
                )
            )
            issues.extend(coordinates.issues)
        return rows, issues

    @classmethod
    def _extract_widget_uid(cls, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.select_one("script#storelocatorscript[data-uid]")
        if script is None:
            raise ProviderFetchError("trb station page no longer exposes a store locator uid")
        uid = str(script.get("data-uid") or "").strip()
        if not uid:
            raise ProviderFetchError("trb store locator uid is empty")
        return uid

    @classmethod
    def _decode_widget_payload(cls, text: str) -> dict[str, Any]:
        body = text.strip()
        if not body:
            raise ValueError("empty trb widget response")
        for prefix in ("slwapi(", "slw("):
            if body.startswith(prefix):
                body = body[len(prefix):]
                if body.endswith(");"):
                    body = body[:-2]
                elif body.endswith(")"):
                    body = body[:-1]
                break
        return json.loads(body)

    @classmethod
    def _extract_records(cls, payload: dict[str, Any], uid: str) -> list[dict[str, Any]]:
        stores = payload.get("stores")
        if isinstance(stores, list):
            source_entries = stores
        else:
            locations = payload.get("locations", [])
            if not isinstance(locations, list):
                raise ValueError("trb widget payload did not contain a stores or locations list")
            source_entries = locations
        records: list[dict[str, Any]] = []
        for location in source_entries:
            record = cls._flatten_location(location, uid=uid)
            if record is not None:
                records.append(record)
        return records

    @classmethod
    def _flatten_location(cls, location: Any, uid: str) -> dict[str, Any] | None:
        if not isinstance(location, dict):
            return None
        data = location.get("data")
        if not isinstance(data, dict):
            data = {}

        name = cls._clean_text(location.get("name") or data.get("name"))
        store_id = location.get("storeid") or data.get("storeid") or location.get("id") or data.get("id")
        if not name or store_id is None:
            return None

        address = cls._first_value(
            data.get("address"),
            data.get("Address"),
            location.get("address"),
        )
        postal_code = cls._first_value(
            data.get("zip"),
            data.get("postal_code"),
            data.get("postcode"),
            data.get("post_code"),
        )
        phone = cls._first_value(
            data.get("phone"),
            data.get("tel"),
            location.get("tel"),
        )
        city = cls._first_value(
            data.get("city"),
            location.get("city"),
        )
        region = cls._first_value(
            data.get("region"),
            data.get("county"),
            data.get("state"),
        )
        opening_hours = cls._first_value(
            data.get("opening_hours"),
            data.get("hours"),
            data.get("openinghours"),
        )
        description = cls._clean_text(location.get("description") or data.get("description"))
        source_url = f"{cls.STATION_PAGE_URL}?uid={uid}"

        return {
            "id": str(store_id),
            "name": name,
            "latitude": cls._to_float(
                data.get("map_lat"),
                data.get("lat"),
                location.get("lat"),
            ),
            "longitude": cls._to_float(
                data.get("map_lng"),
                data.get("lng"),
                location.get("lng"),
            ),
            "address": address,
            "city": city,
            "postal_code": postal_code,
            "region": region,
            "phone": phone,
            "opening_hours": opening_hours,
            "services": cls._split_list_value(data.get("services")),
            "fuels": cls._split_list_value(data.get("fuels")),
            "description": description,
            "source_url": source_url,
        }

    @staticmethod
    def _page_headers() -> dict[str, str]:
        return {
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "accept-language": "sv-SE,sv;q=0.9,en;q=0.8",
        }

    @classmethod
    def _widget_json_headers(cls) -> dict[str, str]:
        return {
            **cls._page_headers(),
            "accept": "application/javascript, application/json, text/javascript, */*; q=0.01",
            "referer": cls.STATION_PAGE_URL,
        }

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        text = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
        return text or None

    @staticmethod
    def _first_value(*values: Any) -> str | None:
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    @staticmethod
    def _to_float(*values: Any) -> float | None:
        for value in values:
            if value is None or value == "":
                continue
            try:
                return float(str(value).replace(",", "."))
            except ValueError:
                continue
        return None

    @staticmethod
    def _split_list_value(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip().lower() for item in value if str(item).strip()]
        if isinstance(value, str):
            parts = re.split(r"[,\|;/]+", value)
            return [part.strip().lower() for part in parts if part.strip()]
        return []

    @staticmethod
    def _dedupe_strings(values: Any) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values or []:
            item = str(value).strip().lower()
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    @staticmethod
    def _format_address(record: dict[str, Any]) -> str | None:
        values = [
            str(record.get("address") or "").strip(),
            str(record.get("postal_code") or "").strip(),
            str(record.get("city") or "").strip(),
        ]
        parts = [value for value in values if value]
        return ", ".join(parts) or None

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="trb",
            source_type="hybrid",
            base_url=self.STATION_PAGE_URL,
            category="fuel_station",
            trust_rank=45,
        )

    def supports_incremental(self) -> bool:
        return False

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=8, burst_size=1)
