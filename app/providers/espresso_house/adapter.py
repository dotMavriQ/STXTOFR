from __future__ import annotations

from typing import Any

from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash


class EspressoHouseAdapter(ProviderAdapter):
    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        response = self.http.get("https://myespressohouse.com/beproud/api/CoffeeShop/v2")
        payload = response.json()
        return FetchResult(
            provider_name="espresso_house",
            fetched_at=utc_now(),
            request_url="https://myespressohouse.com/beproud/api/CoffeeShop/v2",
            status_code=response.status_code,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        rows: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        for record in raw_payload.get("coffeeShops", []):
            if str(record.get("country") or "").lower() != "sweden":
                continue
            coordinates = normalize_coordinates(
                provider_name="espresso_house",
                record_id=str(record["coffeeShopId"]),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
            )
            street = ", ".join(
                value for value in [record.get("address1"), record.get("address2")] if value
            )
            opening_hours = self._compose_opening_hours(record)
            services = ["coffee"]
            if record.get("preorderOnline"):
                services.append("preorder")
            if record.get("expressCheckout"):
                services.append("express_checkout")
            if not record.get("takeAwayOnly"):
                services.append("food")
            else:
                services.append("takeaway_only")
            if record.get("wifi"):
                services.append("wifi")
            if record.get("childFriendly"):
                services.append("child_friendly")
            if record.get("handicapFriendly"):
                services.append("accessible")
            if record.get("limitedInventory"):
                services.append("limited_inventory")
            rows.append(
                NormalizedFacility(
                    provider_name="espresso_house",
                    provider_record_id=str(record["coffeeShopId"]),
                    source_type="api",
                    source_url="https://myespressohouse.com/beproud/api/CoffeeShop/v2",
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="espresso_house"),
                    facility_name=str(record["coffeeShopName"]),
                    facility_brand="Espresso House",
                    category="coffee_shop",
                    latitude=coordinates.latitude,
                    longitude=coordinates.longitude,
                    formatted_address=", ".join(
                        value for value in [street, record.get("city"), record.get("postalCode")] if value
                    ),
                    street=street or None,
                    city=str(record.get("city") or "") or None,
                    postal_code=str(record.get("postalCode") or "") or None,
                    country_code="se",
                    phone=str(record.get("phoneNumber") or "") or None,
                    opening_hours=opening_hours,
                    services=self._dedupe_services(services),
                    heavy_vehicle_relevance=False,
                    confidence_score=max(0.0, 0.9 + coordinates.confidence_adjustment),
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["espresso_house", record.get("coffeeShopId")]),
                    verified_status="unverified",
                    notes="; ".join(coordinates.notes) or None,
                )
            )
            issues.extend(coordinates.issues)
        return rows, issues

    @staticmethod
    def _format_opening_hours(entries: Any) -> str | None:
        if not isinstance(entries, list):
            return None
        parts: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            weekday = str(entry.get("weekDay") or "").strip()
            open_from = str(entry.get("openFrom") or "").strip()
            open_to = str(entry.get("openTo") or "").strip()
            if not weekday or not open_from or not open_to:
                continue
            parts.append(f"{weekday} {open_from[:5]}-{open_to[:5]}")
        return "; ".join(parts) or None

    @classmethod
    def _compose_opening_hours(cls, record: dict[str, Any]) -> str | None:
        parts: list[str] = []

        regular_hours = cls._format_opening_hours(record.get("openingHours"))
        if regular_hours:
            parts.append(regular_hours)

        irregular_hours = cls._format_irregular_opening_hours(record.get("irregularOpeningHours"))
        if irregular_hours:
            parts.append(irregular_hours)

        return "; ".join(parts) or None

    @staticmethod
    def _format_irregular_opening_hours(entries: Any) -> str | None:
        if not isinstance(entries, list):
            return None
        parts: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            date = str(entry.get("date") or entry.get("day") or "").strip()
            open_from = str(entry.get("openFrom") or "").strip()
            open_to = str(entry.get("openTo") or "").strip()
            label = str(entry.get("label") or entry.get("description") or entry.get("reason") or "").strip()
            if date and open_from and open_to:
                segment = f"{date} {open_from[:5]}-{open_to[:5]}"
            elif date and label:
                segment = f"{date} {label}"
            elif date:
                segment = date
            elif label:
                segment = label
            else:
                continue
            parts.append(f"special: {segment}")
        return "; ".join(parts) or None

    @staticmethod
    def _dedupe_services(services: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for service in services:
            if service in seen:
                continue
            seen.add(service)
            ordered.append(service)
        return ordered

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
