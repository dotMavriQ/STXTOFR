from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.geo import normalize_coordinates
from app.normalization.models import NormalizationIssue, NormalizedFacility
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.circlek.mapper import map_circlek_station


class CircleKAdapter(ProviderAdapter):
    INVALID_NAME_VALUES = {"", "none", "null", "undefined", "n/a"}
    FUEL_ALIASES = {
        "EU_ADBLUE": "adblue",
        "ADBLUE": "adblue",
        "EU_DIESEL": "diesel",
        "EU_MILES_DIESEL": "diesel",
        "EU_MILESPLUS_DIESEL": "diesel",
        "DIESEL": "diesel",
        "EU_BENZIN_95": "petrol_95",
        "EU_MILES_95": "petrol_95",
        "BENSIN 95": "petrol_95",
        "MILES 95": "petrol_95",
        "EU_MILESPLUS_98": "petrol_98",
        "MILES+ 98": "petrol_98",
        "EU_E85": "e85",
        "E85": "e85",
        "EU_EV_CHARGER": "fast_charging",
        "SNABBLADDNING": "fast_charging",
    }
    SERVICE_ALIASES = {
        "EU_TRUCKDIESEL_NETWORK": "truck_diesel",
        "TRUCKDIESEL": "truck_diesel",
        "EU_TRUCK_PARKING": "truck_parking",
        "LASTBILSPARKERING": "truck_parking",
        "EU_HIGH_SPEED_CHARGER": "fast_charging",
        "SNABBLADDNING EL": "fast_charging",
        "EU_MOBILE_PAYMENTS_FUEL": "mobile_payment_fuel",
        "EU_MOBILE_PAYMENTS_FUEL_BUSINESS": "mobile_payment_fuel_business",
        "EU_CAR_RENTAL": "car_rental",
        "EU_TRAILER_RENTAL": "trailer_rental",
        "EU_CARWASH": "car_wash",
        "EU_CARWASH_JETWASH": "self_service_car_wash",
        "EU_WINDSHIELD_LIQUID_ON_PUMP": "windshield_fluid",
        "EU_SERVICE_ZONE": "air_water",
        "EU_TOILETS_BOTH": "toilet",
        "TOALETT": "toilet",
        "EU_KFREEZE": "kfreeze",
        "EU_GAS": "lpg",
        "EU_BABY_CHANGING": "baby_changing",
        "EU_VACUUM_CLEANER": "vacuum",
    }

    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        response = self.http.get("https://www.circlek.se/station-search")
        payload = self._extract_station_payload(response.text)
        return FetchResult(
            provider_name="circlek",
            fetched_at=utc_now(),
            request_url="https://www.circlek.se/station-search",
            status_code=response.status_code,
            payload=payload,
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        facilities: list[NormalizedFacility] = []
        issues: list[NormalizationIssue] = []
        for record in raw_payload.get("records", []):
            record_id = str(record.get("site_id"))
            coordinates = normalize_coordinates(
                provider_name="circlek",
                record_id=record_id,
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
            )
            issues.extend(coordinates.issues)
            if not self._has_valid_station_name(record.get("name")):
                issues.append(
                    NormalizationIssue(
                        provider_name="circlek",
                        record_id=record_id,
                        message="station record is missing a valid display name and was skipped",
                        severity="warning",
                    )
                )
                continue
            facilities.append(
                map_circlek_station(
                    record,
                    fetched_at=fetched_at,
                    coordinates=coordinates,
                )
            )
        return facilities, issues

    def get_source_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="circlek",
            source_type="api",
            base_url="https://www.circlek.se/station-search",
            category="fuel_station",
            trust_rank=20,
        )

    def supports_incremental(self) -> bool:
        return False

    def get_rate_limit_policy(self) -> RateLimitPolicy:
        return RateLimitPolicy(requests_per_minute=10, burst_size=1)

    @staticmethod
    def _extract_station_payload(html: str) -> dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        settings_tag = soup.find("script", attrs={"data-drupal-selector": "drupal-settings-json"})
        if not settings_tag:
            return {"records": []}
        settings_text = settings_tag.get_text(strip=True)
        if not settings_text:
            return {"records": []}
        settings = json.loads(settings_text)
        station_results = settings.get("ck_sim_search", {}).get("station_results", {})
        if not isinstance(station_results, dict):
            return {"records": []}
        records: list[dict[str, object]] = []
        for site_id, site_bundle in station_results.items():
            if not isinstance(site_bundle, dict):
                continue
            base_key = f"/sites/{site_id}"
            base = site_bundle.get(base_key, {})
            location = site_bundle.get(f"{base_key}/location", {})
            addresses = site_bundle.get(f"{base_key}/addresses", {})
            business_info = site_bundle.get(f"{base_key}/business-info", {})
            contact = site_bundle.get(f"{base_key}/contact-details", {})
            fuels = site_bundle.get(f"{base_key}/fuels", {})
            services = site_bundle.get(f"{base_key}/services", {})
            opening = site_bundle.get(f"{base_key}/opening-info", {})
            address = CircleKAdapter._extract_physical_address(addresses)
            notes = CircleKAdapter._build_business_notes(business_info)
            records.append(
                {
                    "site_id": str(site_id),
                    "name": base.get("name"),
                    "street": address.get("street"),
                    "city": address.get("city"),
                    "postal_code": address.get("postal_code"),
                    "region": address.get("region"),
                    "country_code": address.get("country_code") or "SE",
                    "latitude": location.get("lat") or location.get("lng") and location.get("lat"),
                    "longitude": location.get("lon") or location.get("lng"),
                    "phones": CircleKAdapter._extract_contact_values(contact, "phones", "phone"),
                    "emails": CircleKAdapter._extract_contact_values(contact, "emails", "email"),
                    "fuels": CircleKAdapter._normalize_feature_list(fuels, kind="fuel"),
                    "services": CircleKAdapter._normalize_feature_list(services, kind="service"),
                    "opening_hours": CircleKAdapter._format_opening_info(opening),
                    "notes": notes,
                    "source_url": "https://www.circlek.se/station-search",
                }
            )
        return {"records": records}

    @staticmethod
    def _as_text_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, dict):
            return [str(item).strip() for item in value.values() if str(item).strip()]
        return [str(value).strip()] if str(value).strip() else []

    @staticmethod
    def _slugify(value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
        return slug

    @classmethod
    def _extract_physical_address(cls, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        if "PHYSICAL" in value and isinstance(value["PHYSICAL"], dict):
            physical = value["PHYSICAL"]
            return {
                "street": physical.get("street"),
                "city": physical.get("city"),
                "postal_code": physical.get("postalCode"),
                "region": physical.get("county") or physical.get("state"),
                "country_code": physical.get("country"),
            }
        return {
            "street": value.get("street"),
            "city": value.get("city"),
            "postal_code": value.get("zip") or value.get("postalCode"),
            "region": value.get("state") or value.get("county"),
            "country_code": value.get("countryCode") or value.get("country"),
        }

    @classmethod
    def _extract_contact_values(cls, value: Any, plural_key: str, singular_key: str) -> list[str]:
        if not isinstance(value, dict):
            return []
        direct = value.get(singular_key)
        plural = value.get(plural_key)
        candidates: list[str] = []
        candidates.extend(cls._as_text_list(direct))
        if isinstance(plural, dict):
            for nested in plural.values():
                candidates.extend(cls._as_text_list(nested))
        else:
            candidates.extend(cls._as_text_list(plural))
        return list(dict.fromkeys(item for item in candidates if item))

    @classmethod
    def _normalize_feature_list(cls, value: Any, *, kind: str) -> list[str]:
        alias_map = cls.FUEL_ALIASES if kind == "fuel" else cls.SERVICE_ALIASES
        items: list[str] = []
        raw_items = value
        if isinstance(value, dict) and "items" in value:
            raw_items = value.get("items")
        if not isinstance(raw_items, list):
            raw_items = cls._as_text_list(raw_items)
        for item in raw_items:
            if isinstance(item, dict):
                label = str(item.get("name") or item.get("displayName") or "").strip()
                display = str(item.get("displayName") or "").strip()
                key = label.upper()
                fallback = display or label
            else:
                label = str(item).strip()
                key = label.upper()
                fallback = label
            if not fallback:
                continue
            normalized = alias_map.get(key) or alias_map.get(fallback.upper()) or cls._slugify(fallback)
            if normalized:
                items.append(normalized)
        return list(dict.fromkeys(items))

    @classmethod
    def _format_opening_info(cls, value: Any) -> str | None:
        if not isinstance(value, dict):
            return str(value).strip() or None if value else None
        if value.get("text"):
            return str(value.get("text")).strip() or None
        if value.get("alwaysOpen"):
            return "24/7"
        sections: list[tuple[str, Any]] = [
            ("Store", value.get("openingTimesStore") or value.get("openingTimes")),
            ("Fuel", value.get("openingTimesFuel")),
            ("Rental", value.get("openingTimesRental")),
            ("Carwash", value.get("openingTimesCarwash")),
        ]
        parts: list[str] = []
        for label, block in sections:
            rendered = cls._format_opening_block(block)
            if rendered:
                parts.append(f"{label} {rendered}")
        return " | ".join(parts) or None

    @staticmethod
    def _format_opening_block(value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        mapping = [
            ("weekdays", "Mon-Fri"),
            ("saturday", "Sat"),
            ("sunday", "Sun"),
        ]
        parts: list[str] = []
        for key, label in mapping:
            entry = value.get(key)
            if not isinstance(entry, dict):
                continue
            open_value = str(entry.get("open") or "").strip()
            close_value = str(entry.get("close") or "").strip()
            if not open_value or not close_value:
                continue
            if open_value == "00:00" and close_value == "24:00":
                parts.append(f"{label} 24h")
            else:
                parts.append(f"{label} {open_value}-{close_value}")
        return "; ".join(parts) or None

    @classmethod
    def _build_business_notes(cls, value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        parts: list[str] = []
        station_format = str(value.get("stationFormat") or "").strip()
        cluster_name = str(value.get("clusterName") or "").strip()
        company_name = str(value.get("companyName") or "").strip()
        if station_format:
            parts.append(f"station_format: {station_format}")
        if cluster_name:
            parts.append(f"cluster: {cluster_name}")
        if company_name:
            parts.append(f"operator: {company_name}")
        if value.get("chainConvenience") is True:
            parts.append("chain_convenience")
        return "; ".join(parts) or None

    @classmethod
    def _has_valid_station_name(cls, value: Any) -> bool:
        cleaned = str(value or "").strip()
        return cleaned.lower() not in cls.INVALID_NAME_VALUES
