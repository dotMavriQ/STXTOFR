from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.core.exceptions import ProviderFetchError
from app.core.http import HttpClient
from app.core.time import utc_now
from app.normalization.models import NormalizationIssue, NormalizedFacility, RawPayloadRef
from app.providers.base import FetchResult, ProviderAdapter, ProviderMetadata, RateLimitPolicy, RunContext
from app.providers.common import stable_hash
from app.providers.rasta.parser import (
    clean_hours,
    extract_contact_url,
    extract_listing_services,
    extract_marker_coordinates,
    extract_services,
    parse_opening_hours_tables,
    split_swedish_address,
)


class RastaAdapter(ProviderAdapter):
    LISTING_URL = "https://www.rasta.se/anlaggningar/"

    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    def fetch(self, run_context: RunContext) -> FetchResult:
        response = self.http.get(
            self.LISTING_URL,
            headers={
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                ),
                "accept-language": "sv-SE,sv;q=0.9,en;q=0.8",
            },
        )
        listing_payload = self._parse_listing_page(response.text)
        with ThreadPoolExecutor(max_workers=4) as executor:
            records = [record for record in executor.map(self._enrich_listing_record, listing_payload["records"]) if record]
        return FetchResult(
            provider_name="rasta",
            fetched_at=utc_now(),
            request_url="https://www.rasta.se/anlaggningar",
            status_code=response.status_code,
            payload={"records": records},
            request_headers={"user-agent": "Mozilla/5.0"},
        )

    def normalize(self, raw_payload: Any, fetched_at: datetime) -> tuple[list[NormalizedFacility], list[NormalizationIssue]]:
        facilities: list[NormalizedFacility] = []
        for record in raw_payload.get("records", []):
            facilities.append(
                NormalizedFacility(
                    provider_name="rasta",
                    provider_record_id=str(record["slug"]),
                    source_type="scrape",
                    source_url=str(record.get("detail_url") or f"https://www.rasta.se/{record['slug']}/"),
                    raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="rasta"),
                    facility_name=str(record["name"]),
                    facility_brand="Rasta",
                    category="roadside_rest",
                    subcategories=["truck_stop"],
                    latitude=record.get("latitude"),
                    longitude=record.get("longitude"),
                    formatted_address=", ".join(
                        value for value in [record.get("street"), record.get("postal_code"), record.get("city")] if value
                    )
                    or None,
                    street=str(record.get("street") or "") or None,
                    city=str(record.get("city") or "") or None,
                    region=None,
                    postal_code=str(record.get("postal_code") or "") or None,
                    country_code="se",
                    phone=str(record.get("phone") or "") or None,
                    opening_hours=clean_hours(str(record.get("hours") or "")),
                    services=self._merge_services(record),
                    fuel_types=[],
                    heavy_vehicle_relevance=True,
                    electric_charging_relevance=False,
                    confidence_score=0.78 if record.get("street") or record.get("phone") else 0.55,
                    freshness_ts=fetched_at,
                    normalized_hash=stable_hash(["rasta", record.get("slug"), record.get("city")]),
                    verified_status="unverified",
                    notes=str(record.get("description") or "") or None,
                )
            )
        return facilities, []

    @classmethod
    def _parse_listing_page(cls, html: str) -> dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[dict[str, object]] = []

        for marker in soup.select("div.acf-map div.marker"):
            info = marker.select_one("div.info-window")
            if info is None:
                continue
            title = info.select_one("h4")
            detail_link = info.select_one("a.knapp.las-mer")
            address = info.select_one("p.address")
            description = info.select_one("p.info-text")
            name = title.get_text(strip=True) if title else None
            detail_url = detail_link.get("href") if detail_link else ""
            slug = detail_url.rstrip("/").rsplit("/", 1)[-1] if detail_url else ""
            city = None
            if address is not None:
                parts = [part.strip() for part in address.get_text(" ", strip=True).split(",") if part.strip()]
                if len(parts) >= 2:
                    city = parts[-2]
            records.append(
                {
                    "slug": slug,
                    "name": name,
                    "city": city,
                    "description": description.get_text(" ", strip=True) if description else None,
                    "hours": None,
                    "services_html": str(info),
                    "listing_services": [],
                    "detail_url": detail_url or None,
                    "latitude": float(marker.get("data-lat")) if marker.get("data-lat") else None,
                    "longitude": float(marker.get("data-lng")) if marker.get("data-lng") else None,
                }
            )
        if records:
            return {"records": records}

        for card in soup.select("div.anlaggning"):
            anchor = card.select_one("a[href]")
            title = card.select_one("h3")
            if anchor is None or title is None:
                continue
            detail_url = urljoin(cls.LISTING_URL, str(anchor.get("href") or ""))
            slug = detail_url.rstrip("/").rsplit("/", 1)[-1]
            records.append(
                {
                    "slug": slug,
                    "name": title.get_text(" ", strip=True),
                    "city": None,
                    "description": None,
                    "hours": None,
                    "services_html": "",
                    "listing_services": extract_listing_services(card.get("class", [])),
                    "detail_url": detail_url,
                    "latitude": None,
                    "longitude": None,
                }
            )
        return {"records": records}

    def _enrich_listing_record(self, record: dict[str, object]) -> dict[str, object] | None:
        detail_url = str(record.get("detail_url") or "")
        if not detail_url:
            return record
        try:
            detail_response = self.http.get(detail_url)
        except ProviderFetchError:
            return record

        enriched = dict(record)
        enriched.update(self._parse_detail_page(detail_response.text, detail_url))

        contact_url = extract_contact_url(detail_response.text, detail_url) or urljoin(detail_url.rstrip("/") + "/", "kontakt/")
        try:
            contact_response = self.http.get(contact_url)
        except ProviderFetchError:
            return enriched
        enriched.update(self._parse_contact_page(contact_response.text))
        return enriched

    @staticmethod
    def _parse_detail_page(html: str, detail_url: str) -> dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        services_html = str(soup.select_one("ul#ikoner") or "")
        page_text = soup.select_one("div.page-text")
        description = None
        if page_text is not None:
            paragraphs = [clean_hours(paragraph.get_text(" ", strip=True)) for paragraph in page_text.select("p")]
            description = next((value for value in paragraphs if value), None)
        hours = parse_opening_hours_tables(html)
        latitude, longitude = extract_marker_coordinates(html)
        footer_text = soup.select_one("footer#footer p")
        street = postal_code = city = None
        if footer_text is not None:
            footer_value = clean_hours(footer_text.get_text(" ", strip=True))
            if footer_value and " - " in footer_value:
                _, _, tail = footer_value.partition(" - ")
                street, postal_code, city = split_swedish_address(tail)
        email_anchor = soup.select_one("footer#footer a.email[href^='mailto:']")
        return {
            "detail_url": detail_url,
            "services_html": services_html,
            "description": description,
            "hours": hours,
            "street": street,
            "postal_code": postal_code,
            "city": city,
            "email": email_anchor.get_text(" ", strip=True) if email_anchor else None,
            "latitude": latitude,
            "longitude": longitude,
        }

    @staticmethod
    def _parse_contact_page(html: str) -> dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        street = postal_code = city = None
        address_heading = next(
            (heading for heading in soup.select("h2") if clean_hours(heading.get_text(" ", strip=True)) == "Adress"),
            None,
        )
        if address_heading is not None:
            address_block = address_heading.find_next("p")
            if address_block is not None:
                parts = [clean_hours(text) for text in address_block.stripped_strings]
                parts = [value for value in parts if value]
                if len(parts) >= 2:
                    street = parts[-2]
                    postal_code, city = split_swedish_address(parts[-1])[1:]
        phones: list[str] = []
        email = None
        for item in soup.select("article#kontaktuppgifter ul li, article#kontaktuppgifter-mob ul li"):
            value = clean_hours(item.get_text(" ", strip=True))
            if not value:
                continue
            if "@" in value:
                email = value
                continue
            phone_match = re.search(r"(\d[\d\s-]{5,}\d)", value)
            if phone_match is not None:
                phones.append(clean_hours(phone_match.group(1)) or phone_match.group(1))
        hours = parse_opening_hours_tables(html)
        latitude, longitude = extract_marker_coordinates(html)
        return {
            "street": street,
            "postal_code": postal_code,
            "city": city,
            "phone": ", ".join(dict.fromkeys(phones)) or None,
            "email": email,
            "hours": hours,
            "latitude": latitude,
            "longitude": longitude,
        }

    @staticmethod
    def _merge_services(record: dict[str, object]) -> list[str]:
        html_services = extract_services(str(record.get("services_html") or ""))
        listing_services = [str(value).strip().lower() for value in record.get("listing_services", []) if str(value).strip()]
        return sorted(set([*html_services, *listing_services]))

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
