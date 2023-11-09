from __future__ import annotations

import json
from dataclasses import asdict, fields
from pathlib import Path
from typing import Any, Callable

from app.core.time import utc_now
from app.normalization.models import NormalizedFacility
from app.providers.base import RunContext
from app.providers.circlek.adapter import CircleKAdapter
from app.providers.espresso_house.adapter import EspressoHouseAdapter
from app.providers.ids.adapter import IDSAdapter
from app.providers.preem.adapter import PreemAdapter
from app.providers.rasta.adapter import RastaAdapter
from app.providers.trafikverket.adapter import TrafikverketParkingAdapter
from app.providers.trb.adapter import TRBAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
NORMALIZED_FIELD_NAMES = [
    field.name for field in fields(NormalizedFacility)
    if field.name != "raw_payload_ref"
]
RAW_TO_NORMALIZED_ALIASES: dict[str, list[str]] = {
    "id": ["provider_record_id"],
    "site_id": ["provider_record_id"],
    "StationId": ["provider_record_id"],
    "coffeeShopId": ["provider_record_id"],
    "name": ["facility_name"],
    "Name": ["facility_name"],
    "coffeeShopName": ["facility_name"],
    "address": ["street", "formatted_address"],
    "address1": ["street", "formatted_address"],
    "address2": ["street", "formatted_address"],
    "Address_line_1": ["street", "formatted_address"],
    "Address_line_2": ["postal_code", "city", "formatted_address"],
    "phones": ["phone"],
    "Phone": ["phone"],
    "phoneNumber": ["phone"],
    "preorderOnline": ["services"],
    "expressCheckout": ["services"],
    "takeAwayOnly": ["services"],
    "wifi": ["services"],
    "childFriendly": ["services"],
    "handicapFriendly": ["services"],
    "limitedInventory": ["services"],
    "fuels": ["fuel_types", "electric_charging_relevance"],
    "Services": ["services", "fuel_types"],
    "equipment": ["services", "parking_features", "electric_charging_relevance"],
    "facilities": ["services"],
    "usage_scenarios": ["parking_features", "heavy_vehicle_relevance"],
    "vehicle_characteristics": ["parking_features", "heavy_vehicle_relevance"],
    "access_points": ["parking_features"],
    "source_url": ["source_url"],
    "NodeURL": ["source_url"],
    "opening_hours": ["opening_hours"],
    "openingHours": ["opening_hours"],
    "irregularOpeningHours": ["opening_hours"],
    "open_status": ["opening_hours", "parking_features"],
    "description": ["notes"],
    "location_description": ["formatted_address", "notes"],
    "distance_to_nearest_city": ["formatted_address", "notes"],
    "operator_phone": ["phone"],
    "operator_name": ["notes"],
    "operator_email": ["notes"],
    "State": ["notes"],
    "IsActive": ["notes"],
    "MaintenanceFrom": ["notes"],
    "MaintenanceUntil": ["notes"],
    "PossibilityToLift": ["notes"],
    "country": ["country_code"],
    "Country": ["country_code"],
    "postalCode": ["postal_code"],
    "XCoordinate": ["latitude"],
    "YCoordinate": ["longitude"],
}


def _read_json(name: str) -> Any:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _load_circlek_payload() -> tuple[str, dict[str, Any]]:
    adapter = CircleKAdapter()
    html = (FIXTURES_DIR / "circlek_station_search_page.html").read_text(encoding="utf-8")
    return "tests/fixtures/circlek_station_search_page.html", adapter._extract_station_payload(html)


def _load_espresso_house_payload() -> tuple[str, dict[str, Any]]:
    return "tests/fixtures/espresso_house_fetch.json", _read_json("espresso_house_fetch.json")


def _load_ids_payload() -> tuple[str, dict[str, Any]]:
    return "tests/fixtures/ids_stations.json", _read_json("ids_stations.json")


def _load_rasta_payload() -> tuple[str, dict[str, Any]]:
    adapter = RastaAdapter()

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text
            self.status_code = 200

    def fake_get(url: str, **kwargs: object) -> FakeResponse:
        if url == "https://www.rasta.se/anlaggningar/":
            return FakeResponse((FIXTURES_DIR / "rasta_listing.html").read_text(encoding="utf-8"))
        if url == "https://www.rasta.se/arboga/":
            return FakeResponse((FIXTURES_DIR / "rasta_detail.html").read_text(encoding="utf-8"))
        if url == "https://www.rasta.se/arboga/kontakt/":
            return FakeResponse((FIXTURES_DIR / "rasta_contact.html").read_text(encoding="utf-8"))
        raise AssertionError(f"unexpected rasta fixture url: {url}")

    adapter.http.get = fake_get  # type: ignore[assignment]
    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))
    return "tests/fixtures/rasta_listing.html + tests/fixtures/rasta_detail.html + tests/fixtures/rasta_contact.html", fetch.payload


def _load_trafikverket_payload() -> tuple[str, dict[str, Any]]:
    raw_payload = _read_json("trafikverket_parking.json")
    return "tests/fixtures/trafikverket_parking.json", {
        "records": TrafikverketParkingAdapter._extract_records(raw_payload),
        "source_url": TrafikverketParkingAdapter.PAGE_URL,
    }


def _load_trb_payload() -> tuple[str, dict[str, Any]]:
    return "tests/fixtures/trb_locations.json", _read_json("trb_locations.json")


def _patch_preem_http_get(adapter: PreemAdapter) -> Callable[[], None]:
    original_get = adapter.http.get

    class FakeResponse:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload

        def json(self) -> dict[str, Any]:
            return self._payload

    def fake_get(url: str, **kwargs: object) -> FakeResponse:
        if url == "https://www.preem.se/page-data/stationer/preem-nykoping/page-data.json":
            return FakeResponse(_read_json("preem_station_detail.json"))
        raise AssertionError(f"unexpected preem fixture url: {url}")

    adapter.http.get = fake_get  # type: ignore[assignment]

    def restore() -> None:
        adapter.http.get = original_get  # type: ignore[assignment]

    return restore


def _load_preem_payload() -> tuple[str, dict[str, Any]]:  # type: ignore[no-redef]
    adapter = PreemAdapter()
    restore = _patch_preem_http_get(adapter)
    try:
        record = adapter._fetch_station_detail("/stationer/preem-nykoping")
    finally:
        restore()
    if record is None:
        raise ValueError("preem detail fixture did not produce a station record")
    return "tests/fixtures/preem_station_list.json + tests/fixtures/preem_station_detail.json", {"records": [record]}


FIXTURE_LOADERS: dict[str, Callable[[], tuple[str, dict[str, Any]]]] = {
    "circlek": _load_circlek_payload,
    "espresso_house": _load_espresso_house_payload,
    "ids": _load_ids_payload,
    "preem": _load_preem_payload,
    "rasta": _load_rasta_payload,
    "trafikverket": _load_trafikverket_payload,
    "trb": _load_trb_payload,
}

ADAPTERS = {
    "circlek": CircleKAdapter,
    "espresso_house": EspressoHouseAdapter,
    "ids": IDSAdapter,
    "preem": PreemAdapter,
    "rasta": RastaAdapter,
    "trafikverket": TrafikverketParkingAdapter,
    "trb": TRBAdapter,
}

RAW_RECORD_EXTRACTORS: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
    "circlek": lambda payload: list(payload.get("records", [])),
    "espresso_house": lambda payload: list(payload.get("coffeeShops", [])),
    "ids": lambda payload: list(payload.get("Stations", {}).get("Station", [])),
    "preem": lambda payload: list(payload.get("records", [])),
    "rasta": lambda payload: list(payload.get("records", [])),
    "trafikverket": lambda payload: list(payload.get("records", [])),
    "trb": lambda payload: list(payload.get("records", [])),
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _normalized_presence(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {field_name: 0 for field_name in NORMALIZED_FIELD_NAMES}
    for row in rows:
        for field_name in NORMALIZED_FIELD_NAMES:
            if not _is_missing(row.get(field_name)):
                counts[field_name] += 1
    return counts


def _dropped_raw_fields(raw_keys: list[str], presence: dict[str, int]) -> list[str]:
    dropped: list[str] = []
    for raw_key in raw_keys:
        targets = RAW_TO_NORMALIZED_ALIASES.get(raw_key, [raw_key])
        if any(presence.get(target, 0) > 0 for target in targets):
            continue
        dropped.append(raw_key)
    return sorted(dropped)


def audit_provider(provider_name: str, null_threshold: float = 0.5) -> dict[str, Any]:
    fixture_source, payload = FIXTURE_LOADERS[provider_name]()
    adapter = ADAPTERS[provider_name]()
    raw_records = RAW_RECORD_EXTRACTORS[provider_name](payload)
    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())
    normalized_rows = [
        {
            key: value
            for key, value in asdict(facility).items()
            if key != "raw_payload_ref"
        }
        for facility in facilities
    ]
    presence = _normalized_presence(normalized_rows)
    raw_keys = sorted(
        {
            key
            for record in raw_records
            if isinstance(record, dict)
            for key in record.keys()
        }
    )
    null_heavy_fields = [
        {
            "field": field_name,
            "populated": populated_count,
            "total": len(normalized_rows),
            "completeness": round(populated_count / len(normalized_rows), 3) if normalized_rows else 0.0,
        }
        for field_name, populated_count in sorted(presence.items())
        if normalized_rows and populated_count / len(normalized_rows) <= null_threshold
    ]
    return {
        "provider_name": provider_name,
        "fixture_source": fixture_source,
        "raw_record_count": len(raw_records),
        "normalized_record_count": len(normalized_rows),
        "raw_payload_keys": raw_keys,
        "normalized_fields_present": sorted(field_name for field_name, count in presence.items() if count > 0),
        "dropped_raw_fields": _dropped_raw_fields(raw_keys, presence),
        "null_heavy_fields": null_heavy_fields,
        "issue_count": len(issues),
        "example_issues": [issue.message for issue in issues[:5]],
    }


def audit_all_providers(null_threshold: float = 0.5) -> list[dict[str, Any]]:
    return [audit_provider(provider_name, null_threshold=null_threshold) for provider_name in sorted(FIXTURE_LOADERS)]


def render_provider_audit_markdown(reports: list[dict[str, Any]]) -> str:
    lines = ["# Provider Audit", "", "Fixture-backed provider normalization audit.", ""]
    for report in reports:
        lines.append(f"## {report['provider_name']}")
        lines.append("")
        lines.append(f"- Fixture source: `{report['fixture_source']}`")
        lines.append(f"- Raw records: `{report['raw_record_count']}`")
        lines.append(f"- Normalized records: `{report['normalized_record_count']}`")
        lines.append(f"- Raw payload keys: `{', '.join(report['raw_payload_keys']) or '(none)'}`")
        lines.append(
            f"- Normalized fields present: `{', '.join(report['normalized_fields_present']) or '(none)'}`"
        )
        lines.append(f"- Dropped raw fields: `{', '.join(report['dropped_raw_fields']) or '(none)'}`")
        if report["null_heavy_fields"]:
            summary = ", ".join(
                f"{row['field']} ({row['populated']}/{row['total']})" for row in report["null_heavy_fields"][:10]
            )
        else:
            summary = "(none)"
        lines.append(f"- Null-heavy normalized fields: `{summary}`")
        lines.append(f"- Normalization issues on fixture: `{report['issue_count']}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
