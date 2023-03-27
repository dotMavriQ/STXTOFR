from __future__ import annotations

from datetime import datetime

from app.normalization.models import NormalizedFacility, RawPayloadRef
from app.providers.common import stable_hash


def map_circlek_station(record: dict[str, object], fetched_at: datetime) -> NormalizedFacility:
    formatted_address = ", ".join(
        part
        for part in [
            str(record.get("street") or ""),
            str(record.get("city") or ""),
            str(record.get("postal_code") or ""),
        ]
        if part
    )
    normalized_hash = stable_hash(
        [
            "circlek",
            record.get("site_id"),
            record.get("latitude"),
            record.get("longitude"),
        ]
    )
    return NormalizedFacility(
        provider_name="circlek",
        provider_record_id=str(record["site_id"]),
        source_type="api",
        source_url=str(record.get("source_url")),
        raw_payload_ref=RawPayloadRef(raw_payload_id=0, provider_name="circlek"),
        facility_name=str(record.get("name")),
        facility_brand="Circle K",
        category="fuel_station",
        subcategories=["truck_stop"],
        latitude=float(record["latitude"]) if record.get("latitude") is not None else None,
        longitude=float(record["longitude"]) if record.get("longitude") is not None else None,
        formatted_address=formatted_address or None,
        street=str(record.get("street") or "") or None,
        city=str(record.get("city") or "") or None,
        region=str(record.get("region") or "") or None,
        postal_code=str(record.get("postal_code") or "") or None,
        country_code=str(record.get("country_code") or "").lower() or None,
        phone=", ".join(record.get("phones", [])),
        opening_hours=str(record.get("opening_hours") or "") or None,
        amenities=[],
        services=list(record.get("services", [])),
        fuel_types=list(record.get("fuels", [])),
        parking_features=[],
        heavy_vehicle_relevance=True,
        electric_charging_relevance="snabbladdning" in record.get("fuels", []),
        confidence_score=0.95,
        freshness_ts=fetched_at,
        normalized_hash=normalized_hash,
        verified_status="unverified",
    )
