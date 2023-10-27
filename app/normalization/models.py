from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.core.time import utc_now


@dataclass(frozen=True)
class RawPayloadRef:
    raw_payload_id: int
    provider_name: str


@dataclass(frozen=True)
class FacilitySourceLink:
    provider_name: str
    provider_record_id: str
    facility_hash: str
    raw_payload_id: int


@dataclass(frozen=True)
class NormalizationIssue:
    provider_name: str
    record_id: str | None
    message: str
    severity: str = "warning"


@dataclass(frozen=True)
class NormalizedFacility:
    provider_name: str
    provider_record_id: str
    source_type: str
    source_url: str | None
    raw_payload_ref: RawPayloadRef
    facility_name: str
    facility_brand: str | None
    category: str
    subcategories: list[str] = field(default_factory=list)
    latitude: float | None = None
    longitude: float | None = None
    formatted_address: str | None = None
    street: str | None = None
    city: str | None = None
    region: str | None = None
    postal_code: str | None = None
    country_code: str | None = None
    phone: str | None = None
    opening_hours: str | None = None
    amenities: list[str] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    fuel_types: list[str] = field(default_factory=list)
    parking_features: list[str] = field(default_factory=list)
    heavy_vehicle_relevance: bool = False
    electric_charging_relevance: bool = False
    confidence_score: float = 0.5
    freshness_ts: datetime = field(default_factory=utc_now)
    normalized_hash: str = ""
    verified_status: str = "unverified"
    notes: str | None = None
