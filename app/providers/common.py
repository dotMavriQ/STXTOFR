from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import hashlib
from typing import Iterable

from app.core.time import parse_utc_datetime
from app.normalization.models import NormalizedFacility, RawPayloadRef


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(part for part in value.replace("\n", " ").split(" ") if part)
    return cleaned or None


def split_address(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    parts = [part.strip() for part in value.split(",", 1)]
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def stable_hash(parts: Iterable[object]) -> str:
    digest = hashlib.sha1("|".join("" if part is None else str(part) for part in parts).encode("utf-8"))
    return digest.hexdigest()


def build_raw_ref(payload_id: int, provider_name: str) -> RawPayloadRef:
    return RawPayloadRef(raw_payload_id=payload_id, provider_name=provider_name)


def facility_to_record(facility: NormalizedFacility) -> dict[str, object]:
    record = asdict(facility)
    record["freshness_ts"] = facility.freshness_ts.isoformat()
    return record


def coerce_datetime(value: str | None, default: datetime) -> datetime:
    if not value:
        return default
    try:
        return parse_utc_datetime(value)
    except ValueError:
        return default
