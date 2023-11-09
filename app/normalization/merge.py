from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt


@dataclass(frozen=True)
class MergeCandidate:
    left_facility_id: int
    right_facility_id: int
    score: float
    reason: str


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    area = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(area))


def score_candidate(left: dict[str, object], right: dict[str, object]) -> MergeCandidate | None:
    left_name = str(left.get("facility_name", "")).lower()
    right_name = str(right.get("facility_name", "")).lower()
    if not left_name or not right_name:
        return None
    if left_name == right_name:
        name_score = 0.6
        reason = "same_name"
    elif left_name in right_name or right_name in left_name:
        name_score = 0.4
        reason = "overlapping_name"
    else:
        return None
    if None in {
        left.get("latitude"),
        left.get("longitude"),
        right.get("latitude"),
        right.get("longitude"),
    }:
        return MergeCandidate(int(left["id"]), int(right["id"]), name_score, reason)
    distance = haversine_km(
        float(left["latitude"]),
        float(left["longitude"]),
        float(right["latitude"]),
        float(right["longitude"]),
    )
    if distance > 2.0:
        return None
    score = max(0.0, 1.0 - (distance / 2.0)) + name_score
    return MergeCandidate(int(left["id"]), int(right["id"]), min(score, 1.0), f"{reason}_nearby")

