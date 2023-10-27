from __future__ import annotations

from dataclasses import dataclass, field

from app.normalization.models import NormalizationIssue


SWEDEN_LATITUDE_RANGE = (55.0, 69.5)
SWEDEN_LONGITUDE_RANGE = (10.5, 24.5)


@dataclass(frozen=True)
class CoordinateNormalizationResult:
    latitude: float | None
    longitude: float | None
    confidence_adjustment: float = 0.0
    notes: list[str] = field(default_factory=list)
    issues: list[NormalizationIssue] = field(default_factory=list)


def _parse_coordinate(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def is_in_sweden(latitude: float | None, longitude: float | None) -> bool:
    if latitude is None or longitude is None:
        return False
    return (
        SWEDEN_LATITUDE_RANGE[0] <= latitude <= SWEDEN_LATITUDE_RANGE[1]
        and SWEDEN_LONGITUDE_RANGE[0] <= longitude <= SWEDEN_LONGITUDE_RANGE[1]
    )


def normalize_coordinates(
    provider_name: str,
    record_id: str,
    latitude: object,
    longitude: object,
) -> CoordinateNormalizationResult:
    parsed_latitude = _parse_coordinate(latitude)
    parsed_longitude = _parse_coordinate(longitude)

    if parsed_latitude is None or parsed_longitude is None:
        return CoordinateNormalizationResult(
            latitude=None,
            longitude=None,
            confidence_adjustment=-0.35,
            notes=["missing or invalid coordinate values"],
            issues=[
                NormalizationIssue(
                    provider_name=provider_name,
                    record_id=record_id,
                    message="missing or invalid coordinate values",
                )
            ],
        )

    if is_in_sweden(parsed_latitude, parsed_longitude):
        return CoordinateNormalizationResult(
            latitude=parsed_latitude,
            longitude=parsed_longitude,
        )

    swapped_latitude = parsed_longitude
    swapped_longitude = parsed_latitude
    if is_in_sweden(swapped_latitude, swapped_longitude):
        return CoordinateNormalizationResult(
            latitude=swapped_latitude,
            longitude=swapped_longitude,
            confidence_adjustment=-0.1,
            notes=["coordinates were swapped during normalization"],
            issues=[
                NormalizationIssue(
                    provider_name=provider_name,
                    record_id=record_id,
                    message="coordinates appeared to be swapped and were corrected",
                    severity="info",
                )
            ],
        )

    return CoordinateNormalizationResult(
        latitude=parsed_latitude,
        longitude=parsed_longitude,
        confidence_adjustment=-0.4,
        notes=["coordinates fell outside Sweden bounds"],
        issues=[
            NormalizationIssue(
                provider_name=provider_name,
                record_id=record_id,
                message="coordinates fell outside Sweden bounds",
            )
        ],
    )

