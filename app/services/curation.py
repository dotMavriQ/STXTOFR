from __future__ import annotations

import logging
from typing import Any

from app.core.time import utc_now
from app.services.baserow import BaserowClient
from app.services.facility_view import FacilityViewService
from app.storage.repository import Repository

logger = logging.getLogger(__name__)


EDITABLE_FIELDS = (
    "facility_name",
    "category",
    "formatted_address",
    "street",
    "city",
    "region",
    "postal_code",
    "latitude",
    "longitude",
    "phone",
    "opening_hours",
    "services",
    "notes",
    "verified_status",
)


class CurationService:
    def __init__(
        self,
        repository: Repository,
        facility_view_service: FacilityViewService,
        baserow_client: BaserowClient,
    ) -> None:
        self.repository = repository
        self.facility_view_service = facility_view_service
        self.baserow = baserow_client

    def bootstrap_baserow_schema(self) -> dict[str, Any]:
        return self.baserow.ensure_review_schema()

    def push_to_baserow(self, provider: str | None = None) -> dict[str, Any]:
        logger.info("curation push started provider=%s", provider or "all")
        sync = self.repository.create_curation_sync("push", {"provider": provider})
        pushed = 0
        created = 0
        updated = 0
        skipped = 0
        try:
            existing_rows = self.baserow.list_rows()
            existing_by_key = {str(row.get("stxtofr_key")): row for row in existing_rows if row.get("stxtofr_key")}
            effective_rows = self.facility_view_service.list_facilities(view="effective", provider=provider)
            for row in effective_rows:
                if row["change_status"] == "manual":
                    continue
                payload = self._build_source_row_payload(row)
                key = str(payload["stxtofr_key"])
                existing = existing_by_key.get(key)
                if existing is None:
                    created_row = self.baserow.create_row(payload)
                    created += 1
                    baserow_row_id = int(created_row["id"])
                else:
                    self.baserow.update_row(int(existing["id"]), payload)
                    updated += 1
                    baserow_row_id = int(existing["id"])
                self.repository.upsert_facility_curation(
                    int(row["source_facility_id"]),
                    {
                        "baserow_row_id": baserow_row_id,
                        "source": "baserow",
                    },
                )
                pushed += 1
            result = self.repository.finish_curation_sync(
                int(sync["id"]),
                status="completed",
                pushed_count=pushed,
                created_count=created,
                updated_count=updated,
                skipped_count=skipped,
                metadata_json={"provider": provider, "row_count": pushed},
            )
            logger.info("curation push complete: pushed=%d created=%d updated=%d", pushed, created, updated)
            return result
        except Exception as exc:
            logger.error("curation push failed: %s", exc)
            return self.repository.finish_curation_sync(
                int(sync["id"]),
                status="failed",
                pushed_count=pushed,
                created_count=created,
                updated_count=updated,
                skipped_count=skipped,
                error_message=str(exc),
                metadata_json={"provider": provider},
            )

    def pull_from_baserow(self) -> dict[str, Any]:
        logger.info("curation pull started")
        sync = self.repository.create_curation_sync("pull")
        pulled = 0
        created = 0
        updated = 0
        skipped = 0
        try:
            rows = self.baserow.list_rows()
            for row in rows:
                row_origin = str(row.get("row_origin") or "source")
                source_facility_id = self._coerce_int(row.get("source_facility_id"))
                if row_origin == "manual" or source_facility_id is None:
                    payload = self._build_manual_payload(row)
                    if not payload.get("facility_name") or not payload.get("category"):
                        skipped += 1
                        continue
                    existing = next(
                        (
                            record
                            for record in self.repository.list_manual_facilities()
                            if record.get("baserow_row_id") == self._coerce_int(row.get("id"))
                        ),
                        None,
                    )
                    self.repository.upsert_manual_facility(payload)
                    if existing is None:
                        created += 1
                    else:
                        updated += 1
                    pulled += 1
                    continue

                source = self.repository.get_facility(source_facility_id)
                if source is None:
                    skipped += 1
                    continue
                override_values = self._build_override_payload(row, source)
                existing_curation = self.repository.get_facility_curation(source_facility_id)
                self.repository.upsert_facility_curation(source_facility_id, override_values)
                if existing_curation is None:
                    created += 1
                else:
                    updated += 1
                pulled += 1

            result = self.repository.finish_curation_sync(
                int(sync["id"]),
                status="completed",
                pulled_count=pulled,
                created_count=created,
                updated_count=updated,
                skipped_count=skipped,
                metadata_json={"row_count": pulled},
            )
            logger.info("curation pull complete: pulled=%d created=%d updated=%d skipped=%d", pulled, created, updated, skipped)
            return result
        except Exception as exc:
            logger.error("curation pull failed: %s", exc)
            return self.repository.finish_curation_sync(
                int(sync["id"]),
                status="failed",
                pulled_count=pulled,
                created_count=created,
                updated_count=updated,
                skipped_count=skipped,
                error_message=str(exc),
            )

    @staticmethod
    def _build_source_row_payload(row: dict[str, Any]) -> dict[str, Any]:
        services = row.get("services") or []
        payload = {
            "stxtofr_key": row["effective_key"],
            "row_origin": "source",
            "source_facility_id": row["source_facility_id"],
            "provider_name": row["provider_name"],
            "provider_record_id": row["provider_record_id"],
            "facility_brand": row.get("facility_brand"),
            "source_type": row.get("source_type"),
            "source_url": row.get("source_url"),
            **{field: row.get(field) for field in EDITABLE_FIELDS if field != "services"},
            "services": ", ".join(str(item).strip() for item in services if str(item).strip()),
        }
        payload["latitude"] = CurationService._round_coordinate(payload.get("latitude"))
        payload["longitude"] = CurationService._round_coordinate(payload.get("longitude"))
        return payload

    @staticmethod
    def _build_override_payload(row: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "baserow_row_id": CurationService._coerce_int(row.get("id")),
            "source": "baserow",
            "last_pulled_at": utc_now(),
        }
        for field in EDITABLE_FIELDS:
            baserow_value = row.get(field)
            source_value = source.get(field)
            normalized = CurationService._normalize_field_value(field, baserow_value)
            normalized_source = CurationService._normalize_field_value(field, source_value)
            payload[field] = normalized if normalized not in (None, "", []) and normalized != normalized_source else None
        return payload

    @staticmethod
    def _build_manual_payload(row: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "baserow_row_id": CurationService._coerce_int(row.get("id")),
            "facility_name": row.get("facility_name"),
            "facility_brand": row.get("facility_brand"),
            "category": row.get("category"),
            "formatted_address": row.get("formatted_address"),
            "street": row.get("street"),
            "city": row.get("city"),
            "region": row.get("region"),
            "postal_code": row.get("postal_code"),
            "country_code": row.get("country_code") or "se",
            "latitude": CurationService._coerce_float(row.get("latitude")),
            "longitude": CurationService._coerce_float(row.get("longitude")),
            "phone": row.get("phone"),
            "opening_hours": row.get("opening_hours"),
            "services": CurationService._normalize_services(row.get("services")),
            "notes": row.get("notes"),
            "verified_status": row.get("verified_status") or "unverified",
            "source": "baserow",
        }
        return payload

    @staticmethod
    def _normalize_field_value(field: str, value: Any) -> Any:
        if field == "services":
            return CurationService._normalize_services(value)
        if field in {"latitude", "longitude"}:
            return CurationService._round_coordinate(value)
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned or None
        return value

    @staticmethod
    def _normalize_services(value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return [str(value).strip()] if str(value).strip() else None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _round_coordinate(value: Any) -> float | None:
        numeric = CurationService._coerce_float(value)
        return round(numeric, 7) if numeric is not None else None
