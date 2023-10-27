from __future__ import annotations

import logging
from typing import Any

from app.core.config import get_settings
from app.core.time import utc_now
from app.services.facility_view import FacilityViewService
from app.storage.repository import Repository

logger = logging.getLogger(__name__)


class ExportService:
    def __init__(self, repository: Repository, facility_view_service: FacilityViewService) -> None:
        self.repository = repository
        self.facility_view_service = facility_view_service

    def build_facility_bundle(self) -> dict[str, Any]:
        settings = get_settings()
        build = self.repository.create_export_build(settings.export_schema_version)
        logger.info("export build %s started schema=%s", build["id"], settings.export_schema_version)
        try:
            records = [self._to_dto(row) for row in self.facility_view_service.list_facilities(view="effective")]
            latest_runs = self._latest_completed_runs()
            metadata = {
                "schema_version": settings.export_schema_version,
                "built_at": utc_now().isoformat(),
                "record_count": len(records),
                "source_runs": latest_runs,
            }
            bundle = {
                "metadata": metadata,
                "records": records,
            }
            self.repository.finish_export_build(
                int(build["id"]),
                status="completed",
                record_count=len(records),
                bundle_json=bundle,
                metadata_json=metadata,
            )
            logger.info("export build %s complete: records=%d", build["id"], len(records))
            return bundle
        except Exception as exc:
            logger.error("export build %s failed: %s", build["id"], exc)
            self.repository.finish_export_build(
                int(build["id"]),
                status="failed",
                error_message=str(exc),
            )
            raise

    def _latest_completed_runs(self) -> list[dict[str, Any]]:
        latest_by_provider: dict[str, dict[str, Any]] = {}
        for run in self.repository.list_runs(status="completed"):
            provider = str(run["provider_name"])
            if provider not in latest_by_provider:
                latest_by_provider[provider] = run
        return list(latest_by_provider.values())

    @staticmethod
    def _to_dto(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row.get("effective_key") or f"source:{row['id']}",
            "name": row.get("facility_name"),
            "brand": row.get("facility_brand"),
            "provider": row.get("provider_name"),
            "provider_record_id": row.get("provider_record_id"),
            "category": row.get("category"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "address": row.get("formatted_address"),
            "street": row.get("street"),
            "city": row.get("city"),
            "region": row.get("region"),
            "postal_code": row.get("postal_code"),
            "country_code": row.get("country_code"),
            "phone": row.get("phone"),
            "opening_hours": row.get("opening_hours"),
            "services": row.get("services") or [],
            "service_labels": row.get("service_labels") or [],
            "service_groups": row.get("service_facets") or [],
            "service_group_labels": row.get("service_facet_labels") or [],
            "fuel_types": row.get("fuel_types") or [],
            "fuel_type_labels": row.get("fuel_type_labels") or [],
            "verified_status": row.get("verified_status"),
            "notes": row.get("notes"),
            "change_status": row.get("change_status", "unchanged"),
        }
