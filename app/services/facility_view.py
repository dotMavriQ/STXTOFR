from __future__ import annotations

from typing import Any

from app.normalization.taxonomy import (
    build_category_label,
    build_service_facet_labels,
    build_service_facets,
    build_value_labels,
    service_facet_options,
)
from app.storage.repository import Repository


class FacilityViewService:
    def __init__(self, repository: Repository) -> None:
        self.repository = repository

    def list_facilities(
        self,
        *,
        view: str = "source",
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        need: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        if view == "source":
            rows = self.repository.list_facilities(provider=provider)
            rows = [self._decorate_row(row) for row in rows]
            return self._filter_rows(rows, category=category, city=city, need=need, verified=verified)
        rows = self._build_effective_rows(provider=provider)
        return self._filter_rows(rows, category=category, city=city, need=need, verified=verified)

    def build_map_snapshot(
        self,
        *,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        need: str | None = None,
    ) -> dict[str, Any]:
        source_rows = self.list_facilities(view="source", provider=provider, category=category, city=city, need=need)
        effective_rows = self.list_facilities(view="effective", provider=provider, category=category, city=city, need=need)
        all_effective_rows = self.list_facilities(view="effective", provider=provider)
        return {
            "source": source_rows,
            "effective": effective_rows,
            "summary": {
                "source_count": len(source_rows),
                "effective_count": len(effective_rows),
                "changed_count": len([row for row in effective_rows if row["change_status"] == "overridden"]),
                "manual_count": len([row for row in effective_rows if row["change_status"] == "manual"]),
            },
            "meta": {
                "need_options": self._build_need_options(all_effective_rows),
                "category_options": self._build_category_options(all_effective_rows),
            },
        }

    def _build_effective_rows(self, *, provider: str | None = None) -> list[dict[str, Any]]:
        source_rows = self.repository.list_facilities(provider=provider)
        curations = {row["facility_id"]: row for row in self.repository.list_facility_curations()}
        rows: list[dict[str, Any]] = []
        for source in source_rows:
            curation = curations.get(source["id"])
            rows.append(self._merge_source_with_curation(source, curation))
        for manual in self.repository.list_manual_facilities():
            if provider and provider != "manual":
                continue
            rows.append(self._manual_to_effective(manual))
        return rows

    @staticmethod
    def _filter_rows(
        rows: list[dict[str, Any]],
        *,
        category: str | None = None,
        city: str | None = None,
        need: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        filtered = rows
        if category:
            filtered = [
                row
                for row in filtered
                if row.get("category") == category or category in (row.get("subcategories") or [])
            ]
        if need:
            filtered = [row for row in filtered if need in (row.get("service_facets") or [])]
        if city:
            filtered = [row for row in filtered if row.get("city") == city]
        if verified is not None:
            target = "verified" if verified else "unverified"
            filtered = [row for row in filtered if row.get("verified_status") == target]
        return filtered

    @staticmethod
    def _decorate_row(row: dict[str, Any]) -> dict[str, Any]:
        decorated = dict(row)
        facets = build_service_facets(decorated)
        decorated["service_facets"] = facets
        decorated["service_facet_labels"] = build_service_facet_labels(facets)
        decorated["service_labels"] = build_value_labels(decorated.get("services") or [])
        decorated["fuel_type_labels"] = build_value_labels(decorated.get("fuel_types") or [])
        decorated["parking_feature_labels"] = build_value_labels(decorated.get("parking_features") or [])
        decorated["source_value_labels"] = build_value_labels(
            [
                *(decorated.get("services") or []),
                *(decorated.get("fuel_types") or []),
                *(decorated.get("parking_features") or []),
            ]
        )
        decorated["category_label"] = build_category_label(decorated.get("category"))
        decorated["raw_category_values"] = [
            value
            for value in [decorated.get("category"), *(decorated.get("subcategories") or [])]
            if value
        ]
        return decorated

    @staticmethod
    def _build_need_options(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        counts = {
            option["id"]: len([row for row in rows if option["id"] in (row.get("service_facets") or [])])
            for option in service_facet_options()
        }
        return [
            {**option, "count": counts[option["id"]]}
            for option in service_facet_options()
            if counts[option["id"]]
        ]

    @staticmethod
    def _build_category_options(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        categories: dict[str, dict[str, Any]] = {}
        for row in rows:
            category = row.get("category")
            if not category:
                continue
            if category not in categories:
                categories[str(category)] = {
                    "id": category,
                    "label": row.get("category_label") or build_category_label(category),
                    "count": 0,
                }
            categories[str(category)]["count"] += 1
        return sorted(categories.values(), key=lambda item: str(item["label"]))

    @staticmethod
    def _merge_source_with_curation(source: dict[str, Any], curation: dict[str, Any] | None) -> dict[str, Any]:
        editable_fields = (
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
        row = dict(source)
        row["view"] = "effective"
        row["source_facility_id"] = source["id"]
        row["effective_key"] = f"source:{source['id']}"
        row["baserow_row_id"] = curation.get("baserow_row_id") if curation else None
        row["change_status"] = "unchanged"
        row["override_fields"] = []
        if not curation:
            return FacilityViewService._decorate_row(row)
        for field in editable_fields:
            value = curation.get(field)
            if value is None:
                continue
            if field == "services":
                if list(value) != list(source.get(field) or []):
                    row[field] = list(value)
                    row["override_fields"].append(field)
            elif value != source.get(field):
                row[field] = value
                row["override_fields"].append(field)
        if row["override_fields"]:
            row["change_status"] = "overridden"
        return FacilityViewService._decorate_row(row)

    @staticmethod
    def _manual_to_effective(manual: dict[str, Any]) -> dict[str, Any]:
        return FacilityViewService._decorate_row({
            "id": manual["id"],
            "provider_name": "manual",
            "provider_record_id": f"manual:{manual['id']}",
            "source_type": manual.get("source") or "manual",
            "source_url": None,
            "raw_payload_ref": None,
            "facility_name": manual["facility_name"],
            "facility_brand": manual.get("facility_brand"),
            "category": manual["category"],
            "subcategories": [],
            "latitude": manual.get("latitude"),
            "longitude": manual.get("longitude"),
            "formatted_address": manual.get("formatted_address"),
            "street": manual.get("street"),
            "city": manual.get("city"),
            "region": manual.get("region"),
            "postal_code": manual.get("postal_code"),
            "country_code": manual.get("country_code"),
            "phone": manual.get("phone"),
            "opening_hours": manual.get("opening_hours"),
            "amenities": [],
            "services": manual.get("services") or [],
            "fuel_types": [],
            "parking_features": [],
            "heavy_vehicle_relevance": False,
            "electric_charging_relevance": False,
            "confidence_score": 1.0,
            "freshness_ts": manual.get("updated_at") or manual.get("created_at"),
            "normalized_hash": f"manual:{manual['id']}",
            "verified_status": manual.get("verified_status") or "unverified",
            "notes": manual.get("notes"),
            "view": "effective",
            "source_facility_id": None,
            "effective_key": f"manual:{manual['id']}",
            "baserow_row_id": manual.get("baserow_row_id"),
            "change_status": "manual",
            "override_fields": [],
        })
