from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta

from app.analysis.models import GapFinding
from app.routing.publisher import Publisher
from app.storage.repository import InMemoryRepository


class AnalysisService:
    def __init__(self, repository: InMemoryRepository, publisher: Publisher):
        self.repository = repository
        self.publisher = publisher

    def run_gap_analysis(
        self, region: str | None = None, category: str | None = None, stale_only: bool = False
    ) -> list[dict[str, object]]:
        facilities = self.repository.list_facilities(category=category)
        findings: list[GapFinding] = []
        counts: dict[tuple[str, str], int] = Counter()
        by_region: dict[str, list[dict[str, object]]] = defaultdict(list)
        stale_cutoff = datetime.utcnow() - timedelta(days=21)

        for facility in facilities:
            facility_region = str(facility.get("region") or facility.get("city") or "unknown")
            if region and facility_region != region:
                continue
            by_region[facility_region].append(facility)
            counts[(facility_region, str(facility.get("category")))] += 1
            freshness = datetime.fromisoformat(str(facility["freshness_ts"]))
            if freshness < stale_cutoff:
                findings.append(
                    GapFinding(
                        finding_type="stale_record",
                        provider_name=str(facility.get("provider_name")),
                        category=str(facility.get("category")),
                        region=facility_region,
                        severity="warning",
                        message="record is older than the freshness threshold",
                        facility_id=int(facility["id"]),
                    )
                )
            missing_fields = [
                key
                for key in ("latitude", "longitude", "formatted_address", "city")
                if not facility.get(key)
            ]
            if missing_fields and not stale_only:
                findings.append(
                    GapFinding(
                        finding_type="missing_fields",
                        provider_name=str(facility.get("provider_name")),
                        category=str(facility.get("category")),
                        region=facility_region,
                        severity="warning",
                        message=f"missing critical fields: {', '.join(missing_fields)}",
                        facility_id=int(facility["id"]),
                    )
                )

        if not stale_only:
            for facility_region, region_facilities in by_region.items():
                category_counts = Counter(str(row.get("category")) for row in region_facilities)
                for category_name, count in category_counts.items():
                    if count < 2:
                        findings.append(
                            GapFinding(
                                finding_type="low_density",
                                provider_name=None,
                                category=category_name,
                                region=facility_region,
                                severity="info",
                                message="facility density is below the minimum threshold",
                            )
                        )
                for required_category in ("fuel_station", "roadside_rest", "parking", "coffee_shop"):
                    if counts[(facility_region, required_category)] == 0:
                        findings.append(
                            GapFinding(
                                finding_type="missing_category",
                                provider_name=None,
                                category=required_category,
                                region=facility_region,
                                severity="warning",
                                message="region is missing a required category",
                            )
                        )

        records = [self.repository.save_gap(finding) for finding in findings]
        for record in records:
            self.publisher.publish_gap(record)
        return records

