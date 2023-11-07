from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
import hashlib
import itertools
import json
from typing import Any, Protocol

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.analysis.models import GapFinding
from app.core.exceptions import RecordNotFound
from app.core.time import ensure_utc, utc_now
from app.normalization.merge import score_candidate
from app.normalization.models import FacilitySourceLink, NormalizationIssue, NormalizedFacility
from app.providers.base import FetchResult
from app.storage.db import SessionLocal
from app.storage.schema import (
    CurationSyncRunRow,
    ExportBuildRow,
    FacilityCurationRow,
    FacilitySourceLinkRow,
    GapFindingRow,
    IngestionRun,
    ManualFacilityRow,
    MergeCandidateRow,
    MergedFacilityRow,
    NormalizationIssueRow,
    NormalizedFacilityRow,
    ProviderCheckpoint,
    ProviderFetch,
    RawPayload,
)


def _stable_digest(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


class Repository(Protocol):
    def create_run(self, provider_name: str, mode: str, dry_run: bool) -> dict[str, Any]: ...
    def finish_run(self, run_id: int, records_fetched: int, records_normalized: int, status: str) -> dict[str, Any]: ...
    def get_run(self, run_id: int) -> dict[str, Any] | None: ...
    def list_runs(self, provider: str | None = None, mode: str | None = None, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]: ...
    def save_fetch(self, run_id: int, fetch_result: FetchResult) -> dict[str, Any]: ...
    def save_raw_payload(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, Any]: ...
    def get_raw_payload(self, payload_id: int) -> dict[str, Any]: ...
    def save_facility(self, facility: NormalizedFacility) -> dict[str, Any]: ...
    def list_facilities(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]: ...
    def get_facility(self, facility_id: int) -> dict[str, Any] | None: ...
    def save_source_link(self, link: FacilitySourceLink, facility_id: int) -> dict[str, Any]: ...
    def save_normalization_issue(
        self,
        issue: NormalizationIssue,
        run_id: int | None = None,
        raw_payload_id: int | None = None,
    ) -> dict[str, Any]: ...
    def list_normalization_issues(
        self,
        provider: str | None = None,
        severity: str | None = None,
        run_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]: ...
    def save_gap(self, finding: GapFinding) -> dict[str, Any]: ...
    def list_gaps(self, region: str | None = None, category: str | None = None, stale_only: bool = False, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]: ...
    def save_checkpoint(self, provider_name: str, checkpoint: str) -> None: ...
    def get_checkpoint(self, provider_name: str) -> str | None: ...
    def get_provider_status(self, provider_name: str) -> dict[str, Any]: ...
    def get_run_detail(self, run_id: int) -> dict[str, Any] | None: ...
    def build_merge_candidates(self) -> list[dict[str, Any]]: ...
    def list_facility_curations(self) -> list[dict[str, Any]]: ...
    def get_facility_curation(self, facility_id: int) -> dict[str, Any] | None: ...
    def upsert_facility_curation(self, facility_id: int, values: dict[str, Any]) -> dict[str, Any]: ...
    def list_facilities_with_curations(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]: ...
    def list_manual_facilities(
        self,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]: ...
    def upsert_manual_facility(self, values: dict[str, Any]) -> dict[str, Any]: ...
    def create_curation_sync(self, direction: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]: ...
    def finish_curation_sync(
        self,
        sync_id: int,
        *,
        status: str,
        pushed_count: int = 0,
        pulled_count: int = 0,
        created_count: int = 0,
        updated_count: int = 0,
        skipped_count: int = 0,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...
    def list_curation_syncs(self) -> list[dict[str, Any]]: ...
    def create_export_build(self, schema_version: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]: ...
    def finish_export_build(
        self,
        build_id: int,
        *,
        status: str,
        record_count: int = 0,
        bundle_json: dict[str, Any] | None = None,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...
    def list_export_builds(self) -> list[dict[str, Any]]: ...
    def serialize_timestamps(self, record: dict[str, Any]) -> dict[str, Any]: ...


class InMemoryRepository:
    def __init__(self) -> None:
        self._ids = itertools.count(1)
        self.runs: list[dict[str, Any]] = []
        self.fetches: list[dict[str, Any]] = []
        self.raw_payloads: list[dict[str, Any]] = []
        self.facilities: list[dict[str, Any]] = []
        self.facility_links: list[dict[str, Any]] = []
        self.normalization_issues: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.checkpoints: dict[str, str] = {}
        self.merge_candidates: list[dict[str, Any]] = []
        self.merged_facilities: list[dict[str, Any]] = []
        self.facility_curations: list[dict[str, Any]] = []
        self.manual_facilities: list[dict[str, Any]] = []
        self.curation_syncs: list[dict[str, Any]] = []
        self.export_builds: list[dict[str, Any]] = []

    def _next_id(self) -> int:
        return next(self._ids)

    def create_run(self, provider_name: str, mode: str, dry_run: bool) -> dict[str, Any]:
        run = {
            "id": self._next_id(),
            "provider_name": provider_name,
            "mode": mode,
            "status": "running",
            "dry_run": dry_run,
            "started_at": utc_now(),
            "finished_at": None,
            "records_fetched": 0,
            "records_normalized": 0,
        }
        self.runs.append(run)
        return run

    def finish_run(self, run_id: int, records_fetched: int, records_normalized: int, status: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if not run:
            raise RecordNotFound(f"run {run_id} not found")
        run["records_fetched"] = records_fetched
        run["records_normalized"] = records_normalized
        run["status"] = status
        run["finished_at"] = utc_now()
        return run

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return next((run for run in self.runs if run["id"] == run_id), None)

    def list_runs(self, provider: str | None = None, mode: str | None = None, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        runs = self.runs
        if provider:
            runs = [run for run in runs if run["provider_name"] == provider]
        if mode:
            runs = [run for run in runs if run["mode"] == mode]
        if status:
            runs = [run for run in runs if run["status"] == status]
        ordered = sorted(runs, key=lambda row: row["started_at"], reverse=True)
        return [self.serialize_timestamps(run) for run in ordered[offset:offset + limit]]

    def save_fetch(self, run_id: int, fetch_result: FetchResult) -> dict[str, Any]:
        record = {
            "id": self._next_id(),
            "run_id": run_id,
            "provider_name": fetch_result.provider_name,
            "request_url": fetch_result.request_url,
            "status_code": fetch_result.status_code,
            "fetched_at": fetch_result.fetched_at,
            "response_checksum": _stable_digest(fetch_result.payload),
        }
        self.fetches.append(record)
        return record

    def save_raw_payload(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, Any]:
        checksum = _stable_digest(fetch_result.payload)
        record = {
            "id": self._next_id(),
            "provider_name": fetch_result.provider_name,
            "fetch_id": fetch_id,
            "request_url": fetch_result.request_url,
            "request_headers": fetch_result.request_headers,
            "status_code": fetch_result.status_code,
            "fetched_at": fetch_result.fetched_at.isoformat(),
            "payload": fetch_result.payload,
            "payload_checksum": checksum,
            "replay_key": f"{fetch_result.provider_name}:{checksum}:{fetch_result.fetched_at.isoformat()}",
        }
        self.raw_payloads.append(record)
        return record

    def get_raw_payload(self, payload_id: int) -> dict[str, Any]:
        payload = next((row for row in self.raw_payloads if row["id"] == payload_id), None)
        if not payload:
            raise RecordNotFound(f"raw payload {payload_id} not found")
        return payload

    def save_facility(self, facility: NormalizedFacility) -> dict[str, Any]:
        record = asdict(facility)
        existing = next(
            (
                row
                for row in self.facilities
                if row["provider_name"] == facility.provider_name and row["provider_record_id"] == facility.provider_record_id
            ),
            None,
        )
        facility_id = existing["id"] if existing else self._next_id()
        saved = {
            "id": facility_id,
            **record,
            "freshness_ts": facility.freshness_ts.isoformat(),
            "raw_payload_ref": asdict(facility.raw_payload_ref),
        }
        if existing:
            existing.clear()
            existing.update(saved)
            return existing
        self.facilities.append(saved)
        return saved

    def list_facilities(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(self.facilities)
        if provider:
            rows = [row for row in rows if row["provider_name"] == provider]
        if category:
            rows = [row for row in rows if row["category"] == category]
        if city:
            rows = [row for row in rows if row.get("city") == city]
        if verified is not None:
            target = "verified" if verified else "unverified"
            rows = [row for row in rows if row.get("verified_status") == target]
        return sorted(rows, key=lambda row: row["id"])

    def get_facility(self, facility_id: int) -> dict[str, Any] | None:
        return next((row for row in self.facilities if row["id"] == facility_id), None)

    def save_source_link(self, link: FacilitySourceLink, facility_id: int) -> dict[str, Any]:
        record = {"id": self._next_id(), "facility_id": facility_id, **asdict(link)}
        self.facility_links.append(record)
        return record

    def save_normalization_issue(
        self,
        issue: NormalizationIssue,
        run_id: int | None = None,
        raw_payload_id: int | None = None,
    ) -> dict[str, Any]:
        record = {
            "id": self._next_id(),
            "run_id": run_id,
            "raw_payload_id": raw_payload_id,
            "provider_name": issue.provider_name,
            "record_id": issue.record_id,
            "message": issue.message,
            "severity": issue.severity,
            "created_at": utc_now().isoformat(),
        }
        self.normalization_issues.append(record)
        return record

    def list_normalization_issues(
        self,
        provider: str | None = None,
        severity: str | None = None,
        run_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = list(self.normalization_issues)
        if provider:
            rows = [row for row in rows if row["provider_name"] == provider]
        if severity:
            rows = [row for row in rows if row["severity"] == severity]
        if run_id is not None:
            rows = [row for row in rows if row["run_id"] == run_id]
        return rows[offset:offset + limit]

    def save_gap(self, finding: GapFinding) -> dict[str, Any]:
        record = asdict(finding)
        record["id"] = self._next_id()
        record["created_at"] = finding.created_at.isoformat()
        self.gaps.append(record)
        return record

    def list_gaps(self, region: str | None = None, category: str | None = None, stale_only: bool = False, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        rows = list(self.gaps)
        if region:
            rows = [row for row in rows if row["region"] == region]
        if category:
            rows = [row for row in rows if row["category"] == category]
        if stale_only:
            rows = [row for row in rows if row["finding_type"] == "stale_record"]
        return rows[offset:offset + limit]

    def save_checkpoint(self, provider_name: str, checkpoint: str) -> None:
        self.checkpoints[provider_name] = checkpoint

    def get_checkpoint(self, provider_name: str) -> str | None:
        return self.checkpoints.get(provider_name)

    def get_provider_status(self, provider_name: str) -> dict[str, Any]:
        runs = [run for run in self.runs if run["provider_name"] == provider_name]
        issue_rows = [row for row in self.normalization_issues if row["provider_name"] == provider_name]
        if not runs:
            return {"last_issue_count": 0, "issue_backlog": len(issue_rows)}
        last_run = max(runs, key=lambda row: row["started_at"])
        finished_at = last_run.get("finished_at")
        stale = isinstance(finished_at, datetime) and ensure_utc(finished_at) < utc_now() - timedelta(days=21)
        last_issue_count = len([row for row in issue_rows if row["run_id"] == last_run["id"]])
        return {
            "last_run_status": last_run.get("status"),
            "last_run_finished_at": finished_at.isoformat() if isinstance(finished_at, datetime) else finished_at,
            "stale": stale,
            "last_issue_count": last_issue_count,
            "issue_backlog": len(issue_rows),
        }

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if not run:
            return None
        detail = self.serialize_timestamps(run)
        detail["issues"] = self.list_normalization_issues(run_id=run_id)
        detail["fetches"] = [self.serialize_timestamps(fetch) for fetch in self.fetches if fetch["run_id"] == run_id]
        return detail

    def build_merge_candidates(self) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for left_index, left in enumerate(self.facilities):
            for right in self.facilities[left_index + 1 :]:
                candidate = score_candidate(left, right)
                if not candidate:
                    continue
                record = {
                    "id": self._next_id(),
                    "left_facility_id": candidate.left_facility_id,
                    "right_facility_id": candidate.right_facility_id,
                    "score": candidate.score,
                    "reason": candidate.reason,
                }
                candidates.append(record)
        self.merge_candidates = candidates
        return candidates

    def list_facilities_with_curations(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
        facilities = self.list_facilities(provider=provider, category=category, city=city, verified=verified)
        curation_map = {row["facility_id"]: self.serialize_timestamps(row) for row in self.facility_curations}
        return [(facility, curation_map.get(facility["id"])) for facility in facilities]

    def list_facility_curations(self) -> list[dict[str, Any]]:
        return [self.serialize_timestamps(row) for row in sorted(self.facility_curations, key=lambda row: row["facility_id"])]

    def get_facility_curation(self, facility_id: int) -> dict[str, Any] | None:
        row = next((row for row in self.facility_curations if row["facility_id"] == facility_id), None)
        return self.serialize_timestamps(row) if row else None

    def upsert_facility_curation(self, facility_id: int, values: dict[str, Any]) -> dict[str, Any]:
        existing = next((row for row in self.facility_curations if row["facility_id"] == facility_id), None)
        now = utc_now()
        if existing is None:
            existing = {
                "id": self._next_id(),
                "facility_id": facility_id,
                "created_at": now,
                "updated_at": now,
                "source": "baserow",
            }
            self.facility_curations.append(existing)
        existing.update({key: value for key, value in values.items() if value is not ...})
        existing["updated_at"] = now
        return self.serialize_timestamps(existing)

    def list_manual_facilities(
        self,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = sorted(self.manual_facilities, key=lambda row: row["id"])
        if category:
            rows = [row for row in rows if row.get("category") == category]
        if city:
            rows = [row for row in rows if row.get("city") == city]
        if verified is not None:
            target = "verified" if verified else "unverified"
            rows = [row for row in rows if row.get("verified_status") == target]
        return [self.serialize_timestamps(row) for row in rows]

    def upsert_manual_facility(self, values: dict[str, Any]) -> dict[str, Any]:
        existing = None
        baserow_row_id = values.get("baserow_row_id")
        if baserow_row_id is not None:
            existing = next((row for row in self.manual_facilities if row.get("baserow_row_id") == baserow_row_id), None)
        if existing is None and values.get("id") is not None:
            existing = next((row for row in self.manual_facilities if row["id"] == values["id"]), None)
        now = utc_now()
        if existing is None:
            existing = {
                "id": self._next_id(),
                "created_at": now,
                "updated_at": now,
                "source": "baserow",
                "country_code": "se",
                "services": [],
                "verified_status": "unverified",
            }
            self.manual_facilities.append(existing)
        existing.update(values)
        existing["updated_at"] = now
        return self.serialize_timestamps(existing)

    def create_curation_sync(self, direction: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]:
        row = {
            "id": self._next_id(),
            "direction": direction,
            "status": "running",
            "pushed_count": 0,
            "pulled_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
            "error_message": None,
            "metadata_json": metadata_json or {},
            "started_at": utc_now(),
            "finished_at": None,
        }
        self.curation_syncs.append(row)
        return self.serialize_timestamps(row)

    def finish_curation_sync(
        self,
        sync_id: int,
        *,
        status: str,
        pushed_count: int = 0,
        pulled_count: int = 0,
        created_count: int = 0,
        updated_count: int = 0,
        skipped_count: int = 0,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = next((item for item in self.curation_syncs if item["id"] == sync_id), None)
        if row is None:
            raise RecordNotFound(f"curation sync {sync_id} not found")
        row.update(
            {
                "status": status,
                "pushed_count": pushed_count,
                "pulled_count": pulled_count,
                "created_count": created_count,
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "error_message": error_message,
                "metadata_json": metadata_json or row.get("metadata_json") or {},
                "finished_at": utc_now(),
            }
        )
        return self.serialize_timestamps(row)

    def list_curation_syncs(self) -> list[dict[str, Any]]:
        return [self.serialize_timestamps(row) for row in sorted(self.curation_syncs, key=lambda item: item["id"], reverse=True)]

    def create_export_build(self, schema_version: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]:
        row = {
            "id": self._next_id(),
            "schema_version": schema_version,
            "status": "running",
            "record_count": 0,
            "metadata_json": metadata_json or {},
            "bundle_json": None,
            "error_message": None,
            "started_at": utc_now(),
            "finished_at": None,
        }
        self.export_builds.append(row)
        return self.serialize_timestamps(row)

    def finish_export_build(
        self,
        build_id: int,
        *,
        status: str,
        record_count: int = 0,
        bundle_json: dict[str, Any] | None = None,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = next((item for item in self.export_builds if item["id"] == build_id), None)
        if row is None:
            raise RecordNotFound(f"export build {build_id} not found")
        row.update(
            {
                "status": status,
                "record_count": record_count,
                "bundle_json": bundle_json,
                "error_message": error_message,
                "metadata_json": metadata_json or row.get("metadata_json") or {},
                "finished_at": utc_now(),
            }
        )
        return self.serialize_timestamps(row)

    def list_export_builds(self) -> list[dict[str, Any]]:
        return [self.serialize_timestamps(row) for row in sorted(self.export_builds, key=lambda item: item["id"], reverse=True)]

    @staticmethod
    def serialize_timestamps(record: dict[str, Any]) -> dict[str, Any]:
        copy = dict(record)
        for key, value in list(copy.items()):
            if isinstance(value, datetime):
                copy[key] = value.isoformat()
        return copy


class SQLRepository:
    CURATION_FIELDS = (
        "baserow_row_id",
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
        "changed_by",
        "source",
        "last_pulled_at",
    )
    MANUAL_FIELDS = (
        "baserow_row_id",
        "facility_name",
        "facility_brand",
        "category",
        "formatted_address",
        "street",
        "city",
        "region",
        "postal_code",
        "country_code",
        "latitude",
        "longitude",
        "phone",
        "opening_hours",
        "services",
        "notes",
        "verified_status",
        "source",
        "changed_by",
    )

    def __init__(self, session_factory=SessionLocal) -> None:
        self.session_factory = session_factory

    def _session(self) -> Session:
        return self.session_factory()

    def create_run(self, provider_name: str, mode: str, dry_run: bool) -> dict[str, Any]:
        with self._session() as session:
            row = IngestionRun(
                provider_name=provider_name,
                mode=mode,
                status="running",
                dry_run=dry_run,
                started_at=utc_now(),
                finished_at=None,
                records_fetched=0,
                records_normalized=0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._run_to_dict(row)

    def finish_run(self, run_id: int, records_fetched: int, records_normalized: int, status: str) -> dict[str, Any]:
        with self._session() as session:
            row = session.get(IngestionRun, run_id)
            if row is None:
                raise RecordNotFound(f"run {run_id} not found")
            row.records_fetched = records_fetched
            row.records_normalized = records_normalized
            row.status = status
            row.finished_at = utc_now()
            session.commit()
            session.refresh(row)
            return self._run_to_dict(row)

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._session() as session:
            row = session.get(IngestionRun, run_id)
            return self._run_to_dict(row) if row is not None else None

    def list_runs(self, provider: str | None = None, mode: str | None = None, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._session() as session:
            query = session.query(IngestionRun)
            if provider:
                query = query.filter(IngestionRun.provider_name == provider)
            if mode:
                query = query.filter(IngestionRun.mode == mode)
            if status:
                query = query.filter(IngestionRun.status == status)
            rows = query.order_by(IngestionRun.started_at.desc()).limit(limit).offset(offset).all()
            return [self.serialize_timestamps(self._run_to_dict(row)) for row in rows]

    def save_fetch(self, run_id: int, fetch_result: FetchResult) -> dict[str, Any]:
        checksum = _stable_digest(fetch_result.payload)
        with self._session() as session:
            row = ProviderFetch(
                run_id=run_id,
                provider_name=fetch_result.provider_name,
                request_url=fetch_result.request_url,
                status_code=fetch_result.status_code,
                fetched_at=fetch_result.fetched_at,
                response_checksum=checksum,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._fetch_to_dict(row)

    def save_raw_payload(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, Any]:
        checksum = _stable_digest(fetch_result.payload)
        replay_key = f"{fetch_result.provider_name}:{checksum}:{fetch_result.fetched_at.isoformat()}"
        with self._session() as session:
            row = RawPayload(
                provider_name=fetch_result.provider_name,
                fetch_id=fetch_id,
                request_url=fetch_result.request_url,
                request_headers=fetch_result.request_headers,
                status_code=fetch_result.status_code,
                fetched_at=fetch_result.fetched_at,
                payload=fetch_result.payload,
                payload_checksum=checksum,
                replay_key=replay_key,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._raw_payload_to_dict(row)

    def get_raw_payload(self, payload_id: int) -> dict[str, Any]:
        with self._session() as session:
            row = session.get(RawPayload, payload_id)
            if row is None:
                raise RecordNotFound(f"raw payload {payload_id} not found")
            return self._raw_payload_to_dict(row)

    def save_facility(self, facility: NormalizedFacility) -> dict[str, Any]:
        _fields = dict(
            source_type=facility.source_type,
            source_url=facility.source_url,
            raw_payload_id=facility.raw_payload_ref.raw_payload_id,
            facility_name=facility.facility_name,
            facility_brand=facility.facility_brand,
            category=facility.category,
            subcategories=facility.subcategories,
            latitude=facility.latitude,
            longitude=facility.longitude,
            formatted_address=facility.formatted_address,
            street=facility.street,
            city=facility.city,
            region=facility.region,
            postal_code=facility.postal_code,
            country_code=facility.country_code,
            phone=facility.phone,
            opening_hours=facility.opening_hours,
            amenities=facility.amenities,
            services=facility.services,
            fuel_types=facility.fuel_types,
            parking_features=facility.parking_features,
            heavy_vehicle_relevance=facility.heavy_vehicle_relevance,
            electric_charging_relevance=facility.electric_charging_relevance,
            confidence_score=facility.confidence_score,
            freshness_ts=facility.freshness_ts,
            normalized_hash=facility.normalized_hash,
            verified_status=facility.verified_status,
            notes=facility.notes,
        )
        with self._session() as session:
            row = NormalizedFacilityRow(
                provider_name=facility.provider_name,
                provider_record_id=facility.provider_record_id,
                **_fields,
            )
            try:
                session.add(row)
                session.flush()
            except IntegrityError:
                session.rollback()
                row = (
                    session.query(NormalizedFacilityRow)
                    .filter(
                        NormalizedFacilityRow.provider_name == facility.provider_name,
                        NormalizedFacilityRow.provider_record_id == facility.provider_record_id,
                    )
                    .one()
                )
                for attr, val in _fields.items():
                    setattr(row, attr, val)
            session.commit()
            session.refresh(row)
            return self._facility_to_dict(row)

    def list_facilities(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        with self._session() as session:
            query = session.query(NormalizedFacilityRow)
            if provider:
                query = query.filter(NormalizedFacilityRow.provider_name == provider)
            if category:
                query = query.filter(NormalizedFacilityRow.category == category)
            if city:
                query = query.filter(NormalizedFacilityRow.city == city)
            if verified is not None:
                target = "verified" if verified else "unverified"
                query = query.filter(NormalizedFacilityRow.verified_status == target)
            rows = query.order_by(NormalizedFacilityRow.id.asc()).all()
            return [self._facility_to_dict(row) for row in rows]

    def get_facility(self, facility_id: int) -> dict[str, Any] | None:
        with self._session() as session:
            row = session.get(NormalizedFacilityRow, facility_id)
            return self._facility_to_dict(row) if row is not None else None

    def save_source_link(self, link: FacilitySourceLink, facility_id: int) -> dict[str, Any]:
        with self._session() as session:
            row = FacilitySourceLinkRow(
                facility_id=facility_id,
                provider_name=link.provider_name,
                provider_record_id=link.provider_record_id,
                facility_hash=link.facility_hash,
                raw_payload_id=link.raw_payload_id,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._source_link_to_dict(row)

    def save_normalization_issue(
        self,
        issue: NormalizationIssue,
        run_id: int | None = None,
        raw_payload_id: int | None = None,
    ) -> dict[str, Any]:
        with self._session() as session:
            row = NormalizationIssueRow(
                run_id=run_id,
                raw_payload_id=raw_payload_id,
                provider_name=issue.provider_name,
                record_id=issue.record_id,
                message=issue.message,
                severity=issue.severity,
                created_at=utc_now(),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._issue_to_dict(row)

    def list_normalization_issues(
        self,
        provider: str | None = None,
        severity: str | None = None,
        run_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        with self._session() as session:
            query = session.query(NormalizationIssueRow)
            if provider:
                query = query.filter(NormalizationIssueRow.provider_name == provider)
            if severity:
                query = query.filter(NormalizationIssueRow.severity == severity)
            if run_id is not None:
                query = query.filter(NormalizationIssueRow.run_id == run_id)
            rows = query.order_by(NormalizationIssueRow.created_at.desc()).limit(limit).offset(offset).all()
            return [self._issue_to_dict(row) for row in rows]

    def save_gap(self, finding: GapFinding) -> dict[str, Any]:
        with self._session() as session:
            row = GapFindingRow(
                finding_type=finding.finding_type,
                provider_name=finding.provider_name,
                category=finding.category,
                region=finding.region,
                severity=finding.severity,
                message=finding.message,
                facility_id=finding.facility_id,
                created_at=finding.created_at,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._gap_to_dict(row)

    def list_gaps(self, region: str | None = None, category: str | None = None, stale_only: bool = False, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._session() as session:
            query = session.query(GapFindingRow)
            if region:
                query = query.filter(GapFindingRow.region == region)
            if category:
                query = query.filter(GapFindingRow.category == category)
            if stale_only:
                query = query.filter(GapFindingRow.finding_type == "stale_record")
            rows = query.order_by(GapFindingRow.created_at.desc()).limit(limit).offset(offset).all()
            return [self._gap_to_dict(row) for row in rows]

    def save_checkpoint(self, provider_name: str, checkpoint: str) -> None:
        with self._session() as session:
            now = utc_now()
            row = ProviderCheckpoint(provider_name=provider_name, checkpoint=checkpoint, updated_at=now)
            try:
                session.add(row)
                session.flush()
            except IntegrityError:
                session.rollback()
                row = session.query(ProviderCheckpoint).filter(ProviderCheckpoint.provider_name == provider_name).one()
                row.checkpoint = checkpoint
                row.updated_at = utc_now()
            session.commit()

    def get_checkpoint(self, provider_name: str) -> str | None:
        with self._session() as session:
            row = session.query(ProviderCheckpoint).filter(ProviderCheckpoint.provider_name == provider_name).one_or_none()
            return None if row is None else row.checkpoint

    def get_provider_status(self, provider_name: str) -> dict[str, Any]:
        with self._session() as session:
            runs = (
                session.query(IngestionRun)
                .filter(IngestionRun.provider_name == provider_name)
                .order_by(IngestionRun.started_at.desc())
                .all()
            )
            issue_count = session.query(NormalizationIssueRow).filter(NormalizationIssueRow.provider_name == provider_name).count()
            if not runs:
                return {"last_issue_count": 0, "issue_backlog": issue_count}
            last_run = runs[0]
            finished_at = last_run.finished_at
            stale = finished_at is not None and ensure_utc(finished_at) < utc_now() - timedelta(days=21)
            last_issue_count = (
                session.query(NormalizationIssueRow)
                .filter(NormalizationIssueRow.provider_name == provider_name, NormalizationIssueRow.run_id == last_run.id)
                .count()
            )
            return {
                "last_run_status": last_run.status,
                "last_run_finished_at": finished_at.isoformat() if finished_at else None,
                "stale": stale,
                "last_issue_count": last_issue_count,
                "issue_backlog": issue_count,
            }

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None:
        with self._session() as session:
            run = session.get(IngestionRun, run_id)
            if run is None:
                return None
            detail = self.serialize_timestamps(self._run_to_dict(run))
            detail["issues"] = [
                self._issue_to_dict(row)
                for row in session.query(NormalizationIssueRow).filter(NormalizationIssueRow.run_id == run_id).all()
            ]
            detail["fetches"] = [
                self.serialize_timestamps(self._fetch_to_dict(row))
                for row in session.query(ProviderFetch).filter(ProviderFetch.run_id == run_id).all()
            ]
            return detail

    def build_merge_candidates(self) -> list[dict[str, Any]]:
        with self._session() as session:
            for row in session.query(MergeCandidateRow).all():
                session.delete(row)
            session.commit()
            facilities = [
                self._facility_to_dict(row)
                for row in session.query(NormalizedFacilityRow).order_by(NormalizedFacilityRow.id.asc()).all()
            ]
            candidates: list[dict[str, Any]] = []
            for left_index, left in enumerate(facilities):
                for right in facilities[left_index + 1 :]:
                    candidate = score_candidate(left, right)
                    if not candidate:
                        continue
                    row = MergeCandidateRow(
                        left_facility_id=candidate.left_facility_id,
                        right_facility_id=candidate.right_facility_id,
                        score=candidate.score,
                        reason=candidate.reason,
                    )
                    session.add(row)
                    session.flush()
                    candidates.append(self._merge_candidate_to_dict(row))
            session.commit()
            return candidates

    def list_facilities_with_curations(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
        with self._session() as session:
            query = (
                session.query(NormalizedFacilityRow, FacilityCurationRow)
                .outerjoin(FacilityCurationRow, NormalizedFacilityRow.id == FacilityCurationRow.facility_id)
            )
            if provider:
                query = query.filter(NormalizedFacilityRow.provider_name == provider)
            if category:
                query = query.filter(NormalizedFacilityRow.category == category)
            if city:
                query = query.filter(NormalizedFacilityRow.city == city)
            if verified is not None:
                target = "verified" if verified else "unverified"
                query = query.filter(NormalizedFacilityRow.verified_status == target)
            rows = query.order_by(NormalizedFacilityRow.id.asc()).all()
            return [
                (self._facility_to_dict(facility), self._curation_to_dict(curation) if curation else None)
                for facility, curation in rows
            ]

    def list_facility_curations(self) -> list[dict[str, Any]]:
        with self._session() as session:
            rows = session.query(FacilityCurationRow).order_by(FacilityCurationRow.facility_id.asc()).all()
            return [self._curation_to_dict(row) for row in rows]

    def get_facility_curation(self, facility_id: int) -> dict[str, Any] | None:
        with self._session() as session:
            row = session.query(FacilityCurationRow).filter(FacilityCurationRow.facility_id == facility_id).one_or_none()
            return self._curation_to_dict(row) if row is not None else None

    def upsert_facility_curation(self, facility_id: int, values: dict[str, Any]) -> dict[str, Any]:
        now = utc_now()
        with self._session() as session:
            row = FacilityCurationRow(facility_id=facility_id, created_at=now, updated_at=now)
            for field in self.CURATION_FIELDS:
                if field in values:
                    setattr(row, field, values[field])
            try:
                session.add(row)
                session.flush()
            except IntegrityError:
                session.rollback()
                row = session.query(FacilityCurationRow).filter(FacilityCurationRow.facility_id == facility_id).one()
                for field in self.CURATION_FIELDS:
                    if field in values:
                        setattr(row, field, values[field])
                row.updated_at = now
            session.commit()
            session.refresh(row)
            return self._curation_to_dict(row)

    def list_manual_facilities(
        self,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        with self._session() as session:
            query = session.query(ManualFacilityRow).order_by(ManualFacilityRow.id.asc())
            if category:
                query = query.filter(ManualFacilityRow.category == category)
            if city:
                query = query.filter(ManualFacilityRow.city == city)
            if verified is not None:
                target = "verified" if verified else "unverified"
                query = query.filter(ManualFacilityRow.verified_status == target)
            rows = query.all()
            return [self._manual_facility_to_dict(row) for row in rows]

    def upsert_manual_facility(self, values: dict[str, Any]) -> dict[str, Any]:
        now = utc_now()
        baserow_row_id = values.get("baserow_row_id")
        with self._session() as session:
            if baserow_row_id is not None:
                row = ManualFacilityRow(
                    baserow_row_id=baserow_row_id,
                    facility_name=str(values.get("facility_name") or ""),
                    category=str(values.get("category") or "manual"),
                    country_code=str(values.get("country_code") or "se"),
                    services=list(values.get("services") or []),
                    verified_status=str(values.get("verified_status") or "unverified"),
                    created_at=now,
                    updated_at=now,
                )
                for field in self.MANUAL_FIELDS:
                    if field in values:
                        setattr(row, field, values[field])
                try:
                    session.add(row)
                    session.flush()
                except IntegrityError:
                    session.rollback()
                    row = session.query(ManualFacilityRow).filter(ManualFacilityRow.baserow_row_id == baserow_row_id).one()
                    for field in self.MANUAL_FIELDS:
                        if field in values:
                            setattr(row, field, values[field])
                    row.updated_at = now
            else:
                row = session.get(ManualFacilityRow, values["id"]) if values.get("id") is not None else None
                if row is None:
                    row = ManualFacilityRow(
                        facility_name=str(values.get("facility_name") or ""),
                        category=str(values.get("category") or "manual"),
                        country_code=str(values.get("country_code") or "se"),
                        services=list(values.get("services") or []),
                        verified_status=str(values.get("verified_status") or "unverified"),
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(row)
                for field in self.MANUAL_FIELDS:
                    if field in values:
                        setattr(row, field, values[field])
                row.updated_at = now
            session.commit()
            session.refresh(row)
            return self._manual_facility_to_dict(row)

    def create_curation_sync(self, direction: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._session() as session:
            row = CurationSyncRunRow(
                direction=direction,
                status="running",
                metadata_json=metadata_json or {},
                started_at=utc_now(),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._curation_sync_to_dict(row)

    def finish_curation_sync(
        self,
        sync_id: int,
        *,
        status: str,
        pushed_count: int = 0,
        pulled_count: int = 0,
        created_count: int = 0,
        updated_count: int = 0,
        skipped_count: int = 0,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._session() as session:
            row = session.get(CurationSyncRunRow, sync_id)
            if row is None:
                raise RecordNotFound(f"curation sync {sync_id} not found")
            row.status = status
            row.pushed_count = pushed_count
            row.pulled_count = pulled_count
            row.created_count = created_count
            row.updated_count = updated_count
            row.skipped_count = skipped_count
            row.error_message = error_message
            if metadata_json is not None:
                row.metadata_json = metadata_json
            row.finished_at = utc_now()
            session.commit()
            session.refresh(row)
            return self._curation_sync_to_dict(row)

    def list_curation_syncs(self) -> list[dict[str, Any]]:
        with self._session() as session:
            rows = session.query(CurationSyncRunRow).order_by(CurationSyncRunRow.started_at.desc()).all()
            return [self._curation_sync_to_dict(row) for row in rows]

    def create_export_build(self, schema_version: str, metadata_json: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._session() as session:
            row = ExportBuildRow(
                schema_version=schema_version,
                status="running",
                record_count=0,
                metadata_json=metadata_json or {},
                started_at=utc_now(),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._export_build_to_dict(row)

    def finish_export_build(
        self,
        build_id: int,
        *,
        status: str,
        record_count: int = 0,
        bundle_json: dict[str, Any] | None = None,
        error_message: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._session() as session:
            row = session.get(ExportBuildRow, build_id)
            if row is None:
                raise RecordNotFound(f"export build {build_id} not found")
            row.status = status
            row.record_count = record_count
            row.bundle_json = bundle_json
            row.error_message = error_message
            if metadata_json is not None:
                row.metadata_json = metadata_json
            row.finished_at = utc_now()
            session.commit()
            session.refresh(row)
            return self._export_build_to_dict(row)

    def list_export_builds(self) -> list[dict[str, Any]]:
        with self._session() as session:
            rows = session.query(ExportBuildRow).order_by(ExportBuildRow.started_at.desc()).all()
            return [self._export_build_to_dict(row) for row in rows]

    @staticmethod
    def serialize_timestamps(record: dict[str, Any]) -> dict[str, Any]:
        copy = dict(record)
        for key, value in list(copy.items()):
            if isinstance(value, datetime):
                copy[key] = value.isoformat()
        return copy

    @staticmethod
    def _run_to_dict(row: IngestionRun) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider_name": row.provider_name,
            "mode": row.mode,
            "status": row.status,
            "dry_run": row.dry_run,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
            "records_fetched": row.records_fetched,
            "records_normalized": row.records_normalized,
        }

    @staticmethod
    def _fetch_to_dict(row: ProviderFetch) -> dict[str, Any]:
        return {
            "id": row.id,
            "run_id": row.run_id,
            "provider_name": row.provider_name,
            "request_url": row.request_url,
            "status_code": row.status_code,
            "fetched_at": row.fetched_at,
            "response_checksum": row.response_checksum,
        }

    @staticmethod
    def _raw_payload_to_dict(row: RawPayload) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider_name": row.provider_name,
            "fetch_id": row.fetch_id,
            "request_url": row.request_url,
            "request_headers": row.request_headers,
            "status_code": row.status_code,
            "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
            "payload": row.payload,
            "payload_checksum": row.payload_checksum,
            "replay_key": row.replay_key,
        }

    @staticmethod
    def _facility_to_dict(row: NormalizedFacilityRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider_name": row.provider_name,
            "provider_record_id": row.provider_record_id,
            "source_type": row.source_type,
            "source_url": row.source_url,
            "raw_payload_ref": {"raw_payload_id": row.raw_payload_id, "provider_name": row.provider_name},
            "facility_name": row.facility_name,
            "facility_brand": row.facility_brand,
            "category": row.category,
            "subcategories": row.subcategories or [],
            "latitude": row.latitude,
            "longitude": row.longitude,
            "formatted_address": row.formatted_address,
            "street": row.street,
            "city": row.city,
            "region": row.region,
            "postal_code": row.postal_code,
            "country_code": row.country_code,
            "phone": row.phone,
            "opening_hours": row.opening_hours,
            "amenities": row.amenities or [],
            "services": row.services or [],
            "fuel_types": row.fuel_types or [],
            "parking_features": row.parking_features or [],
            "heavy_vehicle_relevance": row.heavy_vehicle_relevance,
            "electric_charging_relevance": row.electric_charging_relevance,
            "confidence_score": row.confidence_score,
            "freshness_ts": row.freshness_ts.isoformat() if row.freshness_ts else None,
            "normalized_hash": row.normalized_hash,
            "verified_status": row.verified_status,
            "notes": row.notes,
        }

    @staticmethod
    def _source_link_to_dict(row: FacilitySourceLinkRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "facility_id": row.facility_id,
            "provider_name": row.provider_name,
            "provider_record_id": row.provider_record_id,
            "facility_hash": row.facility_hash,
            "raw_payload_id": row.raw_payload_id,
        }

    @staticmethod
    def _issue_to_dict(row: NormalizationIssueRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "run_id": row.run_id,
            "raw_payload_id": row.raw_payload_id,
            "provider_name": row.provider_name,
            "record_id": row.record_id,
            "message": row.message,
            "severity": row.severity,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _gap_to_dict(row: GapFindingRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "finding_type": row.finding_type,
            "provider_name": row.provider_name,
            "category": row.category,
            "region": row.region,
            "severity": row.severity,
            "message": row.message,
            "facility_id": row.facility_id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _merge_candidate_to_dict(row: MergeCandidateRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "left_facility_id": row.left_facility_id,
            "right_facility_id": row.right_facility_id,
            "score": row.score,
            "reason": row.reason,
        }

    @staticmethod
    def _curation_to_dict(row: FacilityCurationRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "facility_id": row.facility_id,
            "baserow_row_id": row.baserow_row_id,
            "facility_name": row.facility_name,
            "category": row.category,
            "formatted_address": row.formatted_address,
            "street": row.street,
            "city": row.city,
            "region": row.region,
            "postal_code": row.postal_code,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "phone": row.phone,
            "opening_hours": row.opening_hours,
            "services": row.services,
            "notes": row.notes,
            "verified_status": row.verified_status,
            "changed_by": row.changed_by,
            "source": row.source,
            "last_pulled_at": row.last_pulled_at.isoformat() if row.last_pulled_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _manual_facility_to_dict(row: ManualFacilityRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "baserow_row_id": row.baserow_row_id,
            "facility_name": row.facility_name,
            "facility_brand": row.facility_brand,
            "category": row.category,
            "formatted_address": row.formatted_address,
            "street": row.street,
            "city": row.city,
            "region": row.region,
            "postal_code": row.postal_code,
            "country_code": row.country_code,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "phone": row.phone,
            "opening_hours": row.opening_hours,
            "services": row.services or [],
            "notes": row.notes,
            "verified_status": row.verified_status,
            "source": row.source,
            "changed_by": row.changed_by,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _curation_sync_to_dict(row: CurationSyncRunRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "direction": row.direction,
            "status": row.status,
            "pushed_count": row.pushed_count,
            "pulled_count": row.pulled_count,
            "created_count": row.created_count,
            "updated_count": row.updated_count,
            "skipped_count": row.skipped_count,
            "error_message": row.error_message,
            "metadata_json": row.metadata_json or {},
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }

    @staticmethod
    def _export_build_to_dict(row: ExportBuildRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "schema_version": row.schema_version,
            "status": row.status,
            "record_count": row.record_count,
            "metadata_json": row.metadata_json or {},
            "bundle_json": row.bundle_json,
            "error_message": row.error_message,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }
