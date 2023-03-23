from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
import hashlib
import itertools
from typing import Any

from app.analysis.models import GapFinding
from app.core.exceptions import RecordNotFound
from app.normalization.models import FacilitySourceLink, NormalizedFacility
from app.normalization.merge import score_candidate
from app.providers.base import FetchResult


class InMemoryRepository:
    def __init__(self) -> None:
        self._ids = itertools.count(1)
        self.runs: list[dict[str, Any]] = []
        self.fetches: list[dict[str, Any]] = []
        self.raw_payloads: list[dict[str, Any]] = []
        self.facilities: list[dict[str, Any]] = []
        self.facility_links: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.checkpoints: dict[str, str] = {}
        self.merge_candidates: list[dict[str, Any]] = []
        self.merged_facilities: list[dict[str, Any]] = []

    def _next_id(self) -> int:
        return next(self._ids)

    def create_run(self, provider_name: str, mode: str, dry_run: bool) -> dict[str, Any]:
        run = {
            "id": self._next_id(),
            "provider_name": provider_name,
            "mode": mode,
            "status": "running",
            "dry_run": dry_run,
            "started_at": datetime.utcnow(),
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
        run["finished_at"] = datetime.utcnow()
        return run

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return next((run for run in self.runs if run["id"] == run_id), None)

    def list_runs(self, provider: str | None = None, mode: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        runs = self.runs
        if provider:
            runs = [run for run in runs if run["provider_name"] == provider]
        if mode:
            runs = [run for run in runs if run["mode"] == mode]
        if status:
            runs = [run for run in runs if run["status"] == status]
        return [self._serialize_timestamps(run) for run in runs]

    def save_fetch(self, run_id: int, fetch_result: FetchResult) -> dict[str, Any]:
        record = {
            "id": self._next_id(),
            "run_id": run_id,
            "provider_name": fetch_result.provider_name,
            "request_url": fetch_result.request_url,
            "status_code": fetch_result.status_code,
            "fetched_at": fetch_result.fetched_at,
            "response_checksum": hashlib.sha1(repr(fetch_result.payload).encode("utf-8")).hexdigest(),
        }
        self.fetches.append(record)
        return record

    def save_raw_payload(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, Any]:
        checksum = hashlib.sha1(repr(fetch_result.payload).encode("utf-8")).hexdigest()
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
            "replay_key": f"{fetch_result.provider_name}:{checksum}",
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
        record["id"] = self._next_id()
        record["freshness_ts"] = facility.freshness_ts.isoformat()
        record["raw_payload_ref"] = asdict(facility.raw_payload_ref)
        self.facilities.append(record)
        return record

    def list_facilities(
        self,
        provider: str | None = None,
        category: str | None = None,
        city: str | None = None,
        verified: bool | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.facilities
        if provider:
            rows = [row for row in rows if row["provider_name"] == provider]
        if category:
            rows = [row for row in rows if row["category"] == category]
        if city:
            rows = [row for row in rows if row.get("city") == city]
        if verified is not None:
            target = "verified" if verified else "unverified"
            rows = [row for row in rows if row.get("verified_status") == target]
        return rows

    def get_facility(self, facility_id: int) -> dict[str, Any] | None:
        return next((row for row in self.facilities if row["id"] == facility_id), None)

    def save_source_link(self, link: FacilitySourceLink, facility_id: int) -> dict[str, Any]:
        record = {
            "id": self._next_id(),
            "facility_id": facility_id,
            **asdict(link),
        }
        self.facility_links.append(record)
        return record

    def save_gap(self, finding: GapFinding) -> dict[str, Any]:
        record = asdict(finding)
        record["id"] = self._next_id()
        record["created_at"] = finding.created_at.isoformat()
        self.gaps.append(record)
        return record

    def list_gaps(
        self, region: str | None = None, category: str | None = None, stale_only: bool = False
    ) -> list[dict[str, Any]]:
        rows = self.gaps
        if region:
            rows = [row for row in rows if row["region"] == region]
        if category:
            rows = [row for row in rows if row["category"] == category]
        if stale_only:
            rows = [row for row in rows if row["finding_type"] == "stale_record"]
        return rows

    def save_checkpoint(self, provider_name: str, checkpoint: str) -> None:
        self.checkpoints[provider_name] = checkpoint

    def get_checkpoint(self, provider_name: str) -> str | None:
        return self.checkpoints.get(provider_name)

    def get_provider_status(self, provider_name: str) -> dict[str, Any]:
        runs = [run for run in self.runs if run["provider_name"] == provider_name]
        if not runs:
            return {}
        last_run = max(runs, key=lambda row: row["started_at"])
        finished_at = last_run.get("finished_at")
        stale = False
        if finished_at and isinstance(finished_at, datetime):
            stale = finished_at < datetime.utcnow() - timedelta(days=21)
        return {
            "last_run_status": last_run.get("status"),
            "last_run_finished_at": finished_at,
            "stale": stale,
        }

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

    @staticmethod
    def _serialize_timestamps(record: dict[str, Any]) -> dict[str, Any]:
        copy = dict(record)
        for key in ("started_at", "finished_at"):
            if isinstance(copy.get(key), datetime):
                copy[key] = copy[key].isoformat()
        return copy

