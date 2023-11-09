from __future__ import annotations

import logging
from dataclasses import replace

from app.core.exceptions import ActiveRunError
from app.core.time import parse_utc_datetime
from app.normalization.models import FacilitySourceLink
from app.providers.base import RunContext
from app.routing.publisher import build_publisher
from app.services.provider_registry import ProviderRegistry
from app.storage.raw_archive import RawArchive
from app.storage.repository import Repository

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(
        self,
        repository: Repository,
        registry: ProviderRegistry,
        archive: RawArchive,
    ):
        self.repository = repository
        self.registry = registry
        self.archive = archive
        self.publisher = build_publisher()

    def run_provider(self, provider_name: str, mode: str = "full", dry_run: bool = False) -> dict[str, object]:
        active = self.repository.list_runs(provider=provider_name, status="running")
        if active:
            raise ActiveRunError(f"{provider_name} already has an active run (id {active[0]['id']})")
        adapter = self.registry.get(provider_name)
        checkpoint = self.repository.get_checkpoint(provider_name) if mode == "incremental" else None
        run = self.repository.create_run(provider_name=provider_name, mode=mode, dry_run=dry_run)
        run_context = RunContext(mode=mode, checkpoint=checkpoint, dry_run=dry_run)
        logger.info("run %s started: provider=%s mode=%s dry_run=%s", run["id"], provider_name, mode, dry_run)

        try:
            fetch_result = adapter.fetch(run_context)
        except Exception as exc:
            logger.error("run %s fetch failed: provider=%s error=%s", run["id"], provider_name, exc)
            finished_run = self.repository.finish_run(
                run["id"],
                records_fetched=0,
                records_normalized=0,
                status="failed",
            )
            return {
                "run": self.repository.serialize_timestamps(finished_run),
                "error": str(exc),
                "issues": [],
                "saved_records": [],
            }

        fetch = self.repository.save_fetch(run["id"], fetch_result)
        raw_payload = self.archive.store(fetch_result, fetch_id=fetch["id"])
        logger.debug("run %s archived raw payload id=%s", run["id"], raw_payload["id"])

        normalized, issues = adapter.normalize(fetch_result.payload, fetched_at=fetch_result.fetched_at)
        logger.info(
            "run %s normalized: provider=%s records=%d issues=%d",
            run["id"], provider_name, len(normalized), len(issues),
        )

        saved_issue_rows = [
            self.repository.save_normalization_issue(
                issue,
                run_id=int(run["id"]),
                raw_payload_id=int(raw_payload["id"]),
            )
            for issue in issues
        ]

        saved_records: list[dict[str, object]] = []
        if not dry_run:
            for facility in normalized:
                facility_with_ref = replace(
                    facility,
                    raw_payload_ref=replace(
                        facility.raw_payload_ref,
                        raw_payload_id=int(raw_payload["id"]),
                    ),
                )
                saved = self.repository.save_facility(facility_with_ref)
                self.repository.save_source_link(
                    FacilitySourceLink(
                        provider_name=facility_with_ref.provider_name,
                        provider_record_id=facility_with_ref.provider_record_id,
                        facility_hash=facility_with_ref.normalized_hash,
                        raw_payload_id=facility_with_ref.raw_payload_ref.raw_payload_id,
                    ),
                    facility_id=int(saved["id"]),
                )
                self.publisher.publish_facility(saved)
                saved_records.append(saved)
            self.repository.save_checkpoint(provider_name, fetch_result.fetched_at.isoformat())

        finished_run = self.repository.finish_run(
            run["id"],
            records_fetched=len(normalized) + len(issues),
            records_normalized=len(normalized),
            status="completed",
        )
        logger.info(
            "run %s completed: provider=%s upserted=%d issues=%d",
            run["id"], provider_name, len(saved_records), len(issues),
        )
        return {
            "run": self.repository.serialize_timestamps(finished_run),
            "raw_payload_id": raw_payload["id"],
            "issues": saved_issue_rows,
            "saved_records": saved_records,
        }

    def reprocess_raw_payload(self, payload_id: int) -> dict[str, object]:
        payload = self.repository.get_raw_payload(payload_id)
        provider_name = str(payload["provider_name"])
        adapter = self.registry.get(provider_name)
        fetched_at = parse_utc_datetime(str(payload["fetched_at"]))
        logger.info("reprocessing raw payload id=%s provider=%s", payload_id, provider_name)

        normalized, issues = adapter.normalize(payload["payload"], fetched_at=fetched_at)
        saved_issue_rows = [
            self.repository.save_normalization_issue(
                issue,
                run_id=None,
                raw_payload_id=int(payload_id),
            )
            for issue in issues
        ]
        saved = [
            self.repository.save_facility(
                replace(
                    facility,
                    raw_payload_ref=replace(
                        facility.raw_payload_ref,
                        raw_payload_id=int(payload_id),
                    ),
                )
            )
            for facility in normalized
        ]
        logger.info(
            "reprocess complete: payload_id=%s provider=%s records=%d issues=%d",
            payload_id, provider_name, len(saved), len(issues),
        )
        return {
            "raw_payload_id": payload_id,
            "normalized_count": len(normalized),
            "issues": saved_issue_rows,
            "saved_records": saved,
        }
