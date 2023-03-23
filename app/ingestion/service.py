from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from app.core.exceptions import RecordNotFound
from app.normalization.models import FacilitySourceLink
from app.providers.base import RunContext
from app.routing.publisher import build_publisher
from app.services.provider_registry import ProviderRegistry
from app.storage.raw_archive import RawArchive
from app.storage.repository import InMemoryRepository


class IngestionService:
    def __init__(
        self,
        repository: InMemoryRepository,
        registry: ProviderRegistry,
        archive: RawArchive,
    ):
        self.repository = repository
        self.registry = registry
        self.archive = archive
        self.publisher = build_publisher()

    def run_provider(self, provider_name: str, mode: str = "full", dry_run: bool = False) -> dict[str, object]:
        adapter = self.registry.get(provider_name)
        checkpoint = self.repository.get_checkpoint(provider_name) if mode == "incremental" else None
        run = self.repository.create_run(provider_name=provider_name, mode=mode, dry_run=dry_run)
        run_context = RunContext(mode=mode, checkpoint=checkpoint, dry_run=dry_run)

        fetch_result = adapter.fetch(run_context)
        fetch = self.repository.save_fetch(run["id"], fetch_result)
        raw_payload = self.archive.store(fetch_result, fetch_id=fetch["id"])
        normalized, issues = adapter.normalize(fetch_result.payload, fetched_at=fetch_result.fetched_at)

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
        return {
            "run": self.repository._serialize_timestamps(finished_run),
            "raw_payload_id": raw_payload["id"],
            "issues": [issue.message for issue in issues],
            "saved_records": saved_records,
        }

    def reprocess_raw_payload(self, payload_id: int) -> dict[str, object]:
        payload = self.repository.get_raw_payload(payload_id)
        adapter = self.registry.get(str(payload["provider_name"]))
        fetched_at = datetime.fromisoformat(str(payload["fetched_at"]))
        normalized, issues = adapter.normalize(payload["payload"], fetched_at=fetched_at)
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
        return {
            "raw_payload_id": payload_id,
            "normalized_count": len(normalized),
            "issues": [issue.message for issue in issues],
            "saved_records": saved,
        }
