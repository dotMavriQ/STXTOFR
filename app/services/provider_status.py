from __future__ import annotations

from app.providers.base import ProviderAdapter
from app.storage.repository import Repository


class ProviderStatusService:
    def __init__(self, repository: Repository):
        self.repository = repository

    def build_status(self, adapter: ProviderAdapter) -> dict[str, object]:
        metadata = adapter.get_source_metadata()
        summary = self.repository.get_provider_status(metadata.provider_name)
        return {
            "provider": metadata.provider_name,
            "source_type": metadata.source_type,
            "supports_incremental": adapter.supports_incremental(),
            "last_run_status": summary.get("last_run_status"),
            "last_run_finished_at": summary.get("last_run_finished_at"),
            "stale": bool(summary.get("stale", False)),
            "last_issue_count": int(summary.get("last_issue_count", 0)),
            "issue_backlog": int(summary.get("issue_backlog", 0)),
        }
