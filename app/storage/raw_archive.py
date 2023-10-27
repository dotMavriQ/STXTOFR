from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from app.core.config import get_settings
from app.providers.base import FetchResult
from app.storage.repository import Repository


class RawArchive(Protocol):
    def store(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, object]:
        ...


class RepositoryArchive:
    def __init__(self, repository: Repository):
        self.repository = repository

    def store(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, object]:
        return self.repository.save_raw_payload(fetch_result, fetch_id=fetch_id)


class FileArchive:
    def __init__(self, repository: Repository, output_dir: str):
        self.repository = repository
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def store(self, fetch_result: FetchResult, fetch_id: int | None = None) -> dict[str, object]:
        record = self.repository.save_raw_payload(fetch_result, fetch_id=fetch_id)
        path = self.output_dir / f"{record['id']}.json"
        path.write_text(json.dumps(record["payload"], ensure_ascii=False, indent=2), encoding="utf-8")
        record["archive_path"] = str(path)
        return record


def build_archive_backend(repository: Repository) -> RawArchive:
    settings = get_settings()
    if settings.archive_backend == "file":
        return FileArchive(repository=repository, output_dir=settings.file_archive_path)
    return RepositoryArchive(repository=repository)
