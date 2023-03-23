#!/usr/bin/env python3
import argparse
import json

from app.ingestion.service import IngestionService
from app.services.provider_registry import build_provider_registry
from app.storage.raw_archive import build_archive_backend
from app.storage.repository import InMemoryRepository


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True)
    parser.add_argument("--mode", default="full")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repository = InMemoryRepository()
    service = IngestionService(
        repository=repository,
        registry=build_provider_registry(),
        archive=build_archive_backend(repository),
    )
    result = service.run_provider(args.provider, mode=args.mode, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

