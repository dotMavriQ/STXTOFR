#!/usr/bin/env python3
import json

from app.analysis.service import AnalysisService
from app.routing.publisher import build_publisher
from app.storage.repository import InMemoryRepository


def main() -> None:
    repository = InMemoryRepository()
    service = AnalysisService(repository=repository, publisher=build_publisher())
    print(json.dumps(service.run_gap_analysis(), indent=2, default=str))


if __name__ == "__main__":
    main()

