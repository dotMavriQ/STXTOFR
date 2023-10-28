from __future__ import annotations

import os
import sys
import itertools
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("STXTOFR_REPOSITORY_BACKEND", "memory")


@pytest.fixture(autouse=True)
def reset_repository_state() -> None:
    from app.api.dependencies import get_repository

    repository = get_repository()
    if hasattr(repository, "runs"):
        repository._ids = itertools.count(1)
        repository.runs.clear()
        repository.fetches.clear()
        repository.raw_payloads.clear()
        repository.facilities.clear()
        repository.facility_links.clear()
        repository.normalization_issues.clear()
        repository.gaps.clear()
        repository.checkpoints.clear()
        repository.merge_candidates.clear()
        repository.merged_facilities.clear()
        repository.facility_curations.clear()
        repository.manual_facilities.clear()
        repository.curation_syncs.clear()
        repository.export_builds.clear()
