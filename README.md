# STXTOFR

STXTOFR is an internal service for collecting, normalizing, and inspecting Swedish roadside facility data from a mix of provider APIs, feeds, and scrape-based sources.

The service is built as a synchronous Python backend. Provider adapters fetch upstream payloads, raw responses are archived for replay, source records are normalized into a shared facility model, and coverage findings can be queried or published for downstream handling.

## What It Covers

- Circle K
- Rasta
- IDS
- Preem
- Espresso House
- Trafikverket Parking
- TRB

## Main Service Areas

- provider adapters for API, feed, and scrape integrations
- ingestion run tracking and replay support
- raw payload archival with provenance metadata
- normalized facility storage and source attribution
- simple gap analysis for stale data, sparse coverage, and missing fields
- internal FastAPI endpoints for operations and inspection

## Running Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
uvicorn app.main:app --reload
```

## Local Commands

```bash
pytest
python scripts/run_provider.py --provider circlek --mode full
python scripts/run_gap_analysis.py
```

## Notes

- The current `legacy/STXTOFR-main.zip` archive is kept as source reference only.
- The runtime path is the API service and application modules under `app/`.
- Provider adapters should return raw payloads and normalized records through the ingestion pipeline rather than writing final JSON files.

