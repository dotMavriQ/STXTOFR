# STXTOFR

[![CI](https://github.com/DotMavriq/STXTOFR/actions/workflows/ci.yml/badge.svg)](https://github.com/DotMavriq/STXTOFR/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)

STXTOFR is an internal-style service for collecting, normalizing, reviewing, and packaging Swedish roadside facility data from free upstream sources.

It imports facility data from provider APIs, feeds, and scrape-backed sources, stores the raw payloads for replay, normalizes source records into one shared facility model, lets a human review the data through Baserow, and exposes both the imported source layer and the effective human-adjusted layer through a FastAPI control surface.

## What The Service Does

STXTOFR is built around five concrete jobs:

1. Import all supported free sources.
2. Normalize them into one facility model with stable provenance.
3. Plot the imported and effective facility layers on `/map` for spatial QA.
4. Let a human correct or add facilities through a Baserow review flow.
5. Package the final effective dataset as a versioned JSON DTO bundle for a hypothetical downstream backend.

## Supported Sources

- Circle K
- Rasta
- IDS
- Preem
- Espresso House
- Trafikverket Parking
- TRB

The project does not rely on paid APIs. Where a provider is unreliable from the CLI environment, the documented manual capture path remains acceptable, especially for TRB.

## Data Layers

STXTOFR keeps the data model explicit:

- Source layer: the latest normalized import per provider record, backed by raw payload history.
- Curation layer: human overrides and manual-only facilities pulled back from Baserow.
- Effective layer: the final dataset produced by applying human overrides on top of imported data and appending manual-only facilities.

This split matters because the service needs to show both what the source said and what a human decided to correct.

## Main Runtime Areas

- `app/providers`: fetch and normalize each upstream source.
- `app/ingestion`: orchestrate imports and replay from archived payloads.
- `app/storage`: migrations, schema, repositories, and raw payload persistence.
- `app/services/facility_view.py`: builds source vs effective facility views.
- `app/services/curation.py`: pushes review rows to Baserow and pulls edits back.
- `app/services/export_service.py`: builds versioned JSON DTO bundles from the effective dataset.
- `app/api`: internal control endpoints, readiness checks, map UI, curation sync, and export.

## Local Stack

The local stack is intended to demonstrate the full review flow:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
docker compose up -d db baserow
uvicorn app.main:app --reload
```

Key defaults:

- `STXTOFR_REPOSITORY_BACKEND=db`
- `STXTOFR_ARCHIVE_BACKEND=db`
- `STXTOFR_PUBLISHER_BACKEND=noop`
- `STXTOFR_BASEROW_BACKEND=noop` until you configure a token and table id

The app now uses Alembic-backed schema bootstrapping on startup instead of relying on plain `create_all`.

## Core Endpoints

- `POST /runs` and `POST /runs/{provider}`: import source data.
- `POST /reprocess/{raw_payload_id}`: replay normalized output from archived payloads.
- `GET /facilities?view=source|effective`: inspect imported or effective rows.
- `GET /map`: compare imported vs effective geo points visually.
- `POST /curation/bootstrap`: create missing Baserow review-table columns.
- `POST /curation/push`: push reviewable rows to Baserow.
- `POST /curation/pull`: pull human edits back into STXTOFR.
- `POST /exports/facilities`: build the final JSON DTO bundle.
- `GET /exports`: inspect export build history.

## Map And Human Review

`/map` is a QA tool, not just a visual extra.

It should let an operator confirm:

- the imported points are present
- the effective points still land correctly
- which points were changed by a human
- which facilities exist only because a human added them in review

The human-edit flow is intentionally push out, pull back:

1. STXTOFR imports and normalizes source data.
2. STXTOFR pushes review rows into Baserow.
3. A human edits core business fields or adds missing facilities.
4. STXTOFR pulls those edits back and stores durable overrides locally.
5. The effective dataset becomes the export surface.

## Final DTO Bundle

The final handoff is a generated JSON bundle, not downstream delivery infrastructure.

The bundle contains:

- export metadata such as schema version, build time, source runs, and record count
- effective final records only
- fields suitable for handing off to another backend without that backend needing to know about raw payloads, review tables, or map-specific behavior

## Development Commands

```bash
python -m pytest
python scripts/run_provider.py --provider circlek --mode full
python scripts/run_gap_analysis.py
python scripts/audit_providers.py
```

## Companion Docs

- [docs/uml.md](docs/uml.md)
- [docs/ops-workflow.md](docs/ops-workflow.md)
- [docs/source-coverage.md](docs/source-coverage.md)
- [docs/curation-workflow.md](docs/curation-workflow.md)
- [docs/baserow-setup.md](docs/baserow-setup.md)
- [docs/export-bundle.md](docs/export-bundle.md)
- [docs/release-checklist.md](docs/release-checklist.md)
- [docs/trb-capture.md](docs/trb-capture.md)
