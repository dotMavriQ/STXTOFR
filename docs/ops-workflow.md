# Ops Workflow

## Standard Import Cycle

1. Trigger a full provider run: `POST /runs` (all providers) or `POST /runs/{provider}` (one provider).
2. Check run status: `GET /runs` or `GET /runs/{id}`.
3. Review normalization issues: `GET /issues` or `GET /runs/{id}`.
4. Inspect provider health and last fetch times: `GET /providers` or `GET /providers/{provider}/status`.
5. Query the imported source layer: `GET /facilities?view=source`.

## Curation Cycle

1. Ensure the Baserow review table exists and `.env` has a valid token and table id.
2. Create any missing columns: `POST /curation/bootstrap` (idempotent).
3. Push imported rows to Baserow for review: `POST /curation/push`.
4. Operator reviews rows in Baserow, edits field values, or adds manual-only rows with `row_origin=manual`.
5. Pull approved edits back: `POST /curation/pull`.
6. Inspect the effective dataset: `GET /facilities?view=effective`.
7. Open the map for visual QA: `GET /map`.

## Gap Analysis

1. Run gap analysis: `POST /analysis/gaps`.
2. Inspect findings: `GET /gaps`.

Gap findings identify regions or categories with sparse coverage across providers. Use them to prioritise manual review entry in Baserow or to flag upstream sources for investigation.

## Export

1. Build the final DTO bundle: `POST /exports/facilities`.
2. Review export history and metadata: `GET /exports`.

The bundle reflects the effective dataset at build time: imported facilities with any human overrides applied, plus manual-only facilities added through Baserow.

## Replay

1. Find the archived payload id from a run or fetch record.
2. Call `POST /reprocess/{raw_payload_id}`.
3. Review updated normalized rows and any new normalization issues.

Replay reruns normalisation from the archived payload without making a live network call. Use it when a normalization fix needs to be applied to historical data.

## Local Development

- Start PostgreSQL and Baserow: `docker compose up -d db baserow`
- Start the API: `uvicorn app.main:app --reload`
- Keep `STXTOFR_REPOSITORY_BACKEND=db` for persistence across restarts
- Use `STXTOFR_REPOSITORY_BACKEND=memory` only for short-lived debugging
- Set `STXTOFR_PUBLISHER_BACKEND=noop` unless Kafka publishing is under test
- Set `STXTOFR_BASEROW_BACKEND=api` and provide a token and table id for live curation sync
