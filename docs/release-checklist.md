# Release Checklist

Use this checklist before treating STXTOFR as release-ready for demo, review, or handoff.

This list is written for the local stack proved on October 25, 2023.

## 1. Local Stack

- [ ] Start Postgres and Baserow:
  - `docker compose up -d db baserow`
- [ ] Start the API:
  - `uvicorn app.main:app --host 127.0.0.1 --port 8000`
- [ ] Confirm readiness:
  - `curl -sS http://127.0.0.1:8000/ready`
- [ ] Confirm Baserow is reachable:
  - open `http://localhost:8080`

Good result:
- readiness returns database `status: ok`
- Baserow login page loads

## 2. Regression Suite

- [ ] Run the full automated test suite:
  - `python -m pytest -q`

Good result:
- all tests pass

Observed on October 25, 2023:
- `58 passed`

## 3. Import Pass

- [ ] Run a full provider import:
  - `curl -sS -X POST http://127.0.0.1:8000/runs -H 'Content-Type: application/json' --data '{"mode":"full","dry_run":false}'`
- [ ] Inspect the run list:
  - `curl -sS http://127.0.0.1:8000/runs`
- [ ] Inspect normalization issues:
  - `curl -sS http://127.0.0.1:8000/issues`

Good result:
- all seven providers complete
- the effective dataset is populated
- normalization issues are understandable and attributable to upstream data, not app failure

Observed on October 25, 2023:
- completed providers: `circlek`, `rasta`, `ids`, `preem`, `espresso_house`, `trafikverket`, `trb`
- effective facilities after import: `2031`
- issue backlog: `739`, all Circle K coordinate warnings

## 4. Map QA

- [ ] Inspect the map page:
  - `http://127.0.0.1:8000/map`
- [ ] Inspect the map data snapshot:
  - `curl -sS 'http://127.0.0.1:8000/map/data?view=effective'`

Good result:
- source and effective layers are both present
- changed and manual rows are distinguishable
- counts match expectations after import or curation

Observed after live curation on October 25, 2023:
- `source_count: 2031`
- `effective_count: 2032`
- `changed_count: 4`
- `manual_count: 1`

## 5. Baserow Review Flow

- [ ] Confirm Baserow token and table id are configured in `.env`
- [ ] Bootstrap the review table schema:
  - `curl -sS -X POST http://127.0.0.1:8000/curation/bootstrap`
- [ ] Push review rows:
  - `curl -sS -X POST http://127.0.0.1:8000/curation/push`
- [ ] Make a few edits in Baserow
- [ ] Add one manual-only row in Baserow with `row_origin=manual`
- [ ] Pull review changes back:
  - `curl -sS -X POST http://127.0.0.1:8000/curation/pull`
- [ ] Inspect sync history:
  - `curl -sS http://127.0.0.1:8000/curation/syncs`

Good result:
- bootstrap creates missing columns or reports that they already exist
- push completes without duplication
- pull creates overrides for edited imported rows
- pull creates a manual-only facility for the manual row
- the latest sync reflects full-table processing

Observed on October 25, 2023:
- successful full push: `2031` rows pushed
- successful full pull after pagination fix: `2032` rows pulled, `1` manual created

## 6. Effective Data Validation

- [ ] Inspect effective facilities:
  - `curl -sS 'http://127.0.0.1:8000/facilities?view=effective'`
- [ ] Confirm curated rows show `change_status: overridden`
- [ ] Confirm the manual-only row shows `change_status: manual`

Good result:
- imported rows remain intact unless curated
- curated fields win in the effective view
- manual rows appear only in the effective layer

## 7. Export Bundle

- [ ] Build an export:
  - `curl -sS -X POST http://127.0.0.1:8000/exports/facilities`
- [ ] Confirm export metadata includes schema version, build time, record count, and source runs
- [ ] Confirm exported rows reflect the effective dataset, not the raw source layer

Good result:
- export succeeds
- record count matches the effective layer
- curated and manual rows appear in the bundle

Observed on October 25, 2023:
- export record count: `2032`
- schema version: `stxtofr.facilities.v1`

## 8. Known Remaining Work

- [ ] Run the full suite against a truly isolated Postgres-backed test path instead of the default in-memory test harness
- [ ] Decide whether to add a file-writing export artifact mode
- [x] Clean up `datetime.utcnow()` deprecation warnings
