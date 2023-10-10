# Ops Workflow

## Normal Use

1. Trigger a provider run through `POST /runs` or `POST /runs/{provider}`.
2. Check run progress with `GET /runs` or `GET /runs/{id}`.
3. Inspect provider status with `GET /providers` and `GET /providers/{provider}/status`.
4. Query normalized facilities with `GET /facilities`.
5. Run gap analysis with `POST /analysis/gaps` and inspect results with `GET /gaps`.

## Replay

1. Find the archived payload id from a run or provider fetch record.
2. Call `POST /reprocess/{record_id}`.
3. Review the updated normalized rows and replay metadata.

## Local Development

- use `docker-compose.yml` for PostgreSQL and Kafka
- set `STXTOFR_PUBLISHER_BACKEND=noop` unless queue publishing is under test
- use the file archive backend when a local database is not available

