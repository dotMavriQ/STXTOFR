# STXTOFR Build Notes

## Service Shape

- synchronous Python 3.10 backend
- FastAPI control API
- provider adapters for API, feed, and scrape sources
- raw payload archival separated from normalized storage
- rule-based gap analysis and light merge candidate generation

## Source Classification

- Circle K: API-possible but not confirmed
- Rasta: scrape-only
- IDS: hybrid
- Preem: API-native
- Espresso House: API-native
- Trafikverket Parking: API-native
- TRB: hybrid

## Runtime Layers

- `app/providers`: fetch + mapper boundaries
- `app/ingestion`: run orchestration and replay
- `app/storage`: schemas, archive backends, repository
- `app/normalization`: canonical DTOs and merge rules
- `app/analysis`: gap detection
- `app/routing`: publisher abstraction
- `app/api`: internal control endpoints

## Guardrails

- no TUI
- no top-level god script
- no hidden third-party geocoding inside providers
- no final JSON dumps as the main operating model
- keep wording plain and internal

