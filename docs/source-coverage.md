# Source Coverage

STXTOFR covers Swedish roadside facilities from free upstream sources only.

## Current Providers

- Circle K
- Rasta
- IDS
- Preem
- Espresso House
- Trafikverket Parking
- TRB

## Source Policy

- Prefer direct API or feed access when a free source exists.
- Fall back to scrape-backed capture only when no free structured source is available.
- Do not introduce paid APIs for this project.

## Provider Notes

- Circle K: scrape-backed extraction with normalized station payload mapping.
- Rasta: scrape/parser flow.
- IDS: structured station feed.
- Preem: page-data driven station import.
- Espresso House: structured coffee shop API.
- Trafikverket Parking: API-native source.
- TRB: structured widget feed with a documented manual capture fallback.

## Audit Workflow

Use the fixture-backed provider audit to spot fields that are still being dropped or left sparse after normalization.

```bash
python scripts/audit_providers.py
python scripts/audit_providers.py --provider circlek --format json
```
