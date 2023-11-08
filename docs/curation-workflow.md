# Curation Workflow

STXTOFR treats Baserow as the editing surface, not the long-term source of truth.

## Review Flow

1. Import providers into the source layer with `POST /runs` or `POST /runs/{provider}`.
2. Create the Baserow review table described in [docs/baserow-setup.md](baserow-setup.md).
3. Push review rows to Baserow with `POST /curation/push`.
4. Let a human update core business fields or add missing facilities in Baserow.
5. Pull the reviewed data back with `POST /curation/pull`.
6. Query `GET /facilities?view=effective` or open `/map` to inspect the final effective layer.

## What A Human Can Change

- facility name
- category
- address fields
- latitude and longitude
- phone
- opening hours
- services
- notes
- verification status

## What Stays Read-Only In Practice

- imported provider identity
- source URL
- source type
- raw payload provenance

Those fields may still appear in Baserow for review context, but STXTOFR preserves them as provenance rather than treating them as editable final-state fields.

## Override Rules

- Imported source rows stay preserved in STXTOFR.
- Human corrections are stored separately as override rows.
- On the effective layer, human values win until they are changed or cleared.
- Manual-only facilities are appended to the effective layer and never overwrite imported provider records.
