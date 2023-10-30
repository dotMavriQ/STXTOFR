# Export Bundle

The final handoff from STXTOFR is a versioned JSON DTO bundle.

## Purpose

The export is meant to look like the payload that would be handed to a downstream backend, without this repo needing to own that backend or its infrastructure.

## Endpoint

- `POST /exports/facilities`

## Shape

The export returns:

- `metadata`
- `records`

`metadata` includes:

- schema version
- build time
- record count
- latest completed source runs included in the build context

`records` contains effective final rows only:

- imported facilities with human overrides already applied
- manual-only facilities created during review

The export does not include:

- raw payload bodies
- source-layer duplicates
- Baserow-specific review metadata
- downstream transport details
