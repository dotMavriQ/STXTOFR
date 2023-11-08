# Baserow Setup

This is the exact review table shape STXTOFR expects for the first live Baserow round-trip.

## What To Create

Create one table in Baserow for facility review rows.

Use these column names exactly, because STXTOFR currently syncs by user field name:

| Column name | Type | Purpose |
| --- | --- | --- |
| `stxtofr_key` | Text | Stable key for STXTOFR-managed rows, for example `source:123` |
| `row_origin` | Single select or text | Use `source` for imported rows and `manual` for human-created rows |
| `source_facility_id` | Number | STXTOFR source facility id; leave blank for manual-only rows |
| `provider_name` | Text | Read-only context from STXTOFR |
| `provider_record_id` | Text | Read-only upstream record id |
| `facility_brand` | Text | Read-only or lightly editable display context |
| `source_type` | Text | Read-only context such as `api`, `feed`, or `scrape` |
| `source_url` | URL or text | Read-only source reference |
| `facility_name` | Text | Human-editable final name |
| `category` | Text or single select | Human-editable final category |
| `formatted_address` | Long text | Human-editable full address |
| `street` | Text | Human-editable street |
| `city` | Text | Human-editable city |
| `region` | Text | Human-editable region |
| `postal_code` | Text | Human-editable postal code |
| `country_code` | Text | Needed mainly for manual-only rows; default `se` is fine |
| `latitude` | Number | Human-editable latitude |
| `longitude` | Number | Human-editable longitude |
| `phone` | Text | Human-editable phone |
| `opening_hours` | Long text | Human-editable opening hours |
| `services` | Long text | Comma-separated service list, for example `parking,wifi,toalett` |
| `notes` | Long text | Human review notes |
| `verified_status` | Single select or text | Use `verified` or `unverified` |

## Recommended Rules

- Treat these as review context, not normal edit targets:
  - `stxtofr_key`
  - `source_facility_id`
  - `provider_name`
  - `provider_record_id`
  - `facility_brand`
  - `source_type`
  - `source_url`
- Treat these as the main editable fields:
  - `facility_name`
  - `category`
  - `formatted_address`
  - `street`
  - `city`
  - `region`
  - `postal_code`
  - `latitude`
  - `longitude`
  - `phone`
  - `opening_hours`
  - `services`
  - `notes`
  - `verified_status`

## How Manual Rows Work

If a human wants to add a facility that did not come from a source import:

1. Create a new Baserow row.
2. Set `row_origin` to `manual`.
3. Leave `source_facility_id` blank.
4. Fill in at least:
   - `facility_name`
   - `category`
5. Fill in geo and address fields as available.

On pull, STXTOFR will treat that row as a manual-only facility.

## Current Sync Behavior

- Push:
  - STXTOFR creates or updates imported review rows in Baserow.
  - Manual-only rows are not pushed from STXTOFR today.
- Pull:
  - `row_origin=source` rows become field overrides on imported facilities.
  - `row_origin=manual` rows become manual-only facilities in STXTOFR.

## Environment Values You Need After Table Creation

Once the table exists, put these in `.env`:

```env
STXTOFR_BASEROW_BACKEND=api
STXTOFR_BASEROW_URL=http://127.0.0.1:8080
STXTOFR_BASEROW_TOKEN=<your token>
STXTOFR_BASEROW_TABLE_ID=<your table id>
```

Then restart the STXTOFR API and test:

```bash
curl -sS -X POST http://127.0.0.1:8000/curation/push
curl -sS -X POST http://127.0.0.1:8000/curation/pull
```
