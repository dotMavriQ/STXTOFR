# STXTOFR Architecture

STXTOFR is a multi-source ETL service for Swedish roadside facility data. Provider adapters fetch upstream data through HTTP, normalise records into a shared facility model, and persist the output to PostgreSQL. A human review layer via Baserow allows field-level corrections before the final dataset is assembled and exported as a versioned JSON bundle.

## System Context

```mermaid
flowchart TB
    OP([Operator])

    subgraph Sources["Upstream Sources"]
        S1["Circle K<br/>station search"]
        S2["Rasta<br/>HTML pages"]
        S3["IDS<br/>stations feed"]
        S4["Preem<br/>station API"]
        S5["Espresso House<br/>coffee shop API"]
        S6["Trafikverket<br/>parking API"]
        S7["TRB<br/>widget feed"]
    end

    subgraph Core["STXTOFR"]
        API[REST API]
        ORCH[Ingestion Orchestrator]
        ADP[Provider Adapters]
        CUR[Curation Service]
        VIEW[Facility View Service]
        ANA[Gap Analysis]
        EXP[Export Service]
        DB[(PostgreSQL)]
    end

    BR[(Baserow)]
    DS([Downstream consumer])

    OP -- HTTP commands --> API
    API --> ORCH
    API --> CUR
    API --> VIEW
    API --> ANA
    API --> EXP

    ORCH --> ADP
    ADP -- HTTP --> Sources
    ORCH --> DB

    CUR --> DB
    CUR <--> BR

    VIEW --> DB
    ANA --> DB
    EXP --> DB
    EXP -- JSON bundle --> DS
```

## ETL Pipeline

Each import run follows a fixed pipeline. The raw payload is archived before normalisation so any run can be replayed without a live network call.

```mermaid
flowchart TD
    A[POST /runs or POST /runs/{provider}] --> B[IngestionService]
    B --> C[adapter.fetch]
    C --> D{HTTP\nok?}
    D -- no --> E[mark run failed\nlog error]
    D -- yes --> F[archive raw payload]
    F --> G[adapter.normalize]
    G --> H[upsert source facilities]
    H --> I[save source links\nand normalization issues]
    I --> J[finish run\nrecord counts]
    J --> K[publish run event]
```

## Import Run Sequence

```mermaid
sequenceDiagram
    participant OP as Operator
    participant API as REST API
    participant ORCH as IngestionService
    participant ADP as ProviderAdapter
    participant ARC as RawArchive
    participant DB as Repository
    participant PUB as Publisher

    OP->>API: POST /runs/{provider}
    API->>DB: create_run(provider, mode, dry_run)
    API->>ORCH: run_provider(provider, mode, dry_run)
    ORCH->>ADP: fetch(run_context)
    ADP->>ADP: HTTP request to upstream source
    ADP-->>ORCH: FetchResult
    ORCH->>DB: save_fetch(run_id, url, status_code, checksum)
    ORCH->>ARC: store(provider, payload, replay_key)
    ORCH->>ADP: normalize(payload, fetched_at)
    ADP-->>ORCH: facilities[], issues[]
    ORCH->>DB: upsert_facilities(facilities)
    ORCH->>DB: save_source_links(facility_id, raw_payload_id)
    ORCH->>DB: save_issues(issues)
    ORCH->>DB: finish_run(run_id, status, counts)
    ORCH->>PUB: publish(run_complete_event)
    ORCH-->>API: run summary
    API-->>OP: 201 run result
```

## Curation Roundtrip

Baserow is used as the editing surface. STXTOFR pushes review rows out and pulls approved edits back. Human corrections are stored as discrete override rows and applied on top of the source layer when assembling the effective dataset.

```mermaid
sequenceDiagram
    participant OP as Operator
    participant API as REST API
    participant CUR as CurationService
    participant DB as Repository
    participant BR as Baserow

    OP->>API: POST /curation/push
    API->>CUR: push_to_baserow()
    CUR->>DB: load source facilities and existing curations
    CUR->>BR: create or update review rows
    CUR->>DB: save Baserow row linkage
    CUR->>DB: record sync run (push)
    CUR-->>API: push summary
    API-->>OP: 200 sync result

    Note over OP,BR: Operator edits field values or adds manual rows in Baserow

    OP->>API: POST /curation/pull
    API->>CUR: pull_from_baserow()
    CUR->>BR: list all review rows
    CUR->>DB: upsert field overrides for source-origin rows
    CUR->>DB: upsert manual facilities for manual-origin rows
    CUR->>DB: record sync run (pull)
    CUR-->>API: pull summary
    API-->>OP: 200 sync result
```

## Effective Dataset Assembly

The effective dataset is assembled at query time. Human override values win over imported source values for any field that was corrected. Manual-only facilities are appended and never overwrite source rows.

```mermaid
flowchart LR
    A[normalized_facilities\nsource imports] --> D[FacilityViewService]
    B[facility_curations\nhuman field overrides] --> D
    C[manual_facilities\nhuman-added rows] --> D
    D --> E[GET /facilities?view=effective]
    D --> F[GET /map/data]
    D --> G[POST /exports/facilities]
```

## Storage Schema

```mermaid
flowchart TD
    subgraph Capture["Capture"]
        IR[ingestion_runs]
        PF[provider_fetches]
        RP[raw_payloads]
    end

    subgraph Source["Source Layer"]
        NF[normalized_facilities]
        FSL[facility_source_links]
        NI[normalization_issues]
        PC[provider_checkpoints]
    end

    subgraph Review["Review Layer"]
        FC[facility_curations]
        MF[manual_facilities]
        CSR[curation_sync_runs]
    end

    subgraph Output["Output"]
        EB[export_builds]
        GF[gap_findings]
    end

    IR --> PF --> RP
    RP --> NF
    NF --> FSL
    NF --> NI
    NF --> FC
    FC --> EB
    MF --> EB
```

## Replay Flow

Any archived raw payload can be reprocessed without a live network call. This allows normalization fixes to be applied retroactively without re-fetching upstream sources.

```mermaid
sequenceDiagram
    participant OP as Operator
    participant API as REST API
    participant ORCH as IngestionService
    participant ARC as RawArchive
    participant ADP as ProviderAdapter
    participant DB as Repository

    OP->>API: POST /reprocess/{raw_payload_id}
    API->>ORCH: reprocess_raw_payload(payload_id)
    ORCH->>ARC: load(payload_id)
    ARC-->>ORCH: raw payload + metadata
    ORCH->>ADP: normalize(payload, fetched_at)
    ADP-->>ORCH: facilities[], issues[]
    ORCH->>DB: upsert_facilities(facilities)
    ORCH->>DB: save_source_links + issues
    ORCH-->>API: reprocess summary
    API-->>OP: 200 result
```
