# 01. Project Overview

## Scope

MedEnroll834 is a modernization prototype for healthcare enrollment
processing. It models a production-style pipeline for inbound HIPAA X12 834
transactions — the standard EDI format carriers and employers use to enroll,
change, and terminate member benefit coverage.

The prototype demonstrates a faster, more observable, and more reliable
operating model than the legacy file-handoff chain it replaces. **It does not
use production member data** — all 834 files are synthetically generated
(see [Chapter 10](10-synthetic-data-generation.md)).

## Objectives

- Handle high-volume enrollment files through distributed processing
  (PySpark on Databricks) instead of a single-machine batch job.
- Preserve raw source data and track every transformation (Bronze → Silver →
  Gold medallion architecture with Delta Lake).
- Serve both **operational** enrollment processing (PostgreSQL OLTP) and
  **analytical** reporting (Delta Lake star schema + Databricks SQL),
  explicitly kept as two separate consumption paths.
- Demonstrate modern data-engineering practices — idempotent pipelines,
  data-quality quarantine, Airflow orchestration, CI/CD, and Infrastructure
  as Code — in a portfolio-ready project.

## Conceptual model

The conceptual model separates **enrollment processing** from **analytical
data consumption**. Member, employer group, subscriber, benefit plan, and
enrollment entities drive enrollment decisions; the same validated data also
feeds historical/analytical reporting.

```mermaid
flowchart LR

    subgraph SOURCE["SOURCE DOMAIN"]
        S["834 Enrollment Transaction"]
    end

    subgraph PROCESS["ENROLLMENT PROCESSING DOMAIN"]
        P1["Transaction Ingestion"]
        P2["Enrollment Normalization"]
        P3["Validation & Data Quality"]
        P4["Enrollment Business Rules"]
        P1 --> P2 --> P3 --> P4
    end

    subgraph OPERATIONAL["OPERATIONAL DATA DOMAIN"]
        O1["Employer Group"]
        O2["Subscriber"]
        O3["Member"]
        O4["Benefit Plan"]
        O5["Enrollment"]
        O6["Address"]
        O7["Transaction Control"]
        O8["Rejected Record"]
    end

    subgraph ANALYTICAL["ANALYTICAL DATA DOMAIN"]
        A1["Historical Enrollment"]
        A2["Member Analytics"]
        A3["Plan Analytics"]
        A4["Group Analytics"]
    end

    subgraph CONSUMPTION["CONSUMPTION DOMAIN"]
        C1["Operational Reporting"]
        C2["Analytical Reporting"]
        C3["Natural Language Analytics"]
    end

    S --> P1
    P4 --> O1
    P4 --> O2
    P4 --> O3
    P4 --> O4
    P4 --> O5
    P4 --> O6
    P1 --> O7
    P3 -. "Invalid" .-> O8
    O1 --> A1
    O2 --> A1
    O3 --> A1
    O4 --> A1
    O5 --> A1
    A1 --> A2
    A1 --> A3
    A1 --> A4
    O5 --> C1
    A2 --> C2
    A3 --> C2
    A4 --> C2
    A2 --> C3
    A3 --> C3
    A4 --> C3
```

Rejected/invalid records enter a controlled remediation path (quarantine)
rather than silently failing or corrupting downstream tables — see
[Chapter 16](16-data-quality-reject-handling.md).

## Target workflow

1. Receive an 834 file from a (synthetic, in this POC) external source.
2. Land the original file in object storage (S3-compatible blob storage in
   the POC, AWS S3 in the enterprise target).
3. Parse, validate, and normalize member enrollment transactions with
   PySpark (`EDI834Parser`, `SilverLayer`).
4. Apply plan-assignment business rules (`GoldLayer`).
5. Load operational enrollment data into PostgreSQL (`load_gold_to_oltp`).
6. Publish curated Delta tables (OLAP star schema) for analytics and
   dashboards (Databricks SQL + Genie).

Apache Airflow coordinates the end-to-end workflow
(`med834_e2e_pipeline` DAG). Databricks provides scalable processing,
Delta Lake storage, and SQL analytics.

## Design principles

- **Idempotent and incremental** — natural-key upserts (`ON CONFLICT ...
  DO UPDATE`) in the OLTP loader, `overwrite`/`append` semantics with
  `mergeSchema` in the Delta layers.
- **Invalid records are quarantined** with actionable rejection reasons
  (`validation_errors` array column), not silently dropped.
- **Every run emits metrics, logs, and lineage signals** — `setup_logging`,
  `log_pipeline_start`/`log_pipeline_end`, and run summaries returned from
  each layer (`{"bronze_segments": ..., "silver_valid": ..., "gold_records": ...}`).

## Success measures

The prototype targets the legacy system's velocity, volume, and variety
constraints described in [Chapter 2](02-business-problem.md). A validated
smoke-test run (1 synthetic file, 3 subscribers + 3 dependents) produced:

```text
BRONZE     : segments_loaded = 83
SILVER     : total = 6, valid = 6, invalid = 0
GOLD       : records = 6
QUARANTINE : records = 0
```

See [Chapter 34](34-legacy-vs-modern-benchmark.md) for the full legacy vs.
modern comparison and what is measured vs. what is a design target.
