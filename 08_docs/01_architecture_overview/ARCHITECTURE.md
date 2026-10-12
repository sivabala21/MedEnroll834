# MED834ENRL — Architecture

## Idea 1 — Modern Distributed Data Platform for Growing Data

This proof of concept demonstrates how distributed processing can prepare a
healthcare data platform for increasing data volume, velocity, and variety.
A representative business module — X12 834 enrollment processing — is
implemented as the POC, while the architecture is designed to be reusable
across other business domains.

> **Note:** "Enrollment" names the representative POC module, not the
> platform itself. The platform capability being demonstrated is distributed
> processing plus operational/analytical separation, applicable to any
> high-volume healthcare (or other) data domain.

---

## Two Capabilities

The architecture delivers two distinct, composable capabilities. Keeping
them conceptually separate is the core of the modernization story.

### Capability 1 — Distributed Processing

```
X12 834
   |
   v
Landing (object storage)
   |
   v
Databricks / PySpark
   |
   v
Bronze --> Silver --> Gold
```

**Purpose:** Handle increasing Volume, Velocity, and Variety by parsing and
validating EDI data as a distributed, replayable medallion pipeline rather
than a monolithic nightly batch job.

### Capability 2 — Analytical Platform

```
Gold
  |
  v
Star Schema (Delta Lake)
  |
  v
Databricks SQL
  |
  +--> BI / HTML Reports
  |
  +--> Genie AI
```

**Purpose:** Separate analytical workloads from operational systems and
provide governed data for BI and AI use cases, independent of the
transactional (OLTP) store.

And, feeding the operational side:

```
Gold
  |
  v
PostgreSQL OLTP database
  |
  v
Operational applications
```

**Purpose:** Provide an operational relational store for application-facing
reads/writes, decoupled from analytical query load.

---

## Final Architecture

```
                         +-----------------------+
                         |       AIRFLOW         |
                         |    Orchestration      |
                         +-----------+-----------+
                                     |
                                     v
+----------------------------------------------------------------+
|                          INGESTION                              |
|                                                                  |
|  Synthetic / X12 834 --> Landing (Object Storage / Volume)       |
|                              |                                   |
|                              v                                   |
|                    Databricks UC Volume                          |
+------------------------------+----------------------------------+
                               |
                               v
+----------------------------------------------------------------+
|                 DISTRIBUTED PROCESSING                           |
|                                                                  |
|                     Databricks / PySpark                        |
|                                                                  |
|       BRONZE              SILVER               GOLD              |
|   Raw X12 Segments   Validated Members   Analytics Model         |
|                                            |                     |
|                                      DQ / Quarantine             |
+------------------------------+----------------------------------+
                               |
                   +-----------+-----------+
                   |                       |
                   v                       v
          +-----------------+    +------------------------+
          |      OLTP       |    |         OLAP           |
          |                 |    |                        |
          | PostgreSQL      |    | Delta Lake             |
          | (OLTP database) |    | Star Schema            |
          +--------+--------+    +-----------+------------+
                   |                         |
                   |                         v
```

---

## 1. Ingestion Layer

X12 834 EDI files land in object storage before distributed processing picks
them up. For the POC, this is S3-compatible blob storage feeding a
Databricks Unity Catalog Volume; see the POC vs. Enterprise note below for
why the target architecture uses S3 directly.

## 2. Distributed Processing (Databricks + PySpark)

| Layer | Content | Purpose |
|-------|---------|---------|
| **Bronze** | Raw X12 segments | Unmodified ingestion, replayable |
| **Silver** | Validated, conformed members | Business-rule-applied, deduplicated |
| **Gold** | Analytics-ready enrollments | Denormalized, consumption-ready |
| **Quarantine** | DQ failures | Isolated invalid records for remediation |

## 3. Operational vs. Analytical Separation

This is one of the most important parts of the modernization story: **Gold
feeds two independent stores with different purposes.** They are not
interchangeable, and the architecture must not blur this line.

### OLTP — PostgreSQL (operational database)

Used for:
- Operational entities (members, subscribers, groups, plans)
- Enrollment and transaction records
- Application-facing reads/writes

### OLAP — Delta Lake / Gold Star Schema

```
DIM_MEMBER
DIM_PLAN
DIM_GROUP
DIM_DATE
DIM_TRANSACTION_SOURCE
        |
        v
FACT_ENROLLMENT
```

Used for:
- Analytics and aggregations
- BI and dashboarding
- AI / Genie natural-language analytics

**OLAP Star Schema** — Builds dimension and fact tables from the Gold layer,
including `DIM_MEMBER`, `DIM_PLAN`, `DIM_GROUP`, `DIM_DATE`,
`DIM_TRANSACTION_SOURCE`, and `FACT_ENROLLMENT` at the center of the model.

## 4. Analytical Consumption — Two Modes

Both the HTML report and Genie are **consumers of the governed analytical
layer**, not steps inside the pipeline's transformation logic:

```
                   GOLD
                    |
                    v
              ANALYTICAL LAYER (Databricks SQL)
                    |
          +---------+----------+
          |                    |
          v                    v
     HTML Report             Genie
          |                    |
          v                    v
     Human-readable      Natural-language
       reporting           analytics
```

| Mode | Flow | Use Case |
|------|------|----------|
| **Traditional analytics** | Predefined SQL -> HTML report | Fixed KPIs, scheduled reporting |
| **AI analytics** | Natural language -> SQL -> result | Ad hoc exploration via Genie |

Genie is positioned as part of the analytical consumption layer alongside
BI/reporting — it queries the same governed Gold/OLAP data, it does not
participate in Bronze/Silver/Gold transformation.

## 5. Orchestration — Airflow

Airflow orchestrates the end-to-end workflow, including ingestion,
Databricks processing, OLTP loading, reporting, and archival, with task
dependencies, retries, and failure handling. Airflow sits **above** the
pipeline as orchestration — it is not a data layer itself:

```
                      AIRFLOW
                         |
          +--------------+--------------+
          v              v              v
      Ingestion      Databricks      OLTP / Reporting
```

The archive step runs with `trigger_rule="all_done"`, so processed files are
moved to the archive location regardless of whether downstream analytical
steps (report, Genie) succeeded — a deliberate operational-workflow decision
rather than a pure linear transformation chain.

## 6. POC vs. Enterprise Storage

> For the proof of concept, S3-compatible blob storage and Databricks Unity Catalog
> Volumes provide the development storage path. In the target enterprise
> architecture, the landing/object-storage layer can be implemented using
> AWS S3 while retaining the same downstream Bronze-Silver-Gold, OLTP, OLAP,
> BI, and AI architecture.

This distinction is intentional and should be stated explicitly in any
presentation of this architecture, so the storage choice reads as a POC
convenience rather than a design limitation.

## Alignment Summary

| # | Adjustment | Status |
|---|------------|--------|
| 1 | Rename to "Modern Distributed Data Platform for Growing Data" | Done (this doc) |
| 2 | Separate OLTP (PostgreSQL) and OLAP (Delta/Star Schema) explicitly | Done |
| 3 | Add `FACT_ENROLLMENT` explicitly to the star schema | Done |
| 4 | Position Genie as an analytical consumer, not a pipeline step | Done |
| 5 | Position HTML/BI and Genie as two consumption paths | Done |
| 6 | Distinguish POC storage (blob storage) from enterprise storage (S3) | Done |
| 7 | Keep Airflow as orchestration, not a data layer | Done |

See also:
- [E2E Pipeline Runbook](../04_operations_deployment/E2E_PIPELINE_RUNBOOK.md) — implementation detail, deployment steps
- [Backout Runbook](../04_operations_deployment/BACKOUT_RUNBOOK.md) — rollback procedures
