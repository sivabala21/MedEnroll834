# 35. Business Impact

## Velocity

The legacy chain's six-tool handoff (EDI → ECA → Legacy Programs → SCP →
Bash → CORE Programs, see [Chapter 3](03-legacy-architecture.md)) means
enrollment changes are only as fast as the slowest, most fragile link in
that chain. The modern pipeline collapses this into a single orchestrated
DAG (`med834_e2e_pipeline`) with Airflow-managed retries, reducing the
number of places a delay or failure can silently accumulate. Faster, more
predictable enrollment processing means coverage is active sooner — which
matters directly to members submitting claims against a plan that hasn't
finished loading yet.

## Volume

Distributed PySpark processing (see [Chapter 25](25-spark-performance-optimization.md))
scales horizontally with file/record count, rather than being bound to a
single machine's throughput ceiling the way the legacy chain is. The
business impact is a pipeline whose capacity grows with the Databricks
cluster/warehouse size provisioned, not with the age and specs of a
specific on-prem server.

## Variety

A single validation ruleset (`DataFrameValidator`, [Chapter 16](16-data-quality-reject-handling.md))
applied consistently across every record — rather than variety handling
being spread implicitly across 6 different tools' individual quirks —
means a new plan code, relationship type, or malformed field is caught and
explained (`validation_errors`) in one place, by one team, instead of
surfacing as an opaque downstream CORE System load failure.

## Operational cost reduction

- **Fewer tools to license, patch, and staff** — Tidal, SSIS, DataStage,
  Legacy Programs, SCP, and Bash scripts are five-to-six separate
  operational surfaces; the modern stack is Airflow + Databricks +
  PostgreSQL.
- **Unified observability** reduces mean-time-to-diagnosis: a structured
  log line (`START SILVER | ... `) and a per-run summary dict
  (`silver_invalid=12`) replace grepping six different tool-specific log
  formats (see [Chapter 24](24-observability.md)).
- **Idempotent re-runs** (see [Chapter 22](22-idempotency-incremental-processing.md))
  mean a failed run can simply be retried rather than requiring manual
  reconciliation of partial state.

## New analytical capability (not present in the legacy system at all)

The legacy flow terminates at a CORE System keyword-file load — there is
no historical reporting, BI, or ad hoc analytics path. The modern platform
adds, at no cost to the operational path:

- An **OLAP star schema** (`FACT_ENROLLMENT` + 5 dimensions,
  [Chapter 19](19-olap-star-schema.md)) purpose-built for enrollment
  analytics.
- A **traditional analytics** path: scheduled HTML reports over fixed KPIs
  ([Chapter 27](27-databricks-sql-dashboard.md)).
- An **AI analytics** path: natural-language ad hoc exploration via Genie
  ([Chapter 28](28-genie-genai.md)), answering questions like "How many
  members are enrolled on each plan?" without writing SQL.

This is a genuinely new business capability, not a modernization of an
existing one — the legacy architecture has no equivalent to show
stakeholders a trend in enrollment-by-plan over time, for example.

## Compliance and audit posture

Every enrollment record and every rejected record traces back to its
source file via a transaction identifier
(`ENRL_EDXN_TRANSACTION`/`DIM_TRANSACTION_SOURCE`, see
[Chapter 24](24-observability.md)) — a stronger audit trail than the
legacy proprietary intermediate format allows, which matters directly for
HIPAA-adjacent compliance reviews (see [Chapter 29](29-security.md)).
