# 05. Technology Decisions

Every technology choice below is pinned in `requirements.txt` /
`pyproject.toml` and actually exercised by the pipeline — this is not an
aspirational stack list.

## Processing engine: PySpark 4.0+ / Delta Lake 4.0+

**Why:** Distributed DataFrame processing scales horizontally with data
volume (the core "Volume" constraint from [Chapter 2](02-business-problem.md)),
and Delta Lake adds ACID transactions, schema evolution (`mergeSchema`),
time travel, and Change Data Feed (`delta.enableChangeDataFeed`) on top of
plain Parquet. The medallion layers
(`src/medenroll834/ingestion/{bronze,silver,gold}.py`) write exclusively in
Delta format.

**Why Delta over plain Parquet:** Parquet alone is already a columnar,
compressed format — splitting data into column chunks means a query that
only needs a few fields (e.g. `plan_id`, `coverage_start_date`) reads far
less from disk than a row-oriented format would, and per-column min/max
statistics let Spark skip whole files that can't match a filter. Delta
Lake keeps that storage format but adds a transaction log on top of it:
concurrent/retried writes (an Airflow retry re-running Bronze) can't
corrupt or partially overwrite a table, `mergeSchema` lets a layer tolerate
an added column without a manual migration, and `delta.enableChangeDataFeed`
lets a future consumer read only the rows that changed since a given
version instead of re-scanning the whole table (see
[Chapter 37](37-future-improvements.md) for where this is headed next).

## Orchestration: Apache Airflow 3.x (TaskFlow API, CeleryExecutor)

**Why:** The core requirement here is production-grade orchestration — a
task-dependency graph, automatic retries with backoff, and visibility into
every run — delivered by a widely-adopted, actively maintained open-source
project with a large operator/provider ecosystem (official Databricks and
AWS provider packages, in this case) rather than a proprietary scheduler.
The `@dag`/`@task` TaskFlow decorators
(`05_orchestration/airflow/01_dags/med834_e2e_pipeline.py`) keep each stage
as a typed Python function with explicit inputs/outputs, and the same DAG
definition runs identically in this project's own Docker Compose stack or
in a managed service (MWAA, Cloud Composer, Astronomer) with no code
change — a proprietary enterprise scheduler would require a dedicated
license and operator training, while Airflow's skill set and DAG code are
directly portable.

## Compute platform: Databricks (Free/Serverless tier compatible)

**Why:** Managed Spark clusters, Unity Catalog governance, and Databricks
SQL warehouses in one workspace, with a SQL-callable `WorkspaceClient` SDK
(`databricks.sdk`) used directly from both the Airflow DAG and the
reporting/OLTP-loader modules — no separate JDBC driver management.

## Operational store: PostgreSQL (OLTP)

**Why:** A relational, foreign-key-enforced schema (`CORE`/`ENRL`/`UTIL` —
see [Chapter 9](09-database-schema.md)) is the natural fit for
application-facing enrollment reads/writes, and is intentionally **kept
separate** from the analytical Delta star schema so analytical query load
never contends with operational traffic. `psycopg2` is used directly
(`src/medenroll834/oltp/oltp_loader.py`) rather than an ORM, keeping the
upsert SQL explicit and auditable.

## Object storage: S3-compatible blob storage (POC) / AWS S3 (enterprise target)

**Why:** Databricks' free/serverless tier does not support a live
connection to an external AWS S3 bucket, so AWS S3 was not a feasible
landing layer for this proof of concept. A third-party S3-compatible blob
provider was used instead for the POC's landing/archive/report storage
(`src/medenroll834/storage/blob_storage.py`). For an enterprise deployment
with a paid Databricks tier, AWS S3 is the recommended target — the
interface is already implemented at
`src/medenroll834/s3/s3_utils.py` with the equivalent `upload_834` /
`list_landing_files` / `archive_834` functions, so swapping storage
providers requires no change to Bronze/Silver/Gold, OLTP, OLAP, BI, or AI
code.

## Containerization: Docker / Docker Compose

**Why:** The full Airflow stack (webserver, scheduler, worker, Postgres
metadata DB, Redis broker) is reproducible with one `docker compose up -d`
(`docker-compose.yaml`), with the project bind-mounted at
`/opt/airflow/project` so `src/` and `generator/` import directly into DAG
tasks without a separate package build step.

## CI/CD: GitHub Actions

**Why:** Free for public repos, with first-class native GitHub integration
(no separate CI account/billing to provision) and a large marketplace of
pre-built actions for exactly the steps this project needs (Python setup,
Docker buildx/push, Codecov upload). The existing
`.github/workflows/ci.yml` / `cd.yml` cover lint, test, security scan,
build, and Docker image publish — see [Chapter 31](31-ci-cd.md) for the
full pipeline and a known path-configuration bug that currently needs
fixing in `ci.yml`.

## Summary table

| Concern | Technology | Why (grounded in this repo) |
|---|---|---|
| Distributed processing | PySpark 4.0+, Delta Lake 4.0+ | Scales with volume; ACID + schema evolution on Bronze/Silver/Gold |
| Orchestration | Apache Airflow 3.x (TaskFlow, Celery) | Retries, task graph, Variable overrides, UI, portable open-source skill set |
| Compute platform | Databricks (Serverless-compatible) | Managed Spark + Unity Catalog + SQL warehouses + `databricks-sdk` |
| Operational store | PostgreSQL | FK-enforced OLTP schema, kept apart from OLAP |
| OLTP driver | `psycopg2` (raw SQL) | Explicit, auditable upsert statements |
| Object storage (POC) | S3-compatible blob storage | Works on Databricks' free tier, where native AWS S3 access isn't available |
| Object storage (enterprise) | AWS S3 | Same interface (`s3_utils.py`), swap-in only, once on a paid Databricks tier |
| Containerization | Docker / Docker Compose | One-command reproducible Airflow stack |
| CI/CD | GitHub Actions | Free, native GitHub integration, large action marketplace |
| IaC | Terraform | S3 bucket + policy as code (`04_infrastructure/01_terraform`) |

