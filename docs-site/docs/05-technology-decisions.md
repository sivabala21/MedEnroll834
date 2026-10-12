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

**Local dev friction solved:** `src/medenroll834/common/spark_session.py`
(`get_local_spark()`) exists specifically because local Windows PySpark has
three real gotchas, each addressed directly:

- Delta needs `spark.sql.extensions` / `spark.sql.catalog.spark_catalog`
  set *before* the session is created (static configs).
- `PYSPARK_PYTHON` defaults to the literal string `python3`, which doesn't
  exist on Windows — pinned to `sys.executable`.
- Delta shells out to Hadoop's `winutils.exe` on Windows — vendored under
  `12_tools/hadoop/bin` so a clean checkout doesn't need a separate Hadoop
  install.

## Orchestration: Apache Airflow 3.x (TaskFlow API, CeleryExecutor)

**Why:** Mature retry/backoff semantics, a task-dependency graph that
mirrors the pipeline's real shape, Variable-based config override, and a
UI for operational visibility — none of which the legacy Tidal/SSIS stack
exposed in one place. The `@dag`/`@task` TaskFlow decorators
(`05_orchestration/airflow/01_dags/med834_e2e_pipeline.py`) keep each stage
as a typed Python function rather than an opaque operator config.

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

**Why:** The POC uses a token-based, short-lived-credential blob store
(`src/medenroll834/storage/blob_storage.py`) so there is no long-lived
secret to leak — credentials are minted fresh (~10 min TTL) on every call.
The enterprise target substitutes AWS S3 at the exact same interface
(`src/medenroll834/s3/s3_utils.py` already implements the equivalent
`upload_834` / `list_landing_files` / `archive_834` functions), so swapping
storage providers requires no change to Bronze/Silver/Gold, OLTP, OLAP, BI,
or AI code.

## Containerization: Docker / Docker Compose

**Why:** The full Airflow stack (webserver, scheduler, worker, Postgres
metadata DB, Redis broker) is reproducible with one `docker compose up -d`
(`docker-compose.yaml`), with the project bind-mounted at
`/opt/airflow/project` so `src/` and `generator/` import directly into DAG
tasks without a separate package build step.

## CI/CD: GitHub Actions

**Why:** Free for public repos, native GitHub Pages deploy target (used by
this very documentation site — see [Chapter 31](31-ci-cd.md)), and the
existing `.github/workflows/ci.yml` / `cd.yml` already cover lint, test,
security scan, build, and Docker image publish.

## Summary table

| Concern | Technology | Why (grounded in this repo) |
|---|---|---|
| Distributed processing | PySpark 4.0+, Delta Lake 4.0+ | Scales with volume; ACID + schema evolution on Bronze/Silver/Gold |
| Orchestration | Apache Airflow 3.x (TaskFlow, Celery) | Retries, task graph, Variable overrides, UI |
| Compute platform | Databricks (Serverless-compatible) | Managed Spark + Unity Catalog + SQL warehouses + `databricks-sdk` |
| Operational store | PostgreSQL | FK-enforced OLTP schema, kept apart from OLAP |
| OLTP driver | `psycopg2` (raw SQL) | Explicit, auditable upsert statements |
| Object storage (POC) | S3-compatible blob storage | Short-lived, minted credentials (~10 min TTL) |
| Object storage (enterprise) | AWS S3 | Same interface (`s3_utils.py`), swap-in only |
| Containerization | Docker / Docker Compose | One-command reproducible Airflow stack |
| CI/CD | GitHub Actions | Free, native GitHub Pages deploy |
| IaC | Terraform | S3 bucket + policy as code (`04_infrastructure/01_terraform`) |
