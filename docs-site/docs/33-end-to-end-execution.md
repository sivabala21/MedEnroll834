# 33. End-to-End Execution

> Source: [`08_docs/04_operations_deployment/E2E_PIPELINE_RUNBOOK.md`](https://github.com/yourusername/medenroll834/blob/main/08_docs/04_operations_deployment/E2E_PIPELINE_RUNBOOK.md)

This is the complete, real runbook for running `med834_e2e_pipeline`
against live Databricks/OLTP/blob storage infrastructure — it implements
both the **Distributed Processing** and **Analytical Platform** capabilities
from [Chapter 4](04-modern-architecture.md).

## Flow

```text
Generate 834 -> blob storage landing -> Databricks UC Volume
  -> Databricks medallion (bronze/silver/gold) -> OLAP star schema
  -> OLTP database load -> HTML report + Genie (analytical consumers)
  -> archive in blob storage
```

## Already-provisioned prerequisites

- Databricks catalog `med834enrl` with `edi` Delta tables + `gold` OLAP
  star schema (`DIM_MEMBER`, `DIM_PLAN`, `DIM_GROUP`, `DIM_DATE`,
  `DIM_TRANSACTION_SOURCE`, `FACT_ENROLLMENT`)
- Databricks notebook: `/Shared/med834enrl/med834_medallion_job`
- Databricks Job id and a SQL warehouse id
- OLTP database schema (CORE/ENRL) deployed (see [Chapter 8](08-postgresql-setup.md))
- Blob storage bucket (token-based, mints short-lived S3-compatible creds)
- Airflow DAG `med834_e2e_pipeline` (validated, 0 import errors)

## Step 1 — Fill in secrets in `.env`

```bash
BLOB_STORAGE_TOKEN=...      # POC landing/archive/report object store
DATABRICKS_TOKEN=...        # Personal Access Token (containers can't use browser OAuth)
OLTP_DATABASE_URL=...       # already set to the OLTP med834enrl database
GENIE_SPACE_ID=             # optional
```

Generate a Databricks PAT via CLI:

```powershell
$db = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
& $db tokens create --comment airflow --lifetime-seconds 7776000
```

## Step 2 — Recreate the Airflow stack

```powershell
docker compose down
docker compose up -d
```

See [Chapter 32](32-docker-local-development.md) for what this stack
contains and how to verify it's healthy.

## Step 3 — Open Airflow and run

- UI: `http://localhost:8080` (admin/admin)
- Unpause and trigger DAG **`med834_e2e_pipeline`**
- Watch the task graph: `generate → upload_to_blob → push_to_uc_volume →
  databricks → oltp → report → genie → archive` (see
  [Chapter 21](21-airflow-dag-implementation.md) for what each task does)

## Step 4 — Verify outputs

| Store | What to check |
|---|---|
| Blob storage | `archive/834/` (processed files), `audit/reports/enrollment_report_*.html` |
| Databricks (OLAP) | `med834enrl.edi.gold_enrollments`, `med834enrl.gold.dim_*`, `med834enrl.gold.fact_enrollment` |
| OLTP database | `core.core_meme_member`, `core.core_meen_enrollment`, `enrl.enrl_edxn_transaction` |

## Re-deploying the Databricks notebook after a code change

```powershell
$db = "...\databricks.exe"
& $db workspace import --language PYTHON --format SOURCE --overwrite `
  --file 05_orchestration/databricks/med834_medallion_job.py `
  /Shared/med834enrl/med834_medallion_job
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Blob storage 401/expired | `BLOB_STORAGE_TOKEN` credentials expire ~10 min; re-minted automatically per call — a persistent 401 means the token itself is invalid/revoked |
| Databricks 403 | PAT expired/invalid — regenerate and update `.env` |
| OLTP database connection failure | ensure `?sslmode=require` is in `OLTP_DATABASE_URL` |
| DAG not visible in UI | `docker compose logs airflow-scheduler \| Select-String error` |
| pip deps missing in container | wait for `_PIP_ADDITIONAL_REQUIREMENTS` startup install, or `docker compose restart` |

## Enterprise migration note

To move from the POC landing layer to the enterprise target, replace the
`upload_to_blob` / `archive_blob` tasks with the already-implemented AWS S3
equivalents in
[`src/medenroll834/s3/s3_utils.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/s3/s3_utils.py),
and update the Databricks job's landing path accordingly. The
Bronze-Silver-Gold, OLTP, OLAP, BI, and AI layers require **no changes** —
see [Chapter 7](07-aws-s3-setup.md).

## Local (non-Airflow) smoke test

For fast iteration without the full Airflow stack:

```bash
python -c "import sys; sys.path.insert(0,'.'); from generator.synthetic_834_generator import generate_batch; generate_batch('10_data/raw', num_files=3, subscribers_per_file=10)"
python 07_scripts/03_utils/run_e2e_smoke.py
```

This exercises `DeltaMedallionPipeline` directly against local Delta
tables — the same validated baseline referenced throughout this documentation:

```text
BRONZE     : segments_loaded = 83
SILVER     : total = 6, valid = 6, invalid = 0
GOLD       : records = 6
QUARANTINE : records = 0
```
