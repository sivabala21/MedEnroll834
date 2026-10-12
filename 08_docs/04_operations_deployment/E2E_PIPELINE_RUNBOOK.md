# End-to-End Pipeline Runbook (Airflow + Blob Storage + Databricks + OLTP DB + Genie)

Implements the **Distributed Processing** and **Analytical Platform**
capabilities described in [ARCHITECTURE.md](../01_architecture_overview/ARCHITECTURE.md).
This is the POC implementation detail; see that document for the
conceptual/enterprise architecture narrative (and why S3 is the target
enterprise storage while this POC uses S3-compatible blob storage).

Orchestrates: **Generate 834 -> blob storage landing -> Databricks UC Volume
-> Databricks medallion (bronze/silver/gold) -> OLAP star schema -> OLTP
database load -> HTML report + Genie (analytical consumers) -> archive in
blob storage.**

## Architecture

```
Airflow (Docker, CeleryExecutor)                 -- orchestration only
  |-- generate_834         local synthetic 834 files
  |-- upload_to_blob        -> blob storage landing/834/
  |-- push_to_uc_volume     -> Databricks UC Volume /Volumes/med834enrl/edi/landing/834
  |                            (serverless Databricks cannot read external buckets)
  |-- run_databricks_job    -> Databricks Job 950623271437212 (notebook)
  |                            Bronze -> Silver -> Gold (med834enrl.edi)
  |                            -> OLAP star schema (med834enrl.gold.*)
  |-- load_oltp_database    reads gold via SQL warehouse -> OLTP database (CORE/ENRL)
  |-- generate_report       analytics SQL on gold -> HTML -> blob storage audit/reports/
  |-- ask_genie             (optional) NL questions if GENIE_SPACE_ID set
  |-- archive_blob          landing/834/ -> archive/834/ (trigger_rule=all_done)
```

Note: `generate_report` and `ask_genie` are consumers of the governed
analytical (OLAP) layer, not transformation steps — see ARCHITECTURE.md
section 4.

## Already provisioned
- Databricks catalog `med834enrl` with `edi` Delta tables + `gold` OLAP star schema
  (DIM_MEMBER, DIM_PLAN, DIM_GROUP, DIM_DATE, DIM_TRANSACTION_SOURCE, FACT_ENROLLMENT)
- Databricks notebook: `/Shared/med834enrl/med834_medallion_job`
- Databricks Job: **id 950623271437212** (name: med834enrl-medallion-pipeline)
- Databricks SQL warehouse: **f2ae7eb6d25a016a** (Serverless Starter)
- OLTP database schema (CORE/ENRL) deployed
- Blob storage bucket (token-based, mints short-lived S3-compatible creds)
- Airflow DAG: `med834_e2e_pipeline` (validated, 0 import errors)

## Step 1 - Fill in secrets in `.env`
Edit `.env` and set:
- `BLOB_STORAGE_TOKEN` - the POC landing/archive/report object store
- `DATABRICKS_TOKEN` - a Databricks Personal Access Token (User Settings > Developer > Access tokens)
  - containers cannot use browser OAuth, so a PAT is required
- `OLTP_DATABASE_URL` - already set to the OLTP med834enrl database
- `GENIE_SPACE_ID` - optional; set if your workspace has a Genie space
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - only needed if you switch the
  landing layer to AWS S3 for the enterprise target architecture; not used
  by the current blob-storage-based DAG


Generate a Databricks PAT via CLI (local):
```powershell
$db = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
& $db tokens create --comment airflow --lifetime-seconds 7776000

## Step 2 - Recreate the Airflow stack with project mount + deps
```powershell
cd C:\Users\sivab\Workspace\MedEnroll834
docker compose down
docker compose up -d
```
The `docker-compose.yaml`:
- mounts the whole project at `/opt/airflow/project` (so `src/` + `generator/` import)
- installs `databricks-sdk`, `boto3`, `psycopg2-binary`, `requests` on startup
- passes all `.env` vars (blob storage, Databricks, OLTP DB) to scheduler + worker

Wait ~2 min for `_PIP_ADDITIONAL_REQUIREMENTS` to install, then check:
```powershell
docker compose ps
```

## Step 3 - Open Airflow and run
- UI: http://localhost:8080  (admin / admin)
- Unpause and trigger DAG **med834_e2e_pipeline**
- Watch the task graph: generate -> upload_to_blob -> push_to_uc_volume ->
  databricks -> oltp -> report -> genie -> archive

## Step 4 - Verify outputs
- **Blob storage**: `archive/834/` (processed files),
  `audit/reports/enrollment_report_*.html`
- **Databricks (OLAP)**: `med834enrl.edi.gold_enrollments`, `med834enrl.gold.dim_*`,
  `med834enrl.gold.fact_enrollment`
- **OLTP database**: `core.core_meme_member`, `core.core_meen_enrollment`, `enrl.enrl_edxn_transaction`

## Config reference (Airflow Variables override env if set)
| Key | Value |
|-----|-------|
| BLOB_STORAGE_TOKEN | (set in `.env`, git-ignored) |
| OLTP_DATABASE_URL | postgresql://...neon.tech/med834enrl |
| DATABRICKS_HOST | https://dbc-645321b4-3ef9.cloud.databricks.com |
| DATABRICKS_JOB_ID | 950623271437212 |
| DATABRICKS_WAREHOUSE_ID | f2ae7eb6d25a016a |
| GENIE_SPACE_ID | (optional) |

## Re-deploy the Databricks notebook/job after changes
```powershell
$db = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
& $db workspace import --language PYTHON --format SOURCE --overwrite `
  --file 05_orchestration/databricks/med834_medallion_job.py `
  /Shared/med834enrl/med834_medallion_job
```

## Enabling Genie (optional, analytical consumer)
1. In Databricks: Genie > New space, add `med834enrl.edi.gold_enrollments` and
   `med834enrl.gold.*` tables as the space data.
2. Copy the space id from the URL and set `GENIE_SPACE_ID` in `.env`.
3. Re-run the DAG; the `ask_genie` task queries the same governed Gold/OLAP
   data as the HTML report — it is a second consumption mode, not a
   pipeline transformation step.

## Troubleshooting
- **Blob storage 401/expired**: credentials from `BLOB_STORAGE_TOKEN` are
  minted fresh per call and expire in ~10 min; re-minting happens
  automatically on each task, so this indicates an invalid/revoked token.
- **Databricks 403**: PAT expired/invalid; regenerate and update `.env`.
- **OLTP database connection**: ensure `?sslmode=require` is in the URL.
- **DAG not visible**: `docker compose logs airflow-scheduler | Select-String error`.
- **pip deps missing**: wait for startup install, or `docker compose restart`.

## Enterprise migration note
To move from the POC landing layer to the enterprise target, replace the
`upload_to_blob` / `archive_blob` tasks with AWS S3 equivalents (the
`src/medenroll834/s3/s3_utils.py` module already implements this interface)
and update the Databricks job's `landing_path` accordingly. The
Bronze-Silver-Gold, OLTP, OLAP, BI, and AI layers require no changes.

```
