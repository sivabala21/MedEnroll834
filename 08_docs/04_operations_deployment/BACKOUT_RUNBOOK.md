# Backout Runbook (Rollback All Deployed Components)

Simple, standard rollback procedure for every piece of MedEnroll834 that was
deployed: Databricks (medallion + OLAP), OLTP database, blob storage content,
and the local Airflow Docker stack. Each backout mirrors its matching deploy
script and is idempotent (safe to re-run).

## Order

Components are independent, so they can be backed out in any order or
individually. Suggested order if backing out everything:

1. Docker stack (stop the orchestrator first so nothing re-triggers jobs)
2. Databricks (medallion tables, OLAP star schema, job, notebook)
3. OLTP database (CORE/ENRL/UTIL schemas)
4. Blob storage (landing/archive/report objects)

## 1. Docker / Airflow stack

```powershell
# Stop and remove containers, keep volumes (metadata/logs preserved)
.\07_scripts\02_deployment\backout_docker_stack.ps1

# Also wipe the airflow-logs and postgres-db-volume named volumes
.\07_scripts\02_deployment\backout_docker_stack.ps1 -Full
```

Redeploy: `docker compose up -d`

## 2. Databricks (medallion + OLAP + job/notebook)

```powershell
python 07_scripts/02_deployment/backout_databricks.py --profile dbc-645321b4-3ef9
```

What it does:
- Drops `med834enrl.gold.*` (FACT_ENROLLMENT + 5 dimensions) and the
  `bronze` / `silver` / `gold` OLAP schemas
- Drops the 4 `med834enrl.edi.*` Delta tables, the `landing` volume, and the
  `edi` schema
- Deletes the Databricks job (950623271437212) and the
  `/Shared/med834enrl/med834_medallion_job` notebook

Keeps the `med834enrl` catalog itself (so Unity Catalog grants/ownership
aren't disturbed). Pass `--keep-job` to leave the job/notebook in place.

Redeploy:
```powershell
python 07_scripts/02_deployment/deploy_databricks.py --profile dbc-645321b4-3ef9
python 07_scripts/02_deployment/deploy_databricks_olap.py --profile dbc-645321b4-3ef9
# then re-upload the notebook and recreate the job (see E2E_PIPELINE_RUNBOOK.md)
```

## 3. OLTP database

```powershell
python 07_scripts/02_deployment/backout_oltp_database.py "<OLTP_DATABASE_URL>"
```

Runs `04_infrastructure/02_sql/02_oltp/SCHEMA_OLTP_BACKOUT.sql` - drops all
foreign keys, the 8 CORE/ENRL tables, and the CORE/ENRL/UTIL schemas.

Redeploy: `python 07_scripts/02_deployment/deploy_oltp_database.py "<url>"`

## 4. Blob storage content

```powershell
$env:BLOB_STORAGE_TOKEN = "<your token>"
python 07_scripts/02_deployment/backout_blob_storage.py
```

Removes all objects under `landing/834/`, `archive/834/`, and
`audit/reports/`. The bucket itself is managed by the blob storage provider
and is not deleted.

Redeploy: trigger the `med834_e2e_pipeline` DAG; it repopulates these prefixes.

## 5. AWS S3 (legacy, if still provisioned)

```bash
bash 04_infrastructure/03_aws_cloud_shell/aws_s3_backout.sh
```

Removes all folder placeholders and objects from the `med834enrl-s3` bucket
(kept for reference since S3 was set aside in favor of blob storage).

## Full teardown (everything)

```powershell
.\07_scripts\02_deployment\backout_docker_stack.ps1 -Full
python 07_scripts/02_deployment/backout_databricks.py --profile dbc-645321b4-3ef9
python 07_scripts/02_deployment/backout_oltp_database.py "$env:OLTP_DATABASE_URL"
python 07_scripts/02_deployment/backout_blob_storage.py
```

## Verification after backout

```powershell
# Databricks - should error "table/schema not found"
python 07_scripts/03_utils/verify_e2e_data.py

# OLTP database - should error "relation does not exist"
# (connect with psql/psycopg2 and SELECT from core.core_meme_member)

# Docker - no medenroll834-* containers
docker ps -a --filter "name=medenroll834"
```

## Notes

- All backout SQL uses `IF EXISTS` / `DROP ... CASCADE`, so re-running a
  backout that already ran is a safe no-op.
- None of the backout scripts delete source code, deployment scripts, or
  this repository - they only remove the *deployed* objects.
- Secrets (`.env`, Databricks PAT, OLTP database URL, blob storage token) are
  left in place so redeploying afterward does not require re-entering them.

