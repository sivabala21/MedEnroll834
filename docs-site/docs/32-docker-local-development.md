# 32. Docker / Local Development

## Starting the stack

```bash
docker compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
```

`docker-compose.yaml` brings up `postgres` (Airflow metadata), `redis`
(Celery broker), `airflow-webserver`, `airflow-scheduler`,
`airflow-worker`, and a one-shot `airflow-init` — see
[Chapter 20](20-airflow-architecture.md) for the full topology.

Wait ~2 minutes after first start for `_PIP_ADDITIONAL_REQUIREMENTS`
(`databricks-sdk`, `boto3`, `psycopg2-binary`) to install into the
containers, then check:

```bash
docker compose ps
```

## Verifying the stack is healthy

```bash
docker exec medenroll834-airflow-scheduler-1 airflow dags list-import-errors
# -> No data found   (0 import errors)

docker exec medenroll834-airflow-scheduler-1 airflow dags list | Select-String med834
# -> med834_e2e_pipeline | /opt/airflow/dags/med834_e2e_pipeline.py | med834enrl | ...
```

## Dockerfile (standalone package image)

The repo also ships a standalone [`Dockerfile`](https://github.com/yourusername/medenroll834/blob/main/Dockerfile) for
building a distributable image of the package itself (used by the CD
pipeline's `docker` job — see [Chapter 31](31-ci-cd.md)), separate from
the `docker-compose.yaml` Airflow development stack.

## Backing out the local stack

> Source: [`07_scripts/02_deployment/backout_docker_stack.ps1`](https://github.com/yourusername/medenroll834/blob/main/07_scripts/02_deployment/backout_docker_stack.ps1)

```powershell
# Stop and remove containers, keep volumes (metadata/logs preserved)
.\07_scripts\02_deployment\backout_docker_stack.ps1

# Also wipe the airflow-logs and postgres-db-volume named volumes
.\07_scripts\02_deployment\backout_docker_stack.ps1 -Full
```

```powershell
if ($Full) {
    docker compose down -v
} else {
    docker compose down
}
```

Redeploy afterward with `docker compose up -d`.

## Full environment teardown (all deployed components)

> Source: [`08_docs/04_operations_deployment/BACKOUT_RUNBOOK.md`](https://github.com/yourusername/medenroll834/blob/main/08_docs/04_operations_deployment/BACKOUT_RUNBOOK.md)

Every deployed component — Docker stack, Databricks (medallion + OLAP),
OLTP database, and blob storage content — has a matching, idempotent
backout script:

```powershell
.\07_scripts\02_deployment\backout_docker_stack.ps1 -Full
python 07_scripts/02_deployment/backout_databricks.py --profile <profile>
python 07_scripts/02_deployment/backout_oltp_database.py "$env:OLTP_DATABASE_URL"
python 07_scripts/02_deployment/backout_blob_storage.py
```

All backout SQL uses `IF EXISTS` / `DROP ... CASCADE`, so re-running a
backout that already ran is a safe no-op — components are independent and
can be backed out in any order or individually.

## Suggested teardown order (if backing out everything)

1. **Docker stack** — stop the orchestrator first so nothing re-triggers
   jobs while you're tearing down its dependencies.
2. **Databricks** — medallion tables, OLAP star schema, job, notebook.
3. **OLTP database** — CORE/ENRL/UTIL schemas.
4. **Blob storage** — landing/archive/report objects.

## Verifying a backout

```powershell
# Databricks - should error "table/schema not found"
python 07_scripts/03_utils/verify_e2e_data.py

# Docker - no medenroll834-* containers
docker ps -a --filter "name=medenroll834"
```

## Local Python development (without Docker)

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
pytest 02_tests/ -v
```

This path exercises the PySpark/Delta pipeline directly against local
files under `10_data/`, without Airflow or Databricks — useful for fast
iteration on parser/Silver/Gold logic (see
[Chapter 11](11-pyspark-dev-environment.md)).
