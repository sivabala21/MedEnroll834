# 08. PostgreSQL Setup

MedEnroll834 uses PostgreSQL exclusively as the **OLTP operational store**
(see [Chapter 4](04-modern-architecture.md) for why it is kept separate
from the Delta/OLAP analytical layer). In this POC the database is a
managed Neon Postgres instance, but any standard PostgreSQL 13+ works.

## Connection configuration

The connection string lives in a single environment variable,
`OLTP_DATABASE_URL`, consumed by the loader and passed through
`docker-compose.yaml` to the Airflow scheduler/worker:

```bash
# .env
OLTP_DATABASE_URL=postgresql://<user>:<password>@<host>/<db>?sslmode=require
```

`sslmode=require` is mandatory for managed providers like Neon — see the
troubleshooting note in the [E2E runbook](33-end-to-end-execution.md).

## Deploying the schema

> Source: [`07_scripts/02_deployment/deploy_oltp_database.py`](https://github.com/yourusername/medenroll834/blob/main/07_scripts/02_deployment/deploy_oltp_database.py)

The deploy script runs every SQL file under
`04_infrastructure/02_sql/02_oltp/` in dependency order (schemas → parent
tables → child tables → indexes → constraints → comments):

```python
DEPLOY_ORDER = [
    "01_schemas/01_create_core_schema.sql",
    "01_schemas/02_create_enrl_schema.sql",
    "01_schemas/03_create_util_schema.sql",
    "02_tables/CORE_GRGR_GROUP.sql",
    "02_tables/CORE_PLPL_PLAN.sql",
    "02_tables/CORE_MESB_SUBSCRIBER.sql",
    "02_tables/CORE_MEME_MEMBER.sql",
    "02_tables/CORE_SBAD_ADDRESS.sql",
    "02_tables/ENRL_EDXN_TRANSACTION.sql",
    "02_tables/CORE_MEEN_ENROLLMENT.sql",
    "02_tables/ENRL_RJRW_REJECT.sql",
    "03_indexes/INDEXES.sql",
    "04_constraints/CONSTRAINTS.sql",
    "05_views/COMMENTS.sql",
]
```

Run it with:

```bash
python 07_scripts/02_deployment/deploy_oltp_database.py "<OLTP_DATABASE_URL>"
```

The whole deployment runs in a **single transaction** — any statement
failure rolls back everything, so the database never ends up in a
half-migrated state.

Equivalently, via the Makefile (runs against a local `psql` target):

```bash
make setup-db
```

## Rolling back

```bash
python 07_scripts/02_deployment/backout_oltp_database.py "<OLTP_DATABASE_URL>"
```

Runs `04_infrastructure/02_sql/02_oltp/SCHEMA_OLTP_BACKOUT.sql`, which drops
all foreign keys, the 8 CORE/ENRL tables, and the CORE/ENRL/UTIL schemas
with `IF EXISTS` / `CASCADE`, so re-running a backout that already ran is a
safe no-op.

## Verifying a deployment

> Source: [`07_scripts/03_utils/verify_oltp_deploy.py`](https://github.com/yourusername/medenroll834/blob/main/07_scripts/03_utils/verify_oltp_deploy.py)

```python
URL = (
    "postgresql://neondb_owner:***@"
    "ep-late-leaf-b3moj6iv-pooler.c-4.ap-southeast-1.aws.neon.tech/"
    "med834enrl?sslmode=require"
)
```

The verify script connects and confirms the expected tables/row counts
exist after a deploy or an end-to-end pipeline run.

## Schemas provisioned

| Schema | Purpose |
|---|---|
| `CORE` | Group, plan, subscriber, member, address, enrollment — the operational entity model |
| `ENRL` | Transaction control and rejected-record tracking |
| `UTIL` | Shared utility objects (sequences, lookups) |

See [Chapter 9](09-database-schema.md) for the full table-by-table DDL.
