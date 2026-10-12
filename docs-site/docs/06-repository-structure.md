# 06. Project Repository Structure

> Source: [`PROJECT_STRUCTURE.md`](https://github.com/yourusername/medenroll834/blob/main/PROJECT_STRUCTURE.md) and
> [`REORGANIZATION_SUMMARY.md`](https://github.com/yourusername/medenroll834/blob/main/REORGANIZATION_SUMMARY.md).

The repository follows a numbered, enterprise-style top-level layout so
directory order communicates pipeline/delivery order at a glance.

```text
MedEnroll834/
├── 02_tests/                  # pytest + PySpark test suite (Ch. 30)
├── 03_config/                 # pipeline_config.yaml (Delta paths, validation rules)
├── 04_infrastructure/         # Terraform (S3) + SQL DDL (OLTP + OLAP)
├── 05_orchestration/          # Airflow DAGs + Databricks notebook/job
├── 06_deploy/                 # Deployment configs
├── 07_scripts/                # Deployment, backout, and utility scripts
├── 08_docs/                   # Architecture docs, diagrams, runbooks (and this handbook's source)
├── 09_monitoring/             # Observability placeholders
├── 10_data/                   # Local Delta table storage (bronze/silver/gold/quarantine)
├── 11_build/                  # Build artifacts
├── 12_tools/                  # Vendored dev tools (bundled winutils.exe for local Delta on Windows)
├── docs-site/                 # This GitBook-style documentation site (MkDocs)
├── generator/                 # synthetic_834_generator.py
├── src/
│   └── medenroll834/           # the importable Python package (see below)
├── docker-compose.yaml        # Airflow 3.x stack (Celery executor)
├── Dockerfile
├── Makefile                   # install / test / lint / format / docker / setup-db targets
├── pyproject.toml             # package metadata, console scripts, tool configs
└── requirements.txt
```

!!! note "Why `src/` isn't numbered"
    Python does not allow import paths that start with a digit, so the
    importable package could not live under a `01_src/` folder. Every
    *other* top-level directory keeps the numbered convention; `src/` is
    the one documented exception.

## The `src/medenroll834/` package

```text
src/medenroll834/
├── common/
│   ├── config.py          # load_config(), resolve_delta_path(), get_landing_path()
│   ├── logging.py         # setup_logging(), log_pipeline_start/end()
│   └── spark_session.py   # get_local_spark() — Delta + Windows bootstrap
├── parsers/
│   └── edi834_parser.py   # EDI834Parser — X12 834 → Bronze DataFrame
├── ingestion/
│   ├── bronze.py          # BronzeLayer — split_segments(), raw Delta write
│   ├── silver.py          # SilverLayer — HL hierarchy resolution, validation
│   ├── gold.py            # GoldLayer — dedup, full_name, age_years
│   └── pipeline.py        # DeltaMedallionPipeline — Bronze→Silver→Gold orchestrator
├── quality/
│   ├── validator.py       # DataFrameValidator — Spark-SQL validation rules
│   └── quarantine.py      # DeltaQuarantineManager — invalid-record Delta sink
├── oltp/
│   └── oltp_loader.py     # load_gold_to_oltp() — Databricks SQL → psycopg2 upsert
├── reporting/
│   └── report_generator.py # generate_report(), ask_genie()
├── storage/
│   └── blob_storage.py    # POC S3-compatible blob storage client
└── s3/
    └── s3_utils.py         # Enterprise-target AWS S3 client (same interface)
```

## Key configuration files

| File | Purpose |
|---|---|
| [`03_config/pipeline_config.yaml`](https://github.com/yourusername/medenroll834/blob/main/03_config/pipeline_config.yaml) | Delta table paths/options per layer, Unity Catalog settings, validation plan codes |
| [`pyproject.toml`](https://github.com/yourusername/medenroll834/blob/main/pyproject.toml) | Package metadata, `medenroll834-generate` console script, ruff/black/mypy/pytest config |
| [`docker-compose.yaml`](https://github.com/yourusername/medenroll834/blob/main/docker-compose.yaml) | Full local Airflow 3.x stack definition |
| [`.env.example`](https://github.com/yourusername/medenroll834/blob/main/.env.example) | Template for all pipeline secrets (copy to `.env`, git-ignored) |
| [`Makefile`](https://github.com/yourusername/medenroll834/blob/main/Makefile) | `install`, `test`, `lint`, `format`, `docker-up/down`, `setup-db`, `setup-databricks` |

## Numbered directory rationale

1. **Clarity** — numbers indicate order and priority.
2. **Sorting** — logical directory ordering in any file browser or `ls`.
3. **Standards** — mirrors enterprise delivery-pipeline conventions.
4. **Professional presentation** — reads as industry-standard organization
   to a reviewer or interviewer skimming the repo.
