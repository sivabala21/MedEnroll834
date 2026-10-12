# MedEnroll834 - Modern HIPAA X12 834 Healthcare Enrollment Data Platform

MedEnroll834 demonstrates modernizing legacy HIPAA X12 834 healthcare enrollment processing into a distributed, cloud-ready data platform. It uses a **Medallion Architecture** (Bronze → Silver → Gold) powered by Apache Spark, Delta Lake, PostgreSQL, and Apache Airflow to deliver scalable ingestion, validation, transformation, and analytics.

## What It Does

- **Ingests** X12 834 enrollment files and lands them in a structured, traceable format
- **Validates & standardizes** enrollment data with business rules and data quality checks
- **Transforms** raw segments into clean, conformed member/enrollment records
- **Persists** to OLTP and OLAP models for operational and analytical workloads
- **Orchestrates** the end-to-end pipeline with Apache Airflow for reliability and idempotency
- **Enables** BI and AI consumption via curated Gold layer models

## Key Technologies

- **Processing:** Apache Spark, PySpark, Delta Lake
- **Storage:** AWS S3, Local filesystem
- **Databases:** PostgreSQL (OLTP), Delta/Parquet (OLAP)
- **Orchestration:** Apache Airflow, Docker
- **Data Quality:** Pydantic/validation rules, quarantine handling
- **Testing:** PyTest with PySpark fixtures

## Quick Start

```bash
# Clone and set up
git clone https://github.com/sivabala21/MedEnroll834.git
cd MedEnroll834

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest -v

# Start local stack (Docker)
docker-compose up -d
```

## Architecture Snapshot

The platform follows a medallion pattern: raw 834 EDI files are ingested to Bronze (Delta), cleansed and enriched in Silver, and modeled into a star schema in Gold for analytics. The pipeline includes parser logic, plan assignment rules, reject/quarantine handling, bulk upserts to PostgreSQL, and Airflow DAGs for orchestration.

## Documentation

For the complete, in-depth documentation covering architecture, setup, implementation details, testing, deployment, and runbooks, see the [**Full Documentation Site**](docs-site/docs/index.md).
