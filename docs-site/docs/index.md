# MedEnroll834 — Engineering Handbook

**A Modern Distributed Data Platform for Growing Data**, demonstrated through
HIPAA X12 834 healthcare enrollment processing.

This handbook documents the full engineering lifecycle of MedEnroll834: the
business problem, the legacy system it modernizes, the target architecture,
every pipeline layer (Bronze → Silver → Gold), orchestration, data quality,
analytics, security, testing, CI/CD, and the operational runbooks used to
deploy and back out every component.

!!! info "Scope"
    X12 834 enrollment processing is the **representative business module**.
    The architecture — distributed processing + OLTP/OLAP separation +
    BI/AI consumption — is designed to be reusable across other
    high-volume healthcare (or non-healthcare) data domains.

## How this handbook is organized

<div class="grid cards" markdown>

- :material-file-document-outline: **Overview & Business Context**

    Chapters 1–6 — why this project exists, the legacy pain points it
    solves, the target architecture, technology choices, and how the
    repository is laid out.

- :material-cloud-outline: **Foundations & Setup**

    Chapters 7–11 — AWS/S3, PostgreSQL, the OLTP/OLAP schemas, synthetic
    834 data generation, and the local PySpark + Delta development
    environment.

- :material-source-branch: **Pipeline Engineering**

    Chapters 12–19 — the 834 parser and the Bronze/Silver/Gold medallion
    layers, plan-assignment business rules, data quality & quarantine,
    the PostgreSQL bulk upsert, and the OLAP star schema.

- :material-sitemap: **Orchestration & Reliability**

    Chapters 20–24 — Apache Airflow architecture and DAG implementation,
    idempotency, retries, and observability.

- :material-speedometer: **Performance & Analytics**

    Chapters 25–28 — Spark tuning decisions actually applied in this repo,
    the benchmark framework, the Databricks SQL analytics layer, and the
    Genie natural-language AI consumer.

- :material-shield-check: **Quality, Security & Delivery**

    Chapters 29–33 — security posture, the pytest/PySpark test suite,
    GitHub Actions CI/CD, Docker/Airflow local development, and running
    the full pipeline end to end.

- :material-trophy-outline: **Impact & Portfolio**

    Chapters 34–38 — legacy vs. modern comparison, business impact,
    interview talking points, the portfolio README, and the roadmap.

</div>

## Quick links

- [Modern Architecture](04-modern-architecture.md) — the target platform diagram
- [End-to-End Execution](33-end-to-end-execution.md) — run the whole pipeline
- [Airflow DAG Implementation](21-airflow-dag-implementation.md) — `med834_e2e_pipeline`
- [OLAP Star Schema](19-olap-star-schema.md) — `FACT_ENROLLMENT` + 5 dimensions
- [Portfolio README](37-portfolio-readme.md) — the elevator-pitch version of this project

## Source of truth

This handbook is generated from, and kept consistent with, the actual
source code under `src/medenroll834/`, the Airflow DAG under
`05_orchestration/airflow/01_dags/`, the Databricks notebook under
`05_orchestration/databricks/`, the SQL DDL under `04_infrastructure/02_sql/`,
and the operational runbooks under `08_docs/04_operations_deployment/`. Where
a code snippet is shown, it is copied from the real file, not reconstructed
from memory.
