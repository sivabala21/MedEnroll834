# MedEnroll834 — Modern Distributed Data Platform for Growing Data

[![CI Pipeline](https://github.com/yourusername/medenroll834/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/medenroll834/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/yourusername/medenroll834/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/medenroll834)
[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A proof of concept demonstrating how distributed processing can prepare a
healthcare data platform for increasing data volume, velocity, and variety.
X12 834 enrollment processing is the representative business module; the
architecture (distributed processing + OLTP/OLAP separation + BI/AI
consumption) is designed to be reusable across other business domains.

See [docs/architecture](08_docs/01_architecture_overview/ARCHITECTURE.md) for
the full architecture narrative.

## 🏗️ Architecture at a Glance

**Capability 1 — Distributed Processing:** X12 834 → Landing → Databricks
PySpark → Bronze → Silver → Gold (medallion architecture).

**Capability 2 — Analytical Platform:** Gold → Delta Lake Star Schema
(`DIM_MEMBER`, `DIM_PLAN`, `DIM_GROUP`, `DIM_DATE`,
`DIM_TRANSACTION_SOURCE`, `FACT_ENROLLMENT`) → Databricks SQL → HTML/BI
reports and Genie AI (two independent consumption paths).

**Operational store:** Gold → PostgreSQL OLTP database → operational
applications — kept separate from the analytical (OLAP) path.

**Orchestration:** Apache Airflow orchestrates ingestion, Databricks
processing, OLTP loading, reporting, and archival (with retries and
`trigger_rule="all_done"` failure handling) — orchestration sits above the
pipeline, not as a data layer itself.

- **Bronze Layer**: Raw EDI file ingestion and segment parsing
- **Silver Layer**: Cleaned and validated member records
- **Gold Layer**: Enriched enrollment data with plan assignments

> **POC vs. Enterprise storage:** this POC uses S3-compatible blob storage +
> Databricks Unity Catalog Volumes for the landing path. The target enterprise
> architecture can substitute AWS S3 at that layer while keeping the same
> downstream Bronze-Silver-Gold, OLTP, OLAP, BI, and AI design.

### Tech Stack

- **Data Processing**: PySpark 4.0+, Delta Lake
- **Orchestration**: Apache Airflow
- **Analytical Platform**: Databricks SQL, Delta Lake Star Schema, Genie AI
- **Operational Platform**: PostgreSQL (OLTP database)
- **Object Storage**: S3-compatible blob storage (POC) / AWS S3 (enterprise target)
- **Containerization**: Docker
- **CI/CD**: GitHub Actions

## ✨ Features

- ✅ X12 834 EDI file parsing (5010 version)
- ✅ Medallion architecture (Bronze/Silver/Gold layers)
- ✅ Data quality validation and quarantine
- ✅ Delta Lake integration for ACID transactions
- ✅ OLTP (PostgreSQL) and OLAP (Delta star schema) kept architecturally separate
- ✅ Two analytical consumption paths: HTML/BI reports and Genie natural-language AI
- ✅ Airflow orchestration with retries and operational failure handling
- ✅ Docker containerization
- ✅ Comprehensive test suite
- ✅ CI/CD pipelines

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Java 11+ (for PySpark)
- Docker & Docker Compose
- Databricks workspace (Free Edition works) + PostgreSQL + S3-compatible blob storage

### Installation

```bash
git clone https://github.com/yourusername/medenroll834.git
cd medenroll834
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### Run the End-to-End Pipeline

```bash
docker compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
# Trigger the med834_e2e_pipeline DAG
```

See [E2E_PIPELINE_RUNBOOK.md](08_docs/04_operations_deployment/E2E_PIPELINE_RUNBOOK.md)
for full setup steps, and
[BACKOUT_RUNBOOK.md](08_docs/04_operations_deployment/BACKOUT_RUNBOOK.md) to
roll back any deployed component.

## 📖 Documentation

- **[Full Engineering Handbook](docs-site/)** — a 38-chapter GitBook-style
  site (MkDocs + Material), covering every layer of the system in depth:
  business problem, legacy vs. modern architecture, the 834 parser,
  Bronze/Silver/Gold, data quality, OLTP/OLAP, Airflow, performance,
  security, testing, CI/CD, and more. Run `mkdocs serve` inside
  `docs-site/` to preview locally, or see it live on GitHub Pages once
  deployed (see [`docs-site/README.md`](docs-site/README.md)).
- [Architecture](08_docs/01_architecture_overview/ARCHITECTURE.md) — the full architecture narrative
- [E2E Pipeline Runbook](08_docs/04_operations_deployment/E2E_PIPELINE_RUNBOOK.md)
- [Backout Runbook](08_docs/04_operations_deployment/BACKOUT_RUNBOOK.md)

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file.

---

Made with ❤️ for healthcare data engineering

