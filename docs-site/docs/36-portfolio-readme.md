# 36. Portfolio README

A condensed, portfolio/resume-ready version of this project's README — use
this chapter as the elevator-pitch text for a GitHub profile, LinkedIn
project entry, or portfolio site.

## MedEnroll834 — Modern Distributed Data Platform for Growing Data

A proof-of-concept healthcare data platform that demonstrates how
distributed processing modernizes HIPAA X12 834 enrollment transaction
processing — built to show the same architectural pattern (distributed
processing + OLTP/OLAP separation + BI/AI consumption) generalizes beyond
this one business module.

### Highlights

- **Distributed medallion pipeline** (Bronze → Silver → Gold) in
  PySpark 4.0+ / Delta Lake, parsing real X12 834 (5010) EDI structure
  including the `HL` hierarchy, segment extraction, and date normalization.
- **Dual persistence**: PostgreSQL OLTP for operational reads/writes, a
  Delta Lake star schema (`FACT_ENROLLMENT` + 5 dimensions) for analytics —
  deliberately kept architecturally separate.
- **Dual analytics consumption**: a traditional SQL/HTML reporting path and
  a Databricks Genie natural-language AI path, both querying the same
  governed Gold/OLAP data.
- **Apache Airflow 3.x** orchestration (TaskFlow API, CeleryExecutor) with
  retries, run-scoped archive-on-success / quarantine-on-failure handling,
  and Airflow Variable config overrides.
- **Data quality by design**: Spark-SQL validation rules producing
  multi-reason error arrays, with invalid records quarantined (not
  dropped) to a dedicated Delta table.
- **Idempotent throughout**: Delta overwrite semantics, PostgreSQL
  natural-key upserts, deterministic hashed surrogate keys.
- **Infrastructure as Code**: Terraform-managed S3 bucket + least-privilege
  IAM policy, versioned SQL DDL for both OLTP and OLAP schemas, full
  Docker Compose local dev stack.
- **CI/CD**: GitHub Actions — lint (ruff/mypy/black), test (pytest matrix,
  Codecov), security scan (bandit/safety), build/Docker publish on tag.
- **Zero production data**: all 834 files are synthetically generated with
  a custom X12-compliant generator (realistic name/address/plan pools,
  valid ISA checksums).

### Tech stack

`PySpark` `Delta Lake` `Apache Airflow` `Databricks SQL` `Databricks Genie`
`PostgreSQL` `Docker` `Terraform` `GitHub Actions` `Python 3.10+`

### What this demonstrates

| Skill area | Evidence |
|---|---|
| Distributed data engineering | Medallion architecture, window-function-based hierarchy resolution, broadcast-map optimization over a fan-out join |
| Data modeling | Normalized OLTP schema + dimensional OLAP star schema, both from first principles |
| Orchestration | TaskFlow DAG with explicit retry/failure semantics |
| Data quality engineering | Multi-rule validation with structured, actionable rejection reasons |
| Cloud/infra | Terraform IaC, least-privilege IAM, Docker Compose |
| AI integration | Genie natural-language analytics as a governed consumer of curated data |
| Software engineering practice | CI/CD, typed config, structured logging, idempotent design, documented runbooks |

### Links

- [Full documentation](index.md) (this site) — chapters covering every
  layer of the system in depth, generated from and kept consistent with
  the actual source code.
- [Modern Architecture](04-modern-architecture.md)
- [End-to-End Execution Runbook](33-end-to-end-execution.md)

### Author

**Sivabala Selvam**

- GitHub: [github.com/sivabala21](https://github.com/sivabala21)
- LinkedIn: [linkedin.com/in/sivabala-s](https://www.linkedin.com/in/sivabala-s/)

---

!!! tip "Using this chapter"
    Copy the sections above directly into a project's top-level
    `README.md`, a portfolio site project card, or a resume project bullet
    list — they're written to stand alone outside this documentation's context.


