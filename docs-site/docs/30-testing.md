# 30. Testing

> Source: [`02_tests/conftest.py`](https://github.com/yourusername/medenroll834/blob/main/02_tests/conftest.py),
> [`02_tests/test_pipeline.py`](https://github.com/yourusername/medenroll834/blob/main/02_tests/test_pipeline.py)

MedEnroll834 tests against real PySpark + Delta and real synthetic 834
files (via `generate_batch()`, see [Chapter 10](10-synthetic-data-generation.md))
— every test exercises the actual transformation logic end to end against
generated data rather than mocked DataFrames.

## Test coverage by layer

| Test class | What it verifies |
|---|---|
| `TestEDI834Parser` | `parse_files()` returns a non-empty Bronze DataFrame with `segment_id`/`elements` columns |
| `TestSilverLayer` | Silver produces `member_id`/`is_valid` columns; synthetic (non-`--messy`) data is always valid |
| `TestDataFrameValidator` | Missing `member_id` and unknown `plan_id` are each correctly flagged invalid |
| `TestGoldLayer` | Gold deduplicates and derives `full_name`/`age_years` |

## Running the suite

```bash
pytest 02_tests/ -v --cov=src --cov-report=term-missing
# or
make test
```

## DAG validation (outside pytest)

```python
# 07_scripts/03_utils/validate_dag.py
from airflow.models.dagbag import DagBag
db = DagBag("/opt/airflow/dags")
# writes IMPORT_ERRORS and DAGS to /tmp/dagresult.txt
```

Run inside the live Airflow scheduler container to catch DAG-definition
syntax/import errors before relying on the UI — this is how the "0 import
errors" claim in [Chapter 21](21-airflow-dag-implementation.md) is
verified.

## What is and isn't covered

| Covered | Not covered (yet) |
|---|---|
| Parser, Silver validation/transform, Gold dedup/derive | OLTP loader (`oltp_loader.py`) — requires a live Postgres + Databricks warehouse |
| DataFrameValidator rules | Report generator / Genie (`report_generator.py`) — requires a live Databricks workspace |
| DAG import/parse correctness | End-to-end Airflow task execution (covered manually via the runbook, not CI) |

See [Chapter 37](37-future-improvements.md) for extending coverage to the
OLTP/reporting modules with a mocked `databricks-sdk`/`psycopg2`.

