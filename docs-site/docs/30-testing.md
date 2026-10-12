# 30. Testing

> Source: [`02_tests/conftest.py`](https://github.com/yourusername/medenroll834/blob/main/02_tests/conftest.py),
> [`02_tests/test_pipeline.py`](https://github.com/yourusername/medenroll834/blob/main/02_tests/test_pipeline.py)

MedEnroll834 tests against **real PySpark + Delta** (via
`get_local_spark()`, see [Chapter 11](11-pyspark-dev-environment.md)) and
**real synthetic 834 files** (via `generate_batch()`, see
[Chapter 10](10-synthetic-data-generation.md)) — there are no mocked Spark
DataFrames; every test exercises the actual transformation logic end to
end against generated data.

## Shared fixtures

```python
# conftest.py
@pytest.fixture(scope="session")
def spark():
    """Delta-enabled local SparkSession."""
    return get_local_spark("med834enrl-test")

@pytest.fixture(scope="session")
def delta_spark(spark):
    """Alias fixture making the Delta requirement explicit at the call site."""
    return spark
```

`scope="session"` means the (expensive) SparkSession is created once per
test run, not once per test — standard practice to keep a PySpark test
suite's wall-clock time reasonable.

```python
# test_pipeline.py
@pytest.fixture(scope="function")
def sample_834_files(tmp_path):
    files = generate_batch(
        output_dir=str(tmp_path), num_files=2, subscribers_per_file=5,
        dependents_per_sub=1, start_date=date(2026, 9, 1),
    )
    return tmp_path, files
```

`scope="function"` here (vs. the session-scoped `spark` fixture) means
every test gets a **fresh set of generated files** in pytest's isolated
`tmp_path`, so tests cannot interfere with each other's data.

## Test coverage by layer

| Test class | What it verifies |
|---|---|
| `TestEDI834Parser` | `parse_files()` returns a non-empty Bronze DataFrame with `segment_id`/`elements` columns |
| `TestSilverLayer` | Silver produces `member_id`/`is_valid` columns; synthetic (non-`--messy`) data is always valid |
| `TestDataFrameValidator` | Missing `member_id` and unknown `plan_id` are each correctly flagged invalid |
| `TestGoldLayer` | Gold deduplicates and derives `full_name`/`age_years` |

## Example: validator unit test

```python
class TestDataFrameValidator:
    def test_validates_missing_member_id(self, spark):
        data = [("", "JOHN", "SMITH", "", "", "", "GOLD")]
        df = spark.createDataFrame(data, ["member_id", "first_name", "last_name",
            "date_of_birth", "coverage_start_date", "coverage_end_date", "plan_id"])
        result = DataFrameValidator().validate(df)
        invalid = result.filter(F.col("is_valid") == False).count()
        assert invalid == 1

    def test_validates_plan_code(self, spark):
        data = [("M1", "JOHN", "SMITH", "1990-01-01", "2020-01-01", "2030-01-01", "UNKNOWN")]
        ...
        assert invalid == 1
```

Each rule from [Chapter 16](16-data-quality-reject-handling.md) has a
corresponding targeted test constructing a DataFrame literal that should
trip exactly that rule.

## Example: full-layer integration test

```python
class TestGoldLayer:
    def test_gold_deduplicates(self, spark, sample_834_files):
        tmp_path, files = sample_834_files
        parser = EDI834Parser(spark)
        bronze_df = parser.parse_files(str(tmp_path))
        config = load_config()
        config["layers"]["silver"]["path"] = str(tmp_path / "silver")
        config["layers"]["gold"]["path"] = str(tmp_path / "gold")
        silver_df = SilverLayer(spark, config).transform(bronze_df)
        gold_df = GoldLayer(spark, config).transform(silver_df)
        assert gold_df.count() > 0
        assert "full_name" in gold_df.columns
        assert "age_years" in gold_df.columns
```

This test runs the **actual** `EDI834Parser` → `SilverLayer` →
`GoldLayer` chain against generated files, redirecting output paths into
pytest's `tmp_path` so test runs never touch `10_data/` on disk. It is the
closest thing in the repo to a true integration test of the medallion
pipeline, short of running through `DeltaMedallionPipeline` itself.

## Running the suite

```bash
pytest 02_tests/ -v --cov=src --cov-report=term-missing
# or
make test
```

`pyproject.toml` pins the test configuration:

```toml
[tool.pytest.ini_options]
testpaths = ["02_tests"]
pythonpath = ["."]
addopts = "-q"
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

See [Chapter 38](38-future-improvements.md) for extending coverage to the
OLTP/reporting modules with a mocked `databricks-sdk`/`psycopg2`.
