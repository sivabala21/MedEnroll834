# 11. PySpark Development Environment

> Source: [`src/medenroll834/common/spark_session.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/common/spark_session.py)

Production processing for this project runs on **Databricks** (see
[Chapter 25](25-spark-performance-optimization.md) for the cluster-side
tuning), not on a local machine. `get_local_spark()` exists solely to give
the test suite (and fast local iteration on parser/Silver/Gold logic) a
real, Delta-enabled Spark session without needing a live Databricks
workspace for every test run.

```python
def get_local_spark(app_name: str = "med834enrl") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Static configs -- must be set before the session is created.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    ...
```

## Delta package resolution

Delta Maven coordinates are **derived**, not hardcoded, to avoid a mismatch
after a Spark version upgrade:

```python
def _delta_packages() -> str:
    import delta
    delta_version = getattr(delta, "__version__", "")
    from pyspark import __version__ as pyspark_version
    spark_series = ".".join(pyspark_version.split(".")[:2])
    scala = os.environ.get("DELTA_SCALA_VERSION", "2.13")
    return f"io.delta:delta-spark_{spark_series}_{scala}:{delta_version}"
```

This only applies when `delta-spark` is pip-installed locally
(`_delta_packages_available()` checks `import delta` first) — on Databricks,
Delta is already provided by the runtime and this branch is skipped.

## Shared by tests and scripts

`get_local_spark()` is the **single source of truth** — reused by
[`02_tests/conftest.py`](https://github.com/yourusername/medenroll834/blob/main/02_tests/conftest.py)'s `spark` fixture,
[`07_scripts/03_utils/run_e2e_smoke.py`](https://github.com/yourusername/medenroll834/blob/main/07_scripts/03_utils/run_e2e_smoke.py),
and any future entry point:

```python
# 02_tests/conftest.py
@pytest.fixture(scope="session")
def spark():
    return get_local_spark("med834enrl-test")
```

## Setting up locally

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
pytest 02_tests/ -v         # exercises get_local_spark() via conftest.py
```

See [Chapter 30](30-testing.md) for the full test suite and
[Chapter 25](25-spark-performance-optimization.md) for the production
(Databricks) side of Spark tuning.

