# 11. PySpark Development Environment

Local PySpark + Delta Lake on Windows has three specific failure modes that
the rest of a typical PySpark tutorial glosses over. MedEnroll834 solves
all three in one place:
[`src/medenroll834/common/spark_session.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/common/spark_session.py).

## The three problems

1. **Delta Lake isn't enabled by default.** `.format("delta")` fails with
   `DATA_SOURCE_NOT_FOUND` unless `spark.sql.extensions` and
   `spark.sql.catalog.spark_catalog` are set — and these are **static
   configs**, meaning Spark refuses to change them after the session
   already exists (`CANNOT_MODIFY_STATIC_CONFIG`). They must be supplied on
   the builder, before `getOrCreate()`.
2. **`JAVA_HOME` isn't set.** PySpark starts a JVM via `py4j`; if `java`
   isn't resolvable, the gateway dies with `JAVA_GATEWAY_EXITED` before any
   Python code runs.
3. **`PYSPARK_PYTHON` defaults to `python3`.** Spark launches one Python
   worker process per executor and defaults to the literal command
   `python3`, which doesn't exist on a stock Windows install.
4. **`HADOOP_HOME` is unset.** Delta shells out to Hadoop's `winutils.exe`
   on Windows for filesystem operations; without it, session creation fails
   with "HADOOP_HOME and hadoop.home.dir are unset."

## The fix, in order

```python
def get_local_spark(app_name: str = "med834enrl") -> SparkSession:
    _ensure_java_home()
    _ensure_hadoop_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.windowExec.buffer.spill.threshold", "4096")
        .config("spark.sql.session.timeZone", "UTC")
        # Static configs -- must be set before the session is created.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    ...
```

`_ensure_java_home()` probes known JDK install locations
(`C:\Program Files\Java\jdk-21`, `jdk-17`, Eclipse Adoptium) and populates
`JAVA_HOME` + `PATH` if not already set. `_ensure_hadoop_home()` points at a
**vendored** `winutils.exe` under `12_tools/hadoop/bin` so a clean checkout
runs tests without a separate Hadoop install.

## Why `spark.driver.memory` is raised to 4g

```text
# Default JVM heap (1g) is not enough for the window functions in the
# Silver address/hierarchy steps on a multi-core local box: 16 slots
# each sort a partition of the shuffle and the driver OOMs. Raise the
# ceiling and give the driver a real off-heap/spill budget.
```

This is a real, previously-hit OOM — the Silver layer's HL-hierarchy
resolution (see [Chapter 14](14-silver-layer.md)) uses several `Window`
operations, and on a local multi-core machine with the default 1 GB driver
heap, this reliably fails. `spark.sql.windowExec.buffer.spill.threshold` is
also raised to give those windows a bigger in-memory buffer before
spilling to disk.

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
and any future entry point — so Java/Python-worker/Delta configuration is
never re-implemented (or mis-configured) twice.

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
