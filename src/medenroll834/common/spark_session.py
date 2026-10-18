"""
src/utils/spark_session.py
Single source of truth for building a local SparkSession.

Local runs need three things that Databricks provides automatically:

1. Delta Lake -- enabled via the ``spark.sql.extensions`` and ``spark.sql.catalog``
   configuration keys. Without these, ``.format("delta")`` fails with
   DATA_SOURCE_NOT_FOUND and the layers silently fall back to parquet.
2. JAVA_HOME -- PySpark starts a JVM. If ``java`` is not on PATH the gateway
   dies with JAVA_GATEWAY_EXITED before any work happens.
3. PYSPARK_PYTHON -- Spark launches a Python worker per executor and defaults
   to the literal command ``python3``, which does not exist on Windows.
4. HADOOP_HOME -- Delta shells out to Hadoop's ``winutils.exe`` on Windows.
   Without it the session fails with "HADOOP_HOME and hadoop.home.dir are
   unset". The binaries are vendored under ``tools/hadoop/bin`` so a clean
   checkout can run the tests without a separate Hadoop install.

Centralising this keeps ``conftest.py``, scripts and future entry points from
re-implementing (and mis-configuring) the same bootstrap.
"""
import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
import logging
_LOGGER = logging.getLogger(__name__)


# Common JDK install locations checked when JAVA_HOME is not already set.
_JAVA_CANDIDATES = [
    Path(r"C:\Program Files\Java\jdk-21"),
    Path(r"C:\Program Files\Java\jdk-17"),
    Path(r"C:\Program Files\Eclipse Adoptium"),
]

def _repo_root() -> Path:
    """Return the repository root (src/medenroll834/common -> repo root)."""
    return Path(__file__).resolve().parents[3]


def _ensure_hadoop_home() -> Optional[str]:
    """Populate HADOOP_HOME when a bundled winutils.exe is present (Windows)."""
    current = os.environ.get("HADOOP_HOME")
    if current and (Path(current) / "bin" / "winutils.exe").exists():
        return current
    bundled = _repo_root() / "12_tools" / "hadoop"
    if (bundled / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(bundled)
        os.environ["hadoop.home.dir"] = str(bundled)
        os.environ["PATH"] = f"{bundled / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
        return str(bundled)
    return current

def _ensure_java_home() -> Optional[str]:
    """Populate JAVA_HOME from a known JDK path when it is unset."""
    current = os.environ.get("JAVA_HOME")
    if current and Path(current).exists():
        return current

    for candidate in _JAVA_CANDIDATES:
        if candidate.exists():
            # Adoptium-style installs nest the real JDK one level down.
            if (candidate / "bin" / "java.exe").exists():
                resolved = candidate
            else:
                nested = sorted(p for p in candidate.glob("jdk*") if (p / "bin" / "java.exe").exists())
                if not nested:
                    continue
                resolved = nested[-1]
            os.environ["JAVA_HOME"] = str(resolved)
            os.environ["PATH"] = f"{resolved / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return str(resolved)
    return current


def configure_delta(spark: SparkSession) -> SparkSession:
    """Verify Delta is active on an existing session.

    ``spark.sql.extensions`` and ``spark.sql.catalog.spark_catalog`` are static
    configs: Spark refuses to change them after the session exists
    (CANNOT_MODIFY_STATIC_CONFIG). They must therefore be supplied on the
    builder, which ``get_local_spark`` does. This helper only verifies the
    outcome so a mis-wired session fails loudly rather than silently writing
    parquet where Delta was expected.
    """
    if not _delta_packages_available():
        return spark
    if "DeltaSparkSessionExtension" not in spark.conf.get("spark.sql.extensions", ""):
        _LOGGER.warning(
            "Delta extension not active; .format('delta') calls will fail. "
            "Build the session via get_local_spark() to enable it."
        )
    return spark


def get_local_spark(app_name: str = "med834enrl") -> SparkSession:
    """Return a Delta-enabled local SparkSession, reusing one if it exists."""
    _ensure_java_home()
    _ensure_hadoop_home()

    # Spark workers default to "python3"; point them at this interpreter.
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
        # Default JVM heap (1g) is not enough for the window functions in the
        # Silver address/hierarchy steps on a multi-core local box: 16 slots
        # each sort a partition of the shuffle and the driver OOMs. Raise the
        # ceiling and give the driver a real off-heap/spill budget.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.windowExec.buffer.spill.threshold", "4096")
        .config("spark.sql.session.timeZone", "UTC")
        # Static configs -- must be set before the session is created.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    active = SparkSession.getActiveSession()
    if active is not None:
        return configure_delta(active)

    if _delta_packages_available():
        builder = builder.config("spark.jars.packages", _delta_packages())

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "ERROR"))
    return configure_delta(spark)


def _delta_packages_available() -> bool:
    """True when the delta-spark Python package is importable."""
    try:
        import delta  # noqa: F401
    except ImportError:
        return False
    return True


def _delta_packages() -> str:
    """Resolve the Delta Maven coordinates matching the installed Delta version.

    Delta artifacts are published per Spark-Scala build (e.g.
    delta-spark_4.2_2.13), so the coordinates are derived rather than hardcoded
    to avoid a mismatch after a Spark upgrade.
    """
    import delta

    delta_version = getattr(delta, "__version__", "")
    try:
        from pyspark import __version__ as pyspark_version

        spark_series = ".".join(pyspark_version.split(".")[:2])
    except Exception:  # pragma: no cover - defensive
        spark_series = "4.2"
    scala = os.environ.get("DELTA_SCALA_VERSION", "2.13")
    return f"io.delta:delta-spark_{spark_series}_{scala}:{delta_version}"


def stop_local_spark() -> None:
    """Stop the active session if one exists. Useful in script cleanup."""
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
