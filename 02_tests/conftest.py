"""
tests/conftest.py
Shared pytest fixtures for local PySpark tests.
"""
import pytest
from src.medenroll834.common.spark_session import get_local_spark
@pytest.fixture(scope="session")
def spark():
    """Delta-enabled local SparkSession.

    Java, Python worker and Delta configuration all live in
    src/utils/spark_session.py so tests, scripts and entry points share one
    bootstrap instead of each re-implementing it.
    """
    return get_local_spark("med834enrl-test")

@pytest.fixture(scope="session")
def delta_spark(spark):
    """Alias fixture making the Delta requirement explicit at the call site."""
    return spark

