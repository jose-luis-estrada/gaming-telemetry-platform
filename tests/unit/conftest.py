"""Shared fixtures for unit tests.

Unit tests run on small in-memory DataFrames and never touch Delta storage, so
they use a plain local SparkSession with no Delta jars. That keeps them fast
and runnable without network access to Maven.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("telemetry-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
