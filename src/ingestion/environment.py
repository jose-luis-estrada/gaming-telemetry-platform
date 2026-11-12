"""Runtime environment seam.

This is the only module that knows where the pipeline runs. Ingestion, Silver
and Gold code hold an ``Environment`` and never branch on location. Each
environment owns exactly what differs between deployments: the Spark session,
the storage roots, how a source is read, and the terminal write verbs.

Today there is one implementation, ``LocalEnvironment`` (local PySpark against
the local filesystem). The KIND lakehouse environment (S3A on SeaweedFS) plugs
in here as a second subclass without touching the layer code.
"""

import os
from abc import ABC, abstractmethod

from pyspark.sql import Column, DataFrame, SparkSession

from src.ingestion.config import SourceConfig

LAYERS = ("bronze", "quarantine", "silver", "gold")


class Environment(ABC):
    """Strategy base class. Subclasses provide storage roots and I/O verbs."""

    def __init__(self) -> None:
        self._spark: SparkSession | None = None

    # --- session -----------------------------------------------------------

    def spark(self) -> SparkSession:
        # Built once and reused across every source in a run.
        if self._spark is None:
            self._spark = self._build_spark()
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    # --- addressing --------------------------------------------------------

    def landing_uri(self, cfg: SourceConfig) -> str:
        # cfg.landing_path is relative to the landing root and identical across
        # environments; only the root changes.
        return _join(self._landing_root(), cfg.landing_path)

    def checkpoint_uri(self, cfg: SourceConfig) -> str:
        # Stable per-(source, target) location declared in the source YAML.
        return _join(self._checkpoint_root(), cfg.checkpoint_path)

    def table_uri(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}, expected one of {LAYERS}")
        return _join(self._table_root(layer), name)

    # --- abstract ----------------------------------------------------------

    @abstractmethod
    def _build_spark(self) -> SparkSession: ...

    @abstractmethod
    def _landing_root(self) -> str: ...

    @abstractmethod
    def _checkpoint_root(self) -> str: ...

    @abstractmethod
    def _table_root(self, layer: str) -> str: ...

    @abstractmethod
    def read_source(self, cfg: SourceConfig, checkpoint_uri: str) -> DataFrame:
        """Read a source's landing data, schema-on-read."""

    @abstractmethod
    def write_bronze(self, df: DataFrame, cfg: SourceConfig, checkpoint_uri: str) -> int:
        """Write Bronze and return the read-back row count of the table."""

    @abstractmethod
    def write_silver(self, df: DataFrame, cfg: SourceConfig) -> int:
        """Write Silver and return the read-back row count of the table."""

    @abstractmethod
    def source_file_col(self) -> Column:
        """Column expression identifying the source file of each row."""

    @abstractmethod
    def delta_table(self, cfg: SourceConfig, spark: SparkSession, layer: str = "bronze"):
        """Return a DeltaTable handle for a source's table in the given layer."""


class LocalEnvironment(Environment):
    """Local PySpark with Delta tables addressed by filesystem path.

    Storage root comes from ``TELEMETRY_DATA_ROOT`` (default ``data``), so the
    same code runs from a laptop checkout or a container with a mounted volume.
    """

    def __init__(self, data_root: str | None = None) -> None:
        super().__init__()
        self.data_root = (data_root or os.environ.get("TELEMETRY_DATA_ROOT", "data")).rstrip("/")

    def _build_spark(self) -> SparkSession:
        # configure_spark_with_delta_pip resolves the Delta jars that match the
        # installed delta-spark package.
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("telemetry-local")
            .master(os.environ.get("SPARK_MASTER", "local[*]"))
            # The Silver dedup window sorts one group per event_id. At 50M
            # identities the default ~1 GB driver heap runs out of memory in
            # local mode; on a cluster the sort distributes across executors.
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def _landing_root(self) -> str:
        return f"{self.data_root}/landing"

    def _checkpoint_root(self) -> str:
        return f"{self.data_root}/checkpoints"

    def _table_root(self, layer: str) -> str:
        return f"{self.data_root}/{layer}"

    def read_source(self, cfg: SourceConfig, checkpoint_uri: str) -> DataFrame:
        reader = self.spark().read.format(cfg.format)
        for k, v in cfg.read_options.items():
            reader = reader.option(k, v)
        # Hive partition discovery recovers event_date from the path. No explicit
        # schema: Bronze is schema-on-read, so a drifted source still lands.
        return reader.load(self.landing_uri(cfg))

    def write_bronze(self, df: DataFrame, cfg: SourceConfig, checkpoint_uri: str) -> int:
        # Known gap: full overwrite is O(total) per run and discards history,
        # which contradicts the append-only Bronze contract. Replaced by
        # incremental ingestion in the lakehouse environment (see ADR-0005).
        path = self.table_uri("bronze", cfg.name)
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .option("overwriteSchema", "true")
            .save(path)
        )
        # Count the written table rather than df.count(), which would recompute
        # the whole lazy plan.
        return self.spark().read.format("delta").load(path).count()

    def write_silver(self, df: DataFrame, cfg: SourceConfig) -> int:
        # Silver is a full rebuild from Bronze, so overwrite is the idempotency
        # mechanism here: a derived table has no history worth preserving.
        path = self.table_uri("silver", cfg.name)
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .option("overwriteSchema", "true")
            .save(path)
        )
        return self.spark().read.format("delta").load(path).count()

    def source_file_col(self) -> Column:
        from pyspark.sql import functions as F

        return F.input_file_name()

    def delta_table(self, cfg: SourceConfig, spark: SparkSession, layer: str = "bronze"):
        from delta.tables import DeltaTable

        return DeltaTable.forPath(spark, self.table_uri(layer, cfg.name))


_ENVIRONMENTS = {"local": LocalEnvironment}


def get_environment() -> Environment:
    """Resolve the environment once, at the program edge, from ``TELEMETRY_ENV``."""
    name = os.environ.get("TELEMETRY_ENV", "local").lower()
    try:
        return _ENVIRONMENTS[name]()
    except KeyError:
        raise ValueError(
            f"unknown TELEMETRY_ENV {name!r}, expected one of {sorted(_ENVIRONMENTS)}"
        ) from None


def _join(root: str, rel: str | None) -> str:
    # A relative path of "" or "." means the root itself.
    rel = (rel or "").strip("/.")
    return f"{root}/{rel}" if rel else root
