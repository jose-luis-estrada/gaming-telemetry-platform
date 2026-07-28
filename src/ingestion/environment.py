#%%
# ----------------------------
# The seam
# ----------------------------
# The only module that knows local vs cloud. Ingestion logic holds an Environment
# and never asks where it runs. This IS W3 exit criterion #3: "only the path
# resolver changes, no branching in the ingestion logic." The one remaining
# conditional lives in get_environment(), read once at the program edge.

import os
from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter

from src.ingestion.config import SourceConfig


class Environment:
    """Strategy base. Each subclass owns the three things that differ by
    environment: the Spark session, the landing root, and the terminal write verb."""

    def __init__(self):
        self._spark = None

    def spark(self) -> SparkSession:
        # Cached: built once, reused across every source in the run.
        if self._spark is None:
            self._spark = self._build_spark()
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()

    def landing_uri(self, cfg: SourceConfig) -> str:
        # cfg.landing_path is now RELATIVE to the landing root ("" for a source
        # that sits flat at the root, like player_events today). Same value in
        # both environments; only the root differs.
        root = self._landing_root()
        rel = (cfg.landing_path or "").strip("/.")
        return f"{root}/{rel}" if rel else root

    def _build_spark(self) -> SparkSession:
        raise NotImplementedError

    def _landing_root(self) -> str:
        raise NotImplementedError

    def write_bronze(self, writer: DataFrameWriter, cfg: SourceConfig) -> None:
        # Receives a writer already configured in bronze.py (format, mode,
        # partitionBy, overwriteSchema). The ONLY step that differs by environment
        # is the terminal verb, which is why it lives here and nowhere else.
        raise NotImplementedError

    def source_file_col(self):
        # Column expression that identifies the source file per row. The two
        # environments expose this through different APIs, so the difference
        # lives here, not in bronze.py. This keeps add_lineage identical in both.
        raise NotImplementedError

class LocalEnvironment(Environment):
    LANDING_ROOT = "data/landing"
    BRONZE_ROOT = "data/bronze"

    def _build_spark(self) -> SparkSession:
        # Local wires the Delta extension by hand via configure_spark_with_delta_pip,
        # which resolves the Delta jars matching the installed delta-spark. This
        # block MUST NOT run on Databricks: serverless is Delta-native and the jar
        # resolution is unnecessary and unavailable there. Isolating it is the
        # entire reason the seam exists.
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder.appName("bronze-ingest-local")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def _landing_root(self) -> str:
        return self.LANDING_ROOT

    def write_bronze(self, writer: DataFrameWriter, cfg: SourceConfig) -> None:
        # Path-based (unmanaged) Delta: the files ARE the table, no catalog entry.
        writer.save(f"{self.BRONZE_ROOT}/{cfg.name}")

    def source_file_col(self):
        from pyspark.sql import functions as F
        # Legacy Spark API. Works locally; Unity Catalog blocks it (UC_COMMAND_
        # NOT_SUPPORTED) because it is fragile under query optimization.
        return F.input_file_name()

class DatabricksEnvironment(Environment):
    LANDING_ROOT = "/Volumes/workspace/telemetry/landing"
    UC_SCHEMA = "workspace.telemetry"

    def _build_spark(self) -> SparkSession:
        # Serverless already has an active session. getOrCreate() returns it
        # untouched. Zero Delta configs on purpose: native here.
        return SparkSession.builder.getOrCreate()

    def stop(self) -> None:
        # No-op: the serverless session is managed by the platform. Stopping it
        # from a notebook is wrong and can detach the whole session.
        pass

    def _landing_root(self) -> str:
        return self.LANDING_ROOT

    def write_bronze(self, writer: DataFrameWriter, cfg: SourceConfig) -> None:
        # Managed UC table: the catalog owns the name -> files mapping. bronze_
        # prefix namespaces the medallion layer inside the existing telemetry
        # schema, so no new schema is created today.
        writer.saveAsTable(f"{self.UC_SCHEMA}.bronze_{cfg.name}")

    def source_file_col(self):
        from pyspark.sql import functions as F
        # UC-governed metadata column. Stable across any file read; the
        # replacement UC recommends over input_file_name().
        return F.col("_metadata.file_path")

def get_environment() -> Environment:
    # The single surviving conditional, read once at the edge. Everything
    # downstream is environment-blind. Default local so nothing accidentally
    # writes to Unity Catalog.
    env = os.environ.get("TELEMETRY_ENV", "local").lower()
    if env == "local":
        return LocalEnvironment()
    if env == "databricks":
        return DatabricksEnvironment()
    raise ValueError(f"Unknown TELEMETRY_ENV: {env!r} (expected 'local' or 'databricks')")