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

    def checkpoint_uri(self, cfg: SourceConfig) -> str:
        # Same shape as landing_uri: stable logical name in the YAML, physical
        # root swapped by environment. DDIA Ch 3: logical name stable, binding swapped.
        root = self._checkpoint_root()
        rel = (cfg.checkpoint_path or "").strip("/.")
        return f"{root}/{rel}" if rel else root

    def _checkpoint_root(self) -> str:
        raise NotImplementedError

    def read_source(self, cfg: SourceConfig, checkpoint_uri: str):
        # Batch reader locally, Autoloader stream on cloud. The two are different
        # objects (DataFrameReader vs DataStreamReader), which is exactly why this
        # cannot live in bronze.py without an if. checkpoint_uri unused in batch.
        raise NotImplementedError

    def write_bronze(self, df, cfg: SourceConfig, checkpoint_uri: str) -> int:
        # Was write_bronze(writer, cfg). Now owns the WHOLE write, because a batch
        # writer and a stream writer share no terminal verb. Returns the read-back
        # table count, which is the number that must be stable across two runs.
        raise NotImplementedError

    def source_file_col(self):
        # Column expression that identifies the source file per row. The two
        # environments expose this through different APIs, so the difference
        # lives here, not in bronze.py. This keeps add_lineage identical in both.
        raise NotImplementedError

class LocalEnvironment(Environment):
    LANDING_ROOT = "data/landing"
    BRONZE_ROOT = "data/bronze"
    CHECKPOINT_ROOT = "data/checkpoints"

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

    def _checkpoint_root(self) -> str:
        return self.CHECKPOINT_ROOT

    def read_source(self, cfg: SourceConfig, checkpoint_uri: str):
        reader = self.spark().read.format(cfg.format)
        for k, v in cfg.read_options.items():
            reader = reader.option(k, v)
        # Hive partition discovery recovers event_date from the path. No .schema():
        # schema-on-read, a drifted source lands. Same as W2.
        return reader.load(self.landing_uri(cfg))

    def write_bronze(self, df, cfg: SourceConfig, checkpoint_uri: str) -> int:
        path = f"{self.BRONZE_ROOT}/{cfg.name}"
        # overwrite: O(total), destroys history. Fine for a learning env whose job
        # is the Spark UI. checkpoint_uri ignored here on purpose.
        (df.write.format("delta").mode("overwrite")
            .partitionBy("event_date").option("overwriteSchema", "true")
            .save(path))
        # Count the WRITTEN table, not df.count(): symmetric with the cloud path
        # and avoids recomputing the whole lazy pipeline.
        return self.spark().read.format("delta").load(path).count()

    def source_file_col(self):
        from pyspark.sql import functions as F
        # Legacy Spark API. Works locally; Unity Catalog blocks it (UC_COMMAND_
        # NOT_SUPPORTED) because it is fragile under query optimization.
        return F.input_file_name()

    def delta_table(self, cfg, spark):
        from delta.tables import DeltaTable
        # Unmanaged Delta local: la tabla se direcciona por su path.
        return DeltaTable.forPath(spark, self.bronze_path(cfg))

class DatabricksEnvironment(Environment):
    LANDING_ROOT = "/Volumes/workspace/telemetry/landing"
    UC_SCHEMA = "workspace.telemetry"
    CHECKPOINT_ROOT = "/Volumes/workspace/telemetry/checkpoints"

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

    def _checkpoint_root(self) -> str:
        return self.CHECKPOINT_ROOT

    def read_source(self, cfg: SourceConfig, checkpoint_uri: str):
        reader = (
            self.spark().readStream.format("cloudFiles")
            .option("cloudFiles.format", cfg.format)
            # Where Autoloader persists the schema it infers, so it can tell
            # "same schema" from "evolved". Durable state, same lifetime as the
            # checkpoint, so it sits under it.
            .option("cloudFiles.schemaLocation", f"{checkpoint_uri}/schema")
        )
        for k, v in cfg.read_options.items():
            reader = reader.option(k, v)
        # No explicit schema. Parquet is self-describing so every file shares the
        # physical schema (metadata is a string column in all of them). The seeded
        # drift is a KEY inside that JSON string, NOT a physical column, so Autoloader
        # never sees evolution and the drift lands silently. The W1 "JSON string over
        # physical column" decision is what buys this. DDIA Ch 4.
        return reader.load(self.landing_uri(cfg))

    def write_bronze(self, df, cfg: SourceConfig, checkpoint_uri: str) -> int:
        table = f"{self.UC_SCHEMA}.bronze_{cfg.name}"
        query = (
            df.writeStream.format("delta")
            # THE idempotency mechanism now. The file registry lives here: Autoloader
            # remembers which files it already read. Delete it and it re-ingests all
            # 10,211 files and the job STILL exits green. RUNBOOK entry.
            .option("checkpointLocation", f"{checkpoint_uri}/checkpoint")
            .partitionBy("event_date")
            # Process every file available right now across as many micro-batches as
            # needed, then STOP. Batch-shaped run on the streaming engine. The only
            # trigger Free Edition supports, and exactly what incremental batch wants.
            .trigger(availableNow=True)
            .toTable(table)          # managed UC table -> catalog + lineage
        )
        # availableNow returns immediately. Block until it drains every file, or the
        # program exits mid-ingest and leaves a partial run. Non-negotiable.
        query.awaitTermination()

        # Per-run visibility: recentProgress is the only place that shows what
        # Autoloader actually read THIS run, as opposed to the final table count
        # below. Empty list or numInputRows=0 is the file-level idempotency proof:
        # the checkpoint already knows 14/15/16 and skipped them. This is Databricks
        # -only (batch has no StreamingQuery), so the seam in bronze.py stays blind.
        progress = query.recentProgress
        if not progress:
            # AvailableNow can finish without committing any micro-batch when there
            # are no new files, so the list is empty rather than a batch of 0 rows.
            print(f"{cfg.name}: no micro-batch ran, checkpoint reports zero new files")
        for p in progress:
            print(f"{cfg.name}: batch {p['batchId']} numInputRows={p['numInputRows']}")

        # Read-back count of the whole table: a streaming df has no .count() (it is
        # unbounded, it throws), and the total table is what must be stable across runs.
        return self.spark().read.table(table).count()

    def source_file_col(self):
        from pyspark.sql import functions as F
        # UC-governed metadata column. Stable across any file read; the
        # replacement UC recommends over input_file_name().
        return F.col("_metadata.file_path")

    def delta_table(self, cfg, spark):
        from delta.tables import DeltaTable
        # Managed UC table: se direcciona por catalog.schema.name.
        return DeltaTable.forName(spark, self.bronze_target(cfg))

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