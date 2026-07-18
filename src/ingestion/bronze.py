# %%
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

from src.ingestion.config import SourceConfig

# ----------------------------
# Spark session
# ----------------------------
# Local Spark with the Delta extension. configure_spark_with_delta_pip pins the
# Delta jars matching the installed delta-spark, so the session speaks Delta
# without a manual --packages line. On Databricks this whole function is replaced
# by the managed runtime; here it is what lets us read DAGs in the local Spark UI.
def build_spark(app_name: str = "bronze-ingest") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# ----------------------------
# Lineage
# ----------------------------
# Every Bronze row carries where it came from and when we ingested it. This is
# the cataloging/lineage capability the platform sells, and the first thing an
# on-call reads at 3 AM: which file, which run. Underscore prefix marks these as
# framework metadata, not source columns.
def add_lineage(df: DataFrame, source_name: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_file", F.input_file_name())  # exact file per row
        .withColumn("_ingested_at", F.current_timestamp())  # processing time
        .withColumn("_batch_id", F.lit(batch_id))  # ties every row to one run
    )

# ----------------------------
# Bronze path
# ----------------------------
# Logical name in the contract (e.g. bronze.player_events) maps to a local Delta
# path here. On Databricks the same logical name registers as a Unity Catalog
# table instead; the config does not change, only this resolver does.
def bronze_path(cfg: SourceConfig) -> str:
    return f"data/bronze/{cfg.name}"

# ----------------------------
# Ingest
# ----------------------------
def ingest_source(spark: SparkSession, cfg: SourceConfig) -> int:
    batch_id = uuid.uuid4().hex

    reader = spark.read.format(cfg.format)
    for k, v in cfg.read_options.items():
        reader = reader.option(k, v)
    # No .schema(...) on purpose: schema-on-read. Bronze takes the source as-is
    # so a drifted source lands instead of failing. DDIA Ch 4.
    raw = reader.load(cfg.landing_path)

    out = add_lineage(raw, cfg.name, batch_id)

    # overwrite, not append: for the W2 local demo this makes a re-run idempotent
    # by replacement, so running twice does not duplicate. Doctrine says Bronze is
    # append-only; incremental idempotent ingest (Autoloader checkpoint / MERGE)
    # is W3. Trade-off stated, not hidden.
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # tolerate a changed source schema
        .save(bronze_path(cfg))
    )
    return out.count()