# %%
import uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.config import SourceConfig
from src.ingestion.environment import Environment

# ----------------------------
# Lineage
# ----------------------------
def add_lineage(df: DataFrame, source_name: str, batch_id: str, source_file_col) -> DataFrame:
    # source_file_col is supplied by the Environment: input_file_name() locally,
    # _metadata.file_path on Unity Catalog. add_lineage stays environment-blind.
    return (
        df.withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_file", source_file_col)        # env-provided expression
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )

# ----------------------------
# Ingest
# ----------------------------
def ingest_source(env: Environment, cfg: SourceConfig) -> int:
    batch_id = uuid.uuid4().hex

    reader = env.spark().read.format(cfg.format)
    for k, v in cfg.read_options.items():
        reader = reader.option(k, v)
    # Schema-on-read: no .schema(...), a drifted source lands instead of failing.
    # event_date recovered from the landing path via Hive partition discovery in
    # BOTH environments, which is why this logic is identical local and cloud.
    raw = reader.load(env.landing_uri(cfg))

    out = add_lineage(raw, cfg.name, batch_id, env.source_file_col())

    writer = (
        out.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")               # read-side pruning, DDIA Ch 6
        .option("overwriteSchema", "true")
    )
    env.write_bronze(writer, cfg)                # .save() local, .saveAsTable() UC
    return out.count()