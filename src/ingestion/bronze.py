# %%
import uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.config import SourceConfig
from src.ingestion.environment import Environment

# ----------------------------
# Lineage
# ----------------------------
def add_lineage(df: DataFrame, source_name: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_file", F.input_file_name())   # exact file per row
        .withColumn("_ingested_at", F.current_timestamp())  # processing time
        .withColumn("_batch_id", F.lit(batch_id))           # ties row to one run
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

    out = add_lineage(raw, cfg.name, batch_id)

    writer = (
        out.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")               # read-side pruning, DDIA Ch 6
        .option("overwriteSchema", "true")
    )
    env.write_bronze(writer, cfg)                # .save() local, .saveAsTable() UC
    return out.count()