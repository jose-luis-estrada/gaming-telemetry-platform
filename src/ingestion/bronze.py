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
    checkpoint = env.checkpoint_uri(cfg)                 # stable per-(source,target) dir

    raw = env.read_source(cfg, checkpoint)               # batch OR Autoloader stream
    out = add_lineage(raw, cfg.name, batch_id, env.source_file_col())  # IDENTICAL both envs
    return env.write_bronze(out, cfg, checkpoint)        # env picks overwrite vs Autoloader