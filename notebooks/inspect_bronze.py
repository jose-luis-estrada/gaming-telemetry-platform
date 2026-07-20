# %%
# Scratch cells for inspecting Bronze in the local Spark UI.
# Not part of the framework: run.py stays the production entry point with its
# own spark.stop(). Here we leave the session alive so the UI stays up.
from src.ingestion.config import load_source_config
from src.ingestion.bronze import build_spark, ingest_source, bronze_path

spark = build_spark()
cfg = load_source_config("config/sources/player_events.yaml")
n = ingest_source(spark, cfg)
print(f"ingested {n} rows -> {bronze_path(cfg)}")
# No spark.stop() here on purpose: localhost:4040 dies with the session.

# %%
# Optional check: row count landed, and the event_date partitions exist.
df = spark.read.format("delta").load(bronze_path(cfg))
print("bronze rows:", df.count())
df.select("event_date").distinct().orderBy("event_date").show(5)

# %%
# Run this LAST, once you are done looking at the UI, to free the port and RAM.
spark.stop()