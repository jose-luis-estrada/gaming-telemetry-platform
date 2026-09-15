# %%
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.ingestion.config import SourceConfig


# ----------------------------
# Dedup
# ----------------------------
def deduplicate(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """One row per identity_key, keeping the winner by ordering_key.

    Config-driven: identity and order come from the YAML, zero source-specific
    code. Same platform thesis as ingestion and quality."""
    if not cfg.identity_key:
        # Fail loud: a Silver source with no identity has no notion of "duplicate",
        # so silently returning df would hide a broken contract. DDIA Ch 4 belongs
        # upstream; here identity is mandatory.
        raise ValueError(f"{cfg.name}: identity_key is required for Silver dedup")

    # Rank duplicates within each identity group. Highest ordering_key wins, so
    # a correction (higher source_sequence_number) beats the original, and a
    # byte-identical retry ties on everything and collapses to one. The winner
    # keeps EVERY column: this is why a window beats groupBy, which would force
    # re-aggregating each column by hand.
    order = [F.col(c).desc() for c in cfg.ordering_key]
    w = Window.partitionBy(*cfg.identity_key).orderBy(*order)

    # row_number, not rank: rank would keep BOTH rows of a byte-identical tie
    # (same rank 1), which is the opposite of dedup. row_number breaks ties
    # arbitrarily and always yields exactly one winner per identity.
    ranked = df.withColumn("_dedup_rank", F.row_number().over(w))
    return ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")


# ----------------------------
# Bounded window
# ----------------------------
def within_dedup_window(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Split rows into (in-window, out-of-window) by ingestion recency.

    Only in-window rows are reconciled against each other. Out-of-window rows
    pass through UNTOUCHED: this is what makes the 5 seeded escapees survive and
    the bounded guarantee VISIBLE instead of theoretical. DDIA Ch 3: global dedup
    would reshuffle all history every run."""
    if cfg.dedup_window_hours is None:
        # Unbounded: everything is in-window, nothing escapes. purchases path.
        return df, df.limit(0)

    # Window anchored to the LATEST ingestion in the batch, not wall-clock now():
    # keeps the split reproducible across runs, so two runs give identical counts.
    # A now()-based cutoff would drift every run and break Silver idempotency.
    max_ingest = df.agg(F.max("ingestion_timestamp")).first()[0]
    cutoff = F.lit(max_ingest) - F.expr(f"INTERVAL {cfg.dedup_window_hours} HOURS")

    in_window = df.filter(F.col("ingestion_timestamp") >= cutoff)
    out_window = df.filter(F.col("ingestion_timestamp") < cutoff)
    return in_window, out_window


# ----------------------------
# Build Silver
# ----------------------------
def build_silver(bronze_df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Bronze -> Silver: bounded dedup, one row per identity.

    In-window rows are deduplicated; out-of-window rows pass through as-is so the
    bounded guarantee stays observable. Their union is Silver."""
    in_window, out_window = within_dedup_window(bronze_df, cfg)
    deduped = deduplicate(in_window, cfg)
    # unionByName, not union: match on column name, not position, so a schema
    # change upstream cannot silently misalign the two frames. W4 used the same.
    return deduped.unionByName(out_window)


def ingest_silver(env, cfg: SourceConfig) -> int:
    """Read Bronze, build Silver, write it, return the read-back count."""
    bronze_df = env.delta_table(cfg, env.spark()).toDF()  # same seam as quality.py
    silver_df = build_silver(bronze_df, cfg)
    return env.write_silver(silver_df, cfg)