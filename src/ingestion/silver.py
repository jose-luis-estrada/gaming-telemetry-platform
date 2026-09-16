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
        # Fail loud: a Silver source with no identity has no notion of "duplicate".
        raise ValueError(f"{cfg.name}: identity_key is required for Silver dedup")

    # Rank duplicates within each identity group. Highest ordering_key wins, so a
    # correction (higher source_sequence_number) beats the original, and a
    # byte-identical retry ties on everything and collapses to one. The winner
    # keeps EVERY column: this is why a window beats groupBy.
    order = [F.col(c).desc() for c in cfg.ordering_key]
    w = Window.partitionBy(*cfg.identity_key).orderBy(*order)

    # row_number, not rank: rank would keep BOTH rows of a byte-identical tie (same
    # rank 1), the opposite of dedup. row_number always yields exactly one winner.
    ranked = df.withColumn("_dedup_rank", F.row_number().over(w))
    return ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")


# ----------------------------
# Bounded window: measured BETWEEN the two copies, not against the batch max
# ----------------------------
def within_dedup_window(df: DataFrame, cfg: SourceConfig):
    """Split df into (reconcilable, escapees) by how far apart two copies of the
    SAME identity landed. Two rows of one event_id are reconcilable only if their
    ingestion_timestamps are within dedup_window_hours of EACH OTHER. A copy that
    landed far later than its original escapes and survives dedup, which is what
    makes the bounded guarantee visible. DDIA Ch 3: bounded, not global.

    WHY the earlier max-of-batch anchor was wrong: events span 30 days, so the
    batch max ingestion is day 30 and a 72h window covered only the last 3 days.
    27 days of duplicates fell 'out of window' and passed through undeduplicated.
    The window is a distance between copies, not a recency cutoff on the batch."""
    if cfg.dedup_window_hours is None:
        # Unbounded: nothing is ever an escapee. purchases path (no duplicates).
        return df, df.limit(0)

    # Per identity, the EARLIEST ingestion is the "anchor" copy: the original.
    # Every other copy is measured against it. A copy within the window of that
    # earliest one is reconcilable; a copy beyond it escapes. Using the earliest
    # (not the latest) makes the anchor stable regardless of how many copies exist.
    anchor = Window.partitionBy(*cfg.identity_key)
    tagged = df.withColumn(
        "_anchor_ingest", F.min("ingestion_timestamp").over(anchor)
    )

    # Distance in seconds between this row and its identity's earliest copy.
    # <= window: reconcilable (goes through dedup). > window: escapee (untouched).
    within = (
        F.col("ingestion_timestamp").cast("long") - F.col("_anchor_ingest").cast("long")
    ) <= cfg.dedup_window_hours * 3600

    reconcilable = tagged.filter(within).drop("_anchor_ingest")
    escapees = tagged.filter(~within).drop("_anchor_ingest")
    return reconcilable, escapees


# ----------------------------
# Build Silver
# ----------------------------
def build_silver(bronze_df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Bronze -> Silver: bounded dedup, one row per identity within the window.

    Reconcilable copies (within window of their earliest) are deduplicated to one
    winner; escapees (landed beyond the window) pass through as-is so the bounded
    guarantee stays observable. Their union is Silver."""
    reconcilable, escapees = within_dedup_window(bronze_df, cfg)
    deduped = deduplicate(reconcilable, cfg)
    # unionByName, not union: match on column name, not position, so a schema
    # change upstream cannot silently misalign the two frames. W4 used the same.
    return deduped.unionByName(escapees)


def ingest_silver(env, cfg: SourceConfig) -> int:
    """Read Bronze, build Silver, write it, return the read-back count."""
    bronze_df = env.delta_table(cfg, env.spark()).toDF()  # same seam as quality.py
    silver_df = build_silver(bronze_df, cfg)
    return env.write_silver(silver_df, cfg)