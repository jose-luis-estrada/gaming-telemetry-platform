"""Gold layer: three business tables built from Silver. This is where the
config-driven thesis deliberately STOPS. Bronze and Silver are declared in YAML
because ingestion and dedup are the same shape for every source; a Gold
aggregate is bespoke business logic, so a generic aggregate DSL would buy nothing
for three tables and cost the ability to defend each metric. Each builder is a
pure function of a DataFrame so it unit-tests on small hand-built frames."""

from pyspark.sql import DataFrame, functions as F


# ----------------------------
# player_events -> daily tables
# ----------------------------
# event_date is derived from event_timestamp, NEVER ingestion_timestamp: an event
# belongs to the day it HAPPENED, so a late event lands in an already-closed
# partition. That is what makes late data a Gold problem and not a no-op. Ch 11.

def build_player_daily(events: DataFrame) -> DataFrame:
    # Grain: one row per player per event day. Use Silver's canonical event_date
    # column, set once in the generator from event time and carried through every
    # layer. Re-deriving it with to_date(event_timestamp) forked the definition
    # and produced a phantom 31st date, because the generator's day bucket is
    # offset from midnight so a bucket crosses a calendar boundary. One boundary,
    # one definition, or replaceWhere on event_date selects different rows per
    # layer. DDIA Ch 11 semantics hold: Silver's event_date is event-time-derived.
    return (
        events
        .groupBy("event_date", "player_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("game_id").alias("games_played"),
            F.sum((F.col("event_type") == "login").cast("int")).alias("login_count"),
            F.sum((F.col("event_type") == "crash").cast("int")).alias("crash_count"),
        )
    )


def build_game_health_daily(events: DataFrame) -> DataFrame:
    # Same fix: group on Silver's event_date, never re-derive it.
    return (
        events
        .groupBy("event_date", "game_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum((F.col("event_type") == "crash").cast("int")).alias("crash_count"),
        )
        .withColumn("crash_rate", F.col("crash_count") / F.col("total_events"))
    )


# ----------------------------
# purchases -> revenue table
# ----------------------------
# purchases has NO game_id and NO late-arrival defect (single producer, single
# clock). So revenue is grained on what the source actually carries, and this
# table is the clean baseline: no horizon, no reprocessing.

def build_revenue_daily(purchases: DataFrame) -> DataFrame:
    # Grain: one row per item_category per purchase day.
    return (
        purchases
        .withColumn("purchase_date", F.to_date("purchase_timestamp"))
        .groupBy("purchase_date", "item_category")
        .agg(
            F.sum("price_usd").alias("revenue_usd"),
            F.count("*").alias("purchase_count"),
            F.countDistinct("player_id").alias("distinct_buyers"),
        )
    )


# ----------------------------
# writer
# ----------------------------

def write_gold(df: DataFrame, path: str, partition_col: str) -> None:
    # Clean build uses overwrite: each Gold table is a pure function of Silver, so
    # a wholesale rebuild is the same idempotency mechanism as W2 Bronze (two
    # runs, identical counts). W6 late data replaces this on the player_events
    # tables with a partition-scoped replaceWhere so a late event rewrites one
    # date, not the whole table.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy(partition_col)
        .save(path)
    )