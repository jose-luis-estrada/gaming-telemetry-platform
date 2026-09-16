# %%
import os
os.chdir("/Users/joseestrada/projects/gaming-telemetry-platform")

# Scratch to CONFIRM broadcast vs sort-merge join in the query plan, not guess it.
# The strategy is VISIBLE in .explain() and switches on table SIZE. DDIA Ch 10.
# Every cell RESETS the two relevant knobs first, because Spark session config
# persists across cells: a leftover -1 from the skew notebook silently flips the
# plan. Resetting per cell is the difference between a real demo and a confusing one.
from src.ingestion.config import load_source_config
from src.ingestion.environment import get_environment
from pyspark.sql import functions as F

env = get_environment()
spark = env.spark()

cfg = load_source_config("config/sources/player_events.yaml")
bronze = env.delta_table(cfg, spark).toDF()

games_dim = spark.createDataFrame(
    [(1, "Aces"), (2, "Bolt"), (3, "Cinder"), (4, "Drift")],
    "game_id long, game_name string",
)

# %%
# ----------------------------
# Broadcast join (map-side): fact JOIN tiny dimension
# ----------------------------
# AQE OFF here so .explain() shows the FINAL plan. With AQE on, explain prints the
# pre-runtime plan (isFinalPlan=false), where the static planner has no size stats
# for the Delta scan and defaults to sort-merge; AQE would flip it to broadcast at
# runtime, but explain does not run. Turning AQE off lets the static planner use
# the broadcast threshold directly, so the BroadcastHashJoin is visible in explain.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
spark.conf.set("spark.sql.adaptive.enabled", False)

# Force the tiny side to broadcast explicitly with a hint, so the intent is in the
# plan regardless of stats: F.broadcast() tells Spark to ship this side to every node.
broadcast_join = bronze.join(F.broadcast(games_dim), on="game_id", how="inner")
# Now expect BroadcastHashJoin + BroadcastExchange on the dim. DDIA Ch 10.
broadcast_join.explain()
# %%
# ----------------------------
# Sort-merge join (reduce-side): BOTH sides too big to broadcast
# ----------------------------
# HONEST demo: purchases is only 150K rows, so it is ALSO broadcastable and a
# fact-JOIN-purchases plan comes out BroadcastHashJoin, not sort-merge. To force a
# genuine sort-merge we need two large sides. Self-join player_events on player_id:
# both sides are 50M, neither fits a broadcast, so Spark MUST shuffle both, sort
# each partition, and merge. This is the real reduce-side join. DDIA Ch 10.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # default ON
spark.conf.set("spark.sql.adaptive.enabled", True)

# Alias the same table twice so the self-join has distinct sides.
left = bronze.select("player_id", "event_id").alias("l")
right = bronze.select("player_id", F.col("event_id").alias("event_id_r")).alias("r")
sortmerge_join = left.join(right, on="player_id", how="inner")
# Expect SortMergeJoin + an Exchange (shuffle) on BOTH sides. Neither is broadcast
# because both are 50M. That contrast against the BroadcastHashJoin above is the
# whole criterion. (This is a big self-join; do NOT .count() it, just .explain().)
sortmerge_join.explain()

# %%
# ----------------------------
# Proof it is SIZE that decides, not the tables
# ----------------------------
# Same tiny-dim join as cell 1, but auto-broadcast OFF (-1). Now the 4-row dim
# CANNOT be broadcast, so the same join flips to SortMergeJoin. Same tables,
# different plan: the strategy is a planner COST decision on size, not a data property.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", True)
forced_smj = bronze.join(games_dim, on="game_id", how="inner")
forced_smj.explain()

# %%
# env.stop()