# %%
import os
os.chdir("/Users/joseestrada/projects/gaming-telemetry-platform")

# Scratch to CONFIRM broadcast vs sort-merge join in the query plan, not guess it.
# The point of W5's join criterion: the strategy is VISIBLE in .explain(), and it
# switches on table size. DDIA Ch 10 (map-side vs reduce-side join).
from src.ingestion.config import load_source_config
from src.ingestion.environment import get_environment
from pyspark.sql import functions as F

env = get_environment()
spark = env.spark()

cfg = load_source_config("config/sources/player_events.yaml")
bronze = env.delta_table(cfg, spark).toDF()

# %%
# ----------------------------
# Broadcast join (map-side): fact JOIN tiny dimension
# ----------------------------
# A 4-row dim is far below the 10 MB auto-broadcast default, so Spark ships the
# WHOLE dim to every node and each task joins locally. NO shuffle of the 50M side.
# This is the map-side join: the small table becomes a lookup on each mapper. Ch 10.
games_dim = spark.createDataFrame(
    [(1, "Aces"), (2, "Bolt"), (3, "Cinder"), (4, "Drift")],
    "game_id long, game_name string",
)

# Reset both knobs to DEFAULT so the planner chooses freely: auto-broadcast on at
# the 10 MB default, AQE on. We are demonstrating the DEFAULT behavior now.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
spark.conf.set("spark.sql.adaptive.enabled", True)

broadcast_join = bronze.join(games_dim, on="game_id", how="inner")
# .explain() prints the physical plan. Look for "BroadcastHashJoin": that word IS
# the proof the tiny side is broadcast and the 50M side is never shuffled.
broadcast_join.explain()

# %%
# ----------------------------
# Sort-merge join (reduce-side): fact JOIN fact
# ----------------------------
# Join player_events to purchases on player_id. Both are large, neither fits a
# broadcast, so Spark shuffles BOTH sides by the join key, sorts each partition,
# and merges. This is the reduce-side join: the key drives the shuffle. Ch 10.
purchases_cfg = load_source_config("config/sources/purchases.yaml")
purchases = env.delta_table(purchases_cfg, spark).toDF()

# player_events uses player_id (int8, 1..19); purchases uses player_id too, so the
# keys line up. Neither side is broadcastable, so the planner MUST sort-merge.
sortmerge_join = bronze.join(purchases, on="player_id", how="inner")
# Look for "SortMergeJoin" in this plan, plus an Exchange (the shuffle) on BOTH
# sides. That contrast against the BroadcastHashJoin above is the whole criterion.
sortmerge_join.explain()

# %%
# Optional proof it is the SIZE that decides, not the tables: force the tiny-dim
# join to sort-merge by turning auto-broadcast OFF. Same tables, different plan.
# This shows the strategy is a planner COST decision, not a property of the data.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
forced_smj = bronze.join(games_dim, on="game_id", how="inner")
forced_smj.explain()  # now BroadcastHashJoin becomes SortMergeJoin: size drove it

# %%
# env.stop()  # run last, keeps the UI alive if you want to inspect stages too