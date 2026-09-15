# %%
import os
os.chdir("/Users/joseestrada/projects/gaming-telemetry-platform")

# Scratch for OBSERVING the skew straggler in the local Spark UI, then FIXING it
# and measuring before/after. Rule 3: see it suffer, capture the number, THEN fix.
# This notebook is postmortem #1 evidence, gathered not asserted.
from src.ingestion.config import load_source_config
from src.ingestion.environment import get_environment
from pyspark.sql import functions as F

env = get_environment()          # LocalEnvironment via TELEMETRY_ENV
spark = env.spark()

cfg = load_source_config("config/sources/player_events.yaml")
bronze = env.delta_table(cfg, spark).toDF()

# %%
# Ground truth: the skew is real in the DATA. game_id=1 holds ~18.6M of 50M.
# The straggler ratio later should mirror this. DDIA Ch 6.
bronze.groupBy("game_id").agg(F.count("*").alias("n")).orderBy(F.desc("n")).show()

# %%
# A count() shows NOTHING because groupBy(count) is ADDITIVE: Spark pre-aggregates
# map-side (the combiner), so the 18.6M hot key never travels. To SEE the straggler
# we need a shuffle that cannot pre-aggregate: a join keyed on game_id. DDIA Ch 10.
games_dim = spark.createDataFrame(
    [(1, "Aces"), (2, "Bolt"), (3, "Cinder"), (4, "Drift")],
    "game_id long, game_name string",
)

# %%
# ----------------------------
# BEFORE: skew visible, no fix
# ----------------------------
# Force a SORT-MERGE join: disable auto-broadcast so the shuffle happens (the
# shuffle is where skew becomes a straggler). Disable AQE so nothing auto-splits.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", False)

# All 18.6M rows of game_id=1 land in ONE task. In the UI the join stage shows
# Shuffle Read Max ~18.6M records vs Median ~0, Duration Max ~5s vs ~5ms, ~1GB spill.
joined = bronze.join(games_dim, on="game_id", how="inner")
print("BEFORE (no fix) joined rows:", joined.count())

# %%
# ----------------------------
# AQE ATTEMPTED, did not apply at this scale (a FINDING, not a failure)
# ----------------------------
# AQE judges skew in BYTES, not rows. The hot partition is 18.6M rows but only
# ~1.9 MiB compressed, far under the 256 MB default, so AQE never flagged it even
# after lowering the threshold to 1m and the factor to 1.5: with only 4 non-empty
# partitions AQE coalesces to 4 tasks before the skew rule can split anything.
# AQE is built for GB-scale skew; this is MB-scale. Documented, then moved past.
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1m")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1.5")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
joined_aqe = bronze.join(games_dim, on="game_id", how="inner")
print("AQE attempt joined rows:", joined_aqe.count())  # UI: still 1 task at 18.6M

# %%
# ----------------------------
# AFTER: salting (the fix that actually splits the hot key)
# ----------------------------
# Salting does NOT depend on the engine deciding anything: we split the hot key BY
# HAND. Give every big-side row a random salt in [0, N), so one game_id becomes N
# shuffle keys and its 18.6M rows spread across N tasks. The dim is replicated once
# per salt so each salted row still finds its match. DDIA Ch 6 (partition the key).
SALT_N = 8  # 18.6M / 8 ~ 2.3M per sub-partition, near the cold keys' size

# AQE OFF so this compares apples to apples with BEFORE: both plain sort-merge,
# the ONLY difference is the salt. Otherwise AQE coalescing muddies the readout.
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Big side: attach a random salt. floor(rand()*N) is a uniform int in [0, N), so
# each game_id's rows scatter evenly across N buckets instead of piling in one.
bronze_salted = bronze.withColumn(
    "_salt", (F.floor(F.rand() * SALT_N)).cast("int")
)

# Dim side: replicate each of the 4 rows N times, one per salt value, so a big-side
# row salted k joins the dim copy salted k. explode over a [0..N) array does this.
# Cost: dim grows 4 -> 32 rows, trivial. This is why you only salt the HOT key in
# prod: replicating a large dim x N is the salting tax. Here the dim is tiny.
games_dim_salted = games_dim.withColumn(
    "_salt", F.explode(F.array(*[F.lit(i) for i in range(SALT_N)]))
)

# Join on BOTH game_id and salt: the composite key has 4*N distinct values, so the
# hot key is now N partitions of ~2.3M instead of one of 18.6M. Straggler gone.
joined_salted = bronze_salted.join(
    games_dim_salted, on=["game_id", "_salt"], how="inner"
).drop("_salt")  # salt is scaffolding; drop it so the result matches the unsalted join

print("AFTER (salted) joined rows:", joined_salted.count())

# %%
# Sanity: the fix must not change the RESULT, only the runtime shape. All three
# counts identical (50,500,004: 50.5M events + 1% dups, each matching a dim row).
print("counts identical (fix is behavior-preserving):",
      joined.count() == joined_salted.count() == joined_aqe.count())

# %%
# Leave the session alive to read the UI. Run this LAST, once numbers are captured.
# env.stop()
# %%
