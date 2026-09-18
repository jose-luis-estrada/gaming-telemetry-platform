"""Entrypoint: build all three Gold tables from Silver, then demonstrate late-data
reprocessing. Run interactively in VS Code (# %% cells) or as a module via
`make gold`. Clean build first, horizon split second, replaceWhere reprocess last."""

# %%
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from src.ingestion.gold import (
    build_player_daily,
    build_game_health_daily,
    build_revenue_daily,
    write_gold,
    within_horizon,
    past_horizon,
    add_lateness,
    reprocess_partition,
)

# Local Delta session. If Bronze/Silver already give you a session helper, use it
# instead: this stands alone only so run_gold runs by itself.
builder = (
    SparkSession.builder.appName("gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# data/<layer>/<source> convention, same as Bronze and rejects.
SILVER_PLAYER_EVENTS = "data/silver/player_events"
SILVER_PURCHASES = "data/silver/purchases"
GOLD_PLAYER_DAILY = "data/gold/player_daily"
GOLD_GAME_HEALTH_DAILY = "data/gold/game_health_daily"
GOLD_REVENUE_DAILY = "data/gold/revenue_daily"

events = spark.read.format("delta").load(SILVER_PLAYER_EVENTS)
purchases = spark.read.format("delta").load(SILVER_PURCHASES)

# %%
# Gold is built from within-horizon events only: on-time plus late-but-within-48h.
# Past-horizon stragglers are excluded here and routed to late_after_close below, so
# a frozen partition never absorbs a straggler. purchases has no late defect, so it
# is aggregated whole.
eligible = within_horizon(events)

write_gold(build_player_daily(eligible), GOLD_PLAYER_DAILY, "event_date")
write_gold(build_game_health_daily(eligible), GOLD_GAME_HEALTH_DAILY, "event_date")
write_gold(build_revenue_daily(purchases), GOLD_REVENUE_DAILY, "purchase_date")

# %%
# Past-horizon stragglers go to late_after_close: inspectable, partitioned by the
# event_date they belong to, carrying _lateness_hours as the reason. Overwrite for
# idempotency (two runs, same rows), same choice as the W4 rejects table. Only
# player_events feeds this: purchases has no late-arrival defect.
GOLD_LATE_AFTER_CLOSE = "data/gold/late_after_close"
stragglers = past_horizon(events)
(
    stragglers.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date")
    .save(GOLD_LATE_AFTER_CLOSE)
)

# %%
# Nothing dropped: within_horizon + past_horizon must equal all of Silver. The
# clean+rejects=total invariant from W4, restated at the horizon boundary.
n_eligible = eligible.count()
n_straggler = stragglers.count()
n_total = events.count()
print("within horizon:", n_eligible)
print("past horizon (late_after_close):", n_straggler)
print("total silver:", n_total)
assert n_eligible + n_straggler == n_total, "horizon split lost or duplicated rows"

# %%
# Row counts are the first idempotency signal: re-running the three writes must
# not move them, because overwrite makes each table a pure function of Silver.
for name, path in [
    ("player_daily", GOLD_PLAYER_DAILY),
    ("game_health_daily", GOLD_GAME_HEALTH_DAILY),
    ("revenue_daily", GOLD_REVENUE_DAILY),
]:
    print(name, "rows:", spark.read.format("delta").load(path).count())


# %%
# Pick one date that actually received within-horizon late arrivals, so excluding
# them makes a visible wrong-low gap. Any mid-window date qualifies; 2026-01-16 is
# past the drift boundary and well inside the 30-day window.
TARGET_DATE = "2026-01-16"

# Split Silver for that date into two arrivals:
#   on_time: lateness <= 6h, the batch Gold saw when the partition first closed
#   late_in : 6h < lateness <= 48h, within-horizon corrections that arrived after
# 6h is just a within-horizon cut to manufacture a "before" state; the horizon is
# still 48h. Both are within-horizon, so both legitimately belong in Gold.
eligible_all = within_horizon(events)  # on-time + within-48h, all dates
target = add_lateness(eligible_all).where(F.col("event_date") == TARGET_DATE)

on_time = target.where(F.col("_lateness_hours") <= 6).drop("_lateness_hours")
late_in = target.where(F.col("_lateness_hours") > 6).drop("_lateness_hours")
print("on_time rows for target:", on_time.count())
print("within-horizon late rows for target:", late_in.count())  # must be > 0

# %%
# BEFORE: reprocess the target partition with ONLY the on-time batch. This
# simulates Gold closing the partition before the late corrections landed, so the
# aggregate for that date is wrong-low by exactly the late rows.
reprocess_partition(build_player_daily(on_time), GOLD_PLAYER_DAILY, TARGET_DATE)

before = (
    spark.read.format("delta").load(GOLD_PLAYER_DAILY)
    .where(F.col("event_date") == TARGET_DATE)
    .agg(F.sum("total_events").alias("events"))
    .first()["events"]
)
print("BEFORE total_events for", TARGET_DATE, ":", before)

# %%
# AFTER: the late corrections arrive. Recompute the date from the FULL
# within-horizon population (on_time + late_in) and replaceWhere the same
# partition. Only this date's files are rewritten; every other partition is
# untouched.
full_target = on_time.unionByName(late_in)
reprocess_partition(build_player_daily(full_target), GOLD_PLAYER_DAILY, TARGET_DATE)

after = (
    spark.read.format("delta").load(GOLD_PLAYER_DAILY)
    .where(F.col("event_date") == TARGET_DATE)
    .agg(F.sum("total_events").alias("events"))
    .first()["events"]
)
print("AFTER total_events for", TARGET_DATE, ":", after)
print("delta (should equal within-horizon late rows):", after - before)

# %%
# CORRECTNESS: the after value must match Silver restricted to within-horizon
# arrivals for that date. Not "close": exact. This is the ground-truth check.
truth = (
    within_horizon(events)
    .where(F.col("event_date") == TARGET_DATE)
    .count()
)
print("Silver within-horizon truth for", TARGET_DATE, ":", truth)
assert after == truth, "Gold after reprocess does not match within-horizon Silver"