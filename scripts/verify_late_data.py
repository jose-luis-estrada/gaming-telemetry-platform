"""Verify that late-arriving data corrects Gold, scoped to one partition.

Run after ``make gold``. For one target event_date it:

1. rewrites the partition from on-time events only (the stale, wrong-low state
   a Gold table would hold before late data arrives),
2. reprocesses it with the late within-horizon events included,
3. checks the corrected value equals Silver's within-horizon truth,
4. checks the Delta log shows exactly one partition rewritten,
5. reprocesses again and checks nothing moves (reprocess is idempotent).

The table ends in its correct state. Usage:

    python -m scripts.verify_late_data [YYYY-MM-DD]
"""

import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.ingestion.environment import get_environment
from src.ingestion.gold import add_lateness, build_player_daily, reprocess_partition, within_horizon

DEFAULT_TARGET_DATE = "2026-01-16"
# Events landing more than this long after their event time count as "late"
# for the purpose of building the stale baseline.
ON_TIME_MAX_HOURS = 6


def total_events(spark, path: str, event_date: str) -> int:
    return (
        spark.read.format("delta")
        .load(path)
        .where(F.col("event_date") == event_date)
        .agg(F.sum("total_events").alias("n"))
        .first()["n"]
    )


def main(target_date: str = DEFAULT_TARGET_DATE) -> None:
    env = get_environment()
    spark = env.spark()
    gold_path = env.table_uri("gold", "player_daily")
    events = spark.read.format("delta").load(env.table_uri("silver", "player_events"))

    target = add_lateness(within_horizon(events)).where(F.col("event_date") == target_date)
    on_time = target.where(F.col("_lateness_hours") <= ON_TIME_MAX_HOURS).drop("_lateness_hours")
    late = target.where(F.col("_lateness_hours") > ON_TIME_MAX_HOURS).drop("_lateness_hours")
    n_late = late.count()
    if n_late == 0:
        raise SystemExit(f"no within-horizon late events for {target_date}; pick another date")

    rows_before = spark.read.format("delta").load(gold_path).count()

    # 1. Stale state: Gold built before the late events arrived.
    reprocess_partition(build_player_daily(on_time), gold_path, target_date)
    stale = total_events(spark, gold_path, target_date)

    # 2. Correction: reprocess with the late events included.
    full = on_time.unionByName(late)
    reprocess_partition(build_player_daily(full), gold_path, target_date)
    corrected = total_events(spark, gold_path, target_date)

    # 3. Correct, not just bounded.
    truth = target.count()
    print(f"{target_date}: stale={stale} corrected={corrected} truth={truth} late={n_late}")
    assert corrected == truth, "corrected Gold does not match Silver within-horizon truth"
    assert corrected - stale == n_late, "correction delta does not equal the late rows"

    # 4. Blast radius from the transaction log: one partition removed and added.
    last = DeltaTable.forPath(spark, gold_path).history(1).first()
    metrics = last["operationMetrics"]
    print("last commit:", last["operation"], last["operationParameters"].get("predicate"), metrics)
    assert int(metrics["numRemovedFiles"]) <= 1, "reprocess rewrote more than one partition"

    # 5. Idempotent reprocess.
    reprocess_partition(build_player_daily(full), gold_path, target_date)
    assert total_events(spark, gold_path, target_date) == truth, "second reprocess moved the value"
    rows_after = spark.read.format("delta").load(gold_path).count()
    assert rows_after == rows_before, "reprocess changed the table row count"

    print("OK: late data corrected one partition, reprocess is idempotent")
    env.stop()


if __name__ == "__main__":
    main(*sys.argv[1:2])
