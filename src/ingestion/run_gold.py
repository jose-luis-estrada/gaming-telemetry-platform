"""Entrypoint for the Silver -> Gold stage.

Builds the three Gold tables and routes past-horizon stragglers to
late_after_close. This module only builds: it never rewrites a partition to an
intentionally wrong state. The late-data reprocess check lives in
``scripts/verify_late_data.py``.
"""

from pyspark.sql import DataFrame

from src.ingestion.environment import Environment, get_environment
from src.ingestion.gold import (
    build_game_health_daily,
    build_player_daily,
    build_revenue_daily,
    past_horizon,
    within_horizon,
    write_gold,
)

# Each Gold table and the grain it must be unique on.
GRAINS = {
    "player_daily": ["event_date", "player_id"],
    "game_health_daily": ["event_date", "game_id"],
    "revenue_daily": ["purchase_date", "item_category"],
}


def build_gold(env: Environment) -> dict[str, int]:
    spark = env.spark()
    events = spark.read.format("delta").load(env.table_uri("silver", "player_events"))
    purchases = spark.read.format("delta").load(env.table_uri("silver", "purchases"))

    # Only events that arrived within the horizon of their event time feed Gold.
    eligible = within_horizon(events)
    stragglers = past_horizon(events)

    tables: dict[str, tuple[DataFrame, str]] = {
        "player_daily": (build_player_daily(eligible), "event_date"),
        "game_health_daily": (build_game_health_daily(eligible), "event_date"),
        "revenue_daily": (build_revenue_daily(purchases), "purchase_date"),
        "late_after_close": (stragglers, "event_date"),
    }
    for name, (df, partition_col) in tables.items():
        write_gold(df, env.table_uri("gold", name), partition_col)

    counts = {
        name: spark.read.format("delta").load(env.table_uri("gold", name)).count()
        for name in tables
    }

    # Invariants: the horizon split neither drops nor duplicates rows, and every
    # Gold table is unique on its declared grain.
    n_events = events.count()
    if eligible.count() + counts["late_after_close"] != n_events:
        raise AssertionError("horizon split lost or duplicated rows")
    for name, keys in GRAINS.items():
        df = spark.read.format("delta").load(env.table_uri("gold", name))
        if df.select(*keys).distinct().count() != counts[name]:
            raise AssertionError(f"gold.{name} is not unique on its grain {keys}")

    return counts


def main(env: Environment | None = None) -> None:
    env = env or get_environment()
    for name, n in build_gold(env).items():
        print(f"gold.{name}: {n} rows")
    env.stop()


if __name__ == "__main__":
    main()
