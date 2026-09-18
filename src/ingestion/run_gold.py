"""Entrypoint: build all three Gold tables from Silver, clean full rebuild. Run
interactively in VS Code (# %% cells) or as a module. Late-data reprocessing is a
separate module (next batch); this is the baseline clean build."""

# %%
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingestion.gold import (
    build_player_daily,
    build_game_health_daily,
    build_revenue_daily,
    write_gold,
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
write_gold(build_player_daily(events), GOLD_PLAYER_DAILY, "event_date")
write_gold(build_game_health_daily(events), GOLD_GAME_HEALTH_DAILY, "event_date")
write_gold(build_revenue_daily(purchases), GOLD_REVENUE_DAILY, "purchase_date")

# %%
# Row counts are the first idempotency signal: re-running the three writes must
# not move them, because overwrite makes each table a pure function of Silver.
for name, path in [
    ("player_daily", GOLD_PLAYER_DAILY),
    ("game_health_daily", GOLD_GAME_HEALTH_DAILY),
    ("revenue_daily", GOLD_REVENUE_DAILY),
]:
    print(name, "rows:", spark.read.format("delta").load(path).count())