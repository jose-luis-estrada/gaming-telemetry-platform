# %%
# ----------------------------
# Second source: purchases
# ----------------------------
# A deliberately DIFFERENT-shaped source, used to prove the platform thesis:
# onboarding it takes one YAML and zero framework code. Its columns do not
# overlap player_events (no metadata JSON, no producer_id, no stack_trace), and
# that is the point: Bronze is schema-on-read, so the framework ingests columns
# it has never seen without a single change to src/ingestion/.
#
# Landing layout mirrors player_events on purpose: parquet under
# data/landing/purchases/event_date=YYYY-MM-DD/, single-level Hive partition.
# event_date lives in the PATH, not in the file, so Spark partition discovery
# recovers it on read and the hardcoded partitionBy("event_date") just works.

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd


def find_repo_root(marker="config"):
    # Same anchor as the events generator: __file__ is absent in # %% cells, so
    # walk up from CWD until we hit the dir that owns the marker. Keeps this
    # runnable as `python -m src.generator.purchases` from the repo root.
    path = Path.cwd()
    for candidate in [path, *path.parents]:
        if (candidate / marker).is_dir():
            return candidate
    raise FileNotFoundError(f"repo root not found: no '{marker}' dir above {Path.cwd()}")


REPO_ROOT = find_repo_root()
# Same selector as the events generator: TELEMETRY_MANIFEST picks the profile,
# so one env var drives both sources and their dates always line up.
MANIFEST_PATH = REPO_ROOT / os.environ.get("TELEMETRY_MANIFEST", "config/manifest.json")
manifest = json.loads(MANIFEST_PATH.read_text())
window = manifest["event_window"]
# Same root as the events generator and LocalEnvironment (TELEMETRY_DATA_ROOT),
# so a dev run lands next to dev player_events, never in the full landing.
DATA_ROOT = REPO_ROOT / os.environ.get("TELEMETRY_DATA_ROOT", "data")
OUT = DATA_ROOT / "landing" / "purchases"

# Own seed on purpose: the manifest decides the dates, but changing its seed
# must not reshuffle purchases. Still covered by verify-repro (lives in landing/).
rng = np.random.default_rng(7)

# Purchases follows the manifest's event window so Gold can line revenue up
# against player_events by date. Seed and volume stay local: the manifest owns
# WHEN, this producer owns WHAT. Fixed dates (14-16) dated from the retired
# Databricks Volume subset (ADR-0013) and no longer apply.
start = pd.Timestamp(window["start_utc"])
DATES = pd.date_range(start, periods=window["days"], freq="D").strftime("%Y-%m-%d").tolist()
ROWS_PER_DATE = 50_000

ITEM_CATEGORIES = np.array(["skin", "currency_pack", "battle_pass", "dlc", "loot_box"])
CURRENCIES = np.array(["USD", "EUR", "GBP", "JPY", "MXN"])

purchase_id = 0  # monotonic across the whole source, so ids are globally unique

for date in DATES:
    n = ROWS_PER_DATE
    # Timestamps scattered across the day, tz-aware UTC to match player_events.
    day_start = pd.Timestamp(date, tz="UTC")
    offsets = rng.integers(0, 24 * 60 * 60, size=n)
    purchase_timestamp = day_start + pd.to_timedelta(offsets, unit="s")

    df = pd.DataFrame(
        {
            "purchase_id": np.arange(purchase_id, purchase_id + n, dtype=np.int64),
            "player_id": rng.integers(1, 5_000_000, size=n, dtype=np.int64),
            "item_id": rng.integers(1, 2000, size=n, dtype=np.int64),
            # rng.choice on a string array returns object dtype; cast to pandas
            # "string" so the parquet physical type is stable. This is the exact
            # class of bug that forced the same cast in the events generator.
            "item_category": pd.array(rng.choice(ITEM_CATEGORIES, size=n), dtype="string"),
            "currency": pd.array(rng.choice(CURRENCIES, size=n), dtype="string"),
            # Round to cents: a realistic price, not a raw float.
            "price_usd": np.round(rng.uniform(0.99, 99.99, size=n), 2),
            "purchase_timestamp": purchase_timestamp,  # tz-aware preserved
        }
    )
    purchase_id += n

    # event_date goes in the PATH, not the frame: Spark recovers it via partition
    # discovery on read, the same contract player_events lands under.
    part_dir = OUT / f"event_date={date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    # Microsecond timestamps: Spark 3.5 cannot read parquet TIMESTAMP(NANOS).
    # Timestamps have whole-second precision, so nothing is truncated.
    df.to_parquet(
        part_dir / "part-0.parquet",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=False,
    )
    print(f"purchases {date}: {n} rows -> {part_dir}/part-0.parquet")

print(f"done: {purchase_id} purchase rows across {len(DATES)} dates in {OUT}")
