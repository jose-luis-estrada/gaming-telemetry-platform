#%%
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
OUT = REPO_ROOT / "data" / "landing" / "purchases"
 
# Fixed seed: purchases is a standalone producer, not part of the manifest's
# reproducibility guarantee, so it carries its own deterministic seed.
rng = np.random.default_rng(7)
 
# Three dates that also exist in the cloud Volume subset (14/15/16), so if this
# source is later mirrored to the Volume the dates line up. Small on purpose:
# the goal is proving onboarding, not scale, which player_events already owns.
DATES = ["2026-01-14", "2026-01-15", "2026-01-16"]
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
            # class of bug that forced the events-generator cast in W2.
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
    df.to_parquet(part_dir / "part-0.parquet", index=False)
    print(f"purchases {date}: {n} rows -> {part_dir}/part-0.parquet")
 
print(f"done: {purchase_id} purchase rows across {len(DATES)} dates in {OUT}")