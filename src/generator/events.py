# %%
import json
from pathlib import Path

import pandas as pd
import numpy as np
import shutil
import hashlib

# ----------------------------
# Repo root
# ----------------------------
def find_repo_root(marker="config"):
    # __file__ does not exist in # %% cells, so we cannot anchor to the source
    # file. Walk up from CWD until we hit the directory that owns the marker.
    path = Path.cwd()
    for candidate in [path, *path.parents]:
        if (candidate / marker).is_dir():
            return candidate
    # Fail loud: a missing root is a broken checkout, not something to guess at.
    raise FileNotFoundError(f"repo root not found: no '{marker}' dir above {Path.cwd()}")

REPO_ROOT = find_repo_root()

# ----------------------------
# Load manifest
# ----------------------------
# The manifest is the spec: written before generation, single source of truth.
# The generator reads from it and holds no distribution numbers of its own.
MANIFEST_PATH = REPO_ROOT / "config" / "manifest.json"  # absolute, CWD-independent
manifest = json.loads(MANIFEST_PATH.read_text())

seed = manifest["seed"]
n_rows = manifest["rows"]
skew = manifest["defects"]["skew"]
window = manifest["event_window"]

# ----------------------------
# RNG
# ----------------------------
# One rng object seeded from the manifest so every draw is reproducible.
rng = np.random.default_rng(seed)

# ----------------------------
# Helpers
# ----------------------------
def build_weights(game_ids, hot_game_id, hot_prob, tolerance):
    # Derive weights from the skew SHAPE: hot game gets hot_prob, the rest split
    # the remainder evenly. Deriving beats hand-typing so weights can't drift.
    if hot_game_id not in game_ids:
        raise ValueError(
            f"manifest: hot_game_id {hot_game_id} not in game_ids {game_ids}"
        )
    
    # Guard 2: a probability outside (0, 1) is nonsense, and numpy would only
    # complain later with a message that never mentions your manifest.
    if not 0 < hot_prob < 1:
        raise ValueError(
            f"manifest: hot game probability {hot_prob} must be between 0 and 1"
        )
    
    cold_ids = [g for g in game_ids if g != hot_game_id]

    if not cold_ids:
        raise ValueError("manifest: need at least one game besides the hot one")
    
    cold_weight = (1 - hot_prob) / len(cold_ids)

    weights = [hot_prob if g == hot_game_id else cold_weight for g in game_ids]

    assert abs(sum(weights) - 1.0) < tolerance, f"weights sum to {sum(weights)}"

    return weights

# ----------------------------
# Generate data
# ----------------------------
weights = build_weights(
    game_ids=skew["game_ids"],
    hot_game_id=skew["hot_game_id"],
    hot_prob=skew["hot_game_probability"],
    tolerance=skew["tolerance"]
)

# start_utc carries the Z, so this Timestamp is tz-aware UTC and every
# event_timestamp inherits it. One clock, declared, not implicit. DDIA Ch 8.
start = pd.Timestamp(window["start_utc"])
window_seconds = window["days"] * 86_400
# Shared by the base event_type draw and the correction-duplicate redraw.
event_types = ["login", "logout", "purchase", "match_start", "match_end"]

events = pd.DataFrame({
    # Sequential ints: reproducible event_id across runs.
    "event_id": range(1, n_rows + 1),
    "player_id": rng.integers(1, 20, size=n_rows),
    # Skew lives in the key distribution, driven by the manifest weights.
    "game_id": rng.choice(skew["game_ids"], size=n_rows, p=weights),
    "event_type": rng.choice(event_types, size=n_rows),
    # Drawn LAST so player_id/game_id/event_type keep the same rng stream and
    # stay bit-identical. rng.integers excludes the high, i.e. a semi-open
    # window [start, start+30d). Jan 31 never appears.
    "event_timestamp": start + pd.to_timedelta(rng.integers(
        0, window_seconds, size=n_rows), unit="s")
})

# ----------------------------
# Late arrivals
# ----------------------------
# ingestion_timestamp is when the event landed in the platform, separate from
# event_timestamp (when it happened). Event time vs processing time, DDIA Ch 11.
# Drawn AFTER the DataFrame so the earlier columns keep their rng stream.
late = manifest["defects"]["late_arrivals"]
normal_max = late["normal_max_delay_seconds"]
max_late_s = late["max_lateness_hours"] * 3600

is_late = rng.random(n_rows) < late["late_fraction"]
normal_delay = rng.integers(0, normal_max, size=n_rows)
# Late delays start where normal ends, so a single threshold separates the two
# populations with no overlap and the late fraction stays verifiable.
late_delay = rng.integers(normal_max, max_late_s, size=n_rows)
delay_seconds = np.where(is_late, late_delay, normal_delay)

# event_timestamp still drives the partition, not this. A late event lands in a
# partition whose event_date already passed. That is the whole defect.
events["ingestion_timestamp"] = events["event_timestamp"] + pd.to_timedelta(
    delay_seconds, unit="s"
)

# ----------------------------
# Producer identity
# ----------------------------
# Every event carries its producer and that producer's own monotonic sequence.
# This pair, not the wall clock, is the dedup ordering key: 3 producers, 3
# clocks. DDIA Ch 8. producer_id is reused later by the small-files defect.
producer_id = rng.integers(1, 4, size=n_rows) # producers 1..3
events["producer_id"] = producer_id
# per-producer counter in emission (row) order
events["source_sequence_number"] = (
    pd.Series(producer_id).groupby(producer_id).cumcount().to_numpy()
)

# ----------------------------
# Metadata and schema drift
# ----------------------------
# metadata lands as a raw JSON string, the way telemetry actually arrives: the
# platform stores the blob in Bronze and parses it on read. Drift here is a KEY
# appearing INSIDE the blob, not a new table column. The framework's declared
# per-source schema is the contract; a key it doesn't know about is the defect.
drift = manifest["defects"]["schema_drift"]

# Boundary keyed on event_timestamp, not ingestion_timestamp: a producer ships a
# new client version at a wall-clock moment and every event it EMITS afterward
# carries the new key. event_timestamp drives the partition, so the boundary is
# clean in partition space and one query verifies it. DDIA Ch 11.
drift_start = start + pd.Timedelta(days=drift["drift_day"])
is_new_schema = events["event_timestamp"] >= drift_start

device = pd.Series(rng.choice(drift["devices"], size=len(events)))
network = pd.Series(rng.choice(drift["new_key_values"], size=len(events)))

# Closed vocabulary (no quotes to escape), so we interpolate the JSON string
# vectorized instead of paying a json.dumps call per row across 50M rows.
before = '{"device": "' + device + '", "app_version": "' + drift["version_before"] + '"}'
after = (
    '{"device": "' + device + '", "app_version": "' + drift["version_after"]
    + '", "' + drift["new_key"] + '": "' + network + '"}'
)
events["metadata"] = np.where(is_new_schema, after, before)

# ----------------------------
# Duplicates
# ----------------------------
dup = manifest["defects"]["duplicates"]
n_dup = int(n_rows * dup["duplicate_fraction"])
dup_idx = rng.choice(n_rows, size=n_dup, replace=False)
dups = events.iloc[dup_idx].copy()

# Tail of the duplicate set are corrections: same event_id, redrawn payload, and
# a strictly higher sequence so dedup keeps the correction over the original.
n_identical = int(n_dup * dup["byte_identical_ratio"])
is_correction = np.arange(n_dup) >= n_identical
if is_correction.sum() > 0:
    dups.loc[is_correction, "event_type"] = rng.choice(
        event_types, size=int(is_correction.sum())
    )
    dups.loc[is_correction, "source_sequence_number"] += 1_000_000

# A few duplicates land past the 3-day dedup window, so they survive dedup on
# purpose. This is what makes the bounded guarantee visible, not theoretical.
escape = np.arange(n_dup)[: dup ["out_of_window_count"]]
col = dups.columns.get_loc("ingestion_timestamp")
dups.iloc[escape, col] = dups.iloc[escape, col] + pd.Timedelta(
    hours=dup["dedup_window_hours"]
) + pd.Timedelta(hours=1)

events = pd.concat([events, dups], ignore_index=True)

# ----------------------------
# Write files
# ----------------------------
# event_date (from event_timestamp) is the PARTITION directory, single level.
# The file split WITHIN a partition is one producer's flush window, keyed on
# ingestion_timestamp: partition = event time, file = processing time. A late
# event lands in an old event_date directory via a new flush. DDIA Ch 11.
small = manifest["defects"]["small_files"]
flush = f"{small['flush_minutes']}min"

events["event_date"] = events["event_timestamp"].dt.strftime("%Y-%m-%d")
# Floor the producer's wall clock to the flush cadence: every event a producer
# received in the same 5-minute window flushes together into one file.
events["flush_window"] = events["ingestion_timestamp"].dt.floor(flush)

out = REPO_ROOT / "data" / "landing"
# Clean stale runs so file counts stay verifiable against the manifest.
if out.exists():
    shutil.rmtree(out)

n_files = 0
# One file per (event_date, producer, flush window): 3 producers x 288 windows
# x 30 days ~ 25,920 tiny files. Deliberately bad. Compaction is a W3 platform
# capability, not the producer's job.
for (event_date, producer_id, flush_window), group in events.groupby(
    ["event_date", "producer_id", "flush_window"], sort=False
):
    part_dir = out / f"event_date={event_date}"  # single-level Hive partition
    part_dir.mkdir(parents=True, exist_ok=True)
    stamp = flush_window.strftime("%Y%m%dT%H%M")
    fname = f"part-p{producer_id}-{stamp}.parquet"
    # Drop the two bookkeeping columns: event_date is implied by the directory,
    # flush_window was only ever a grouping key, not part of the event schema.
    group.drop(columns=["event_date", "flush_window"]).to_parquet(
        part_dir / fname, index=False
    )
    n_files += 1

print("files written:", n_files)

# ----------------------------
# Inspect
# ----------------------------

# W1 exit criterion: the small-files defect is measurable, not asserted.
sizes = [p.stat().st_size for p in out.rglob("*.parquet")]
print("files:", len(sizes), "mean KB:", round(sum(sizes) / len(sizes) / 1024, 1))
# %%
