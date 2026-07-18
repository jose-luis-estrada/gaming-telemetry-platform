# %%
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

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
window = manifest["event_window"]
skew = manifest["defects"]["skew"]
late = manifest["defects"]["late_arrivals"]
drift = manifest["defects"]["schema_drift"]
crash = manifest["defects"]["crashes"]
dup = manifest["defects"]["duplicates"]
small = manifest["defects"]["small_files"]

# ----------------------------
# RNG
# ----------------------------
# One rng object seeded from the manifest. Every draw below happens in Phase 1,
# in a fixed order, so the whole run is reproducible from the seed alone.
rng = np.random.default_rng(seed)

# ----------------------------
# Helpers
# ----------------------------
def build_weights(game_ids, hot_game_id, hot_prob, tolerance):
    # Derive weights from the skew SHAPE: hot game gets hot_prob, the rest split
    # the remainder evenly. Deriving beats hand-typing so weights can't drift.
    if hot_game_id not in game_ids:
        raise ValueError(f"manifest: hot_game_id {hot_game_id} not in game_ids {game_ids}")
    if not 0 < hot_prob < 1:
        raise ValueError(f"manifest: hot game probability {hot_prob} must be between 0 and 1")
    cold_ids = [g for g in game_ids if g != hot_game_id]
    if not cold_ids:
        raise ValueError("manifest: need at least one game besides the hot one")
    cold_weight = (1 - hot_prob) / len(cold_ids)
    weights = [hot_prob if g == hot_game_id else cold_weight for g in game_ids]
    assert abs(sum(weights) - 1.0) < tolerance, f"weights sum to {sum(weights)}"
    return weights

# Vocabularies live as arrays and are referenced BY CODE in the compact frame.
# The wide string columns get expanded from these codes at write time, one file
# at a time, so 50M copies of each string never exist at once.
EVENT_TYPES = np.array(["login", "logout", "purchase", "match_start", "match_end"])
EXCEPTIONS = np.array(["NullPointerException", "TimeoutException", "OutOfMemoryError"])
DEVICES = np.array(drift["devices"])
NETWORKS = np.array(drift["new_key_values"])

def make_trace(exc_name):
    # Synthetic multi-line trace: enough shape to be semi-structured, not real.
    # The point is the storage decision, not the payload.
    return (
        f"{exc_name}: game loop stalled\n"
        f"  at engine.render.Frame.draw(Frame.java:812)\n"
        f"  at engine.core.Loop.tick(Loop.java:144)\n"
        f"  at engine.core.Main.run(Main.java:57)"
    )

# One trace string per exception, expanded by index in the loop. No per-row
# f-string over 50M rows.
TRACES = np.array([make_trace(e) for e in EXCEPTIONS])

# ----------------------------
# Phase 1: global decisions (compact)
# ----------------------------
# Everything that consumes the rng happens here, in a fixed order. Columns are
# kept as compact codes (int8) and timestamps (int64 under the hood), NOT as
# strings. The wide JSON/text columns are the memory hog; they are built later,
# per output file. GLOBAL work (dedup sampling, per-producer sequence) needs
# every row at once, but only over these cheap arrays. That is what keeps 50M
# rows off the single-frame ~36 GB ceiling.
weights = build_weights(
    skew["game_ids"], skew["hot_game_id"], skew["hot_game_probability"], skew["tolerance"]
)

# start_utc carries the Z, so this Timestamp is tz-aware UTC and every timestamp
# inherits it. One clock, declared, not implicit. DDIA Ch 8.
start = pd.Timestamp(window["start_utc"])
window_seconds = window["days"] * 86_400

player_id = rng.integers(1, 20, size=n_rows, dtype=np.int8)
# Skew lives in the key distribution, driven by the manifest weights.
game_id = rng.choice(skew["game_ids"], size=n_rows, p=weights).astype(np.int8)
event_type_code = rng.integers(0, len(EVENT_TYPES), size=n_rows, dtype=np.int8)
# Semi-open window [start, start+30d): the high is excluded, so day 31 never
# appears and the 30 partitions stay clean.
event_timestamp = start + pd.to_timedelta(rng.integers(0, window_seconds, size=n_rows), unit="s")

# ingestion_timestamp is when the event landed, separate from when it happened.
# Event time vs processing time, DDIA Ch 11. Late delays start where normal ends
# so a single threshold separates the two populations and the late fraction is
# verifiable, not approximate.
is_late = rng.random(n_rows) < late["late_fraction"]
normal_delay = rng.integers(0, late["normal_max_delay_seconds"], size=n_rows)
late_delay = rng.integers(late["normal_max_delay_seconds"], late["max_lateness_hours"] * 3600, size=n_rows)
delay_seconds = np.where(is_late, late_delay, normal_delay)
ingestion_timestamp = event_timestamp + pd.to_timedelta(delay_seconds, unit="s")

producer_id = rng.integers(1, 4, size=n_rows, dtype=np.int8)  # producers 1..3
device_code = rng.integers(0, len(DEVICES), size=n_rows, dtype=np.int8)
network_code = rng.integers(0, len(NETWORKS), size=n_rows, dtype=np.int8)

# Schema drift: a metadata KEY appearing from drift_day on. Gated on
# event_timestamp, not ingestion, so the boundary is clean in partition space
# and one query verifies it. DDIA Ch 11. Stored as a bool, expanded to JSON later.
drift_start = start + pd.Timedelta(days=drift["drift_day"])
is_new_schema = np.asarray(event_timestamp >= drift_start)

# A crash is its own event_type and carries a text stack trace. exc_code is the
# exception index for crash rows, -1 otherwise: this doubles as the crash flag.
is_crash = rng.random(n_rows) < crash["crash_fraction"]
# Only a subset of crashes capture a screenshot: bounds binary volume independent
# of row count.
has_shot = is_crash & (rng.random(n_rows) < crash["screenshot_fraction"])
exc_code = np.full(n_rows, -1, dtype=np.int8)
crash_pos = np.flatnonzero(is_crash)
exc_code[crash_pos] = rng.integers(0, len(EXCEPTIONS), size=crash_pos.size)

# Screenshot binaries: written once here, named by content hash (identical bytes
# collapse to one file). The row keeps only an index into this small table, so
# the null-heavy pointer columns never exist over all 50M rows.
shots_dir = REPO_ROOT / "data" / "screenshots"
if shots_dir.exists():
    shutil.rmtree(shots_dir)
shots_dir.mkdir(parents=True, exist_ok=True)

shot_pos = np.flatnonzero(has_shot)
shot_sizes = rng.integers(
    crash["screenshot_min_bytes"], crash["screenshot_max_bytes"], size=shot_pos.size
)
shot_idx = np.full(n_rows, -1, dtype=np.int32)  # -1 = no screenshot
shot_paths, shot_bytes, shot_types, shot_digests = [], [], [], []
for size in shot_sizes:
    # Deterministic bytes from the rng so the dataset stays bit-identical by seed.
    blob = rng.bytes(int(size))
    digest = hashlib.sha256(blob).hexdigest()
    (shots_dir / f"{digest}.png").write_bytes(blob)
    shot_paths.append(f"screenshots/{digest}.png")
    shot_bytes.append(int(size))
    shot_types.append("image/png")
    shot_digests.append(digest)
shot_idx[shot_pos] = np.arange(shot_pos.size, dtype=np.int32)
shot_table = {
    "path": np.array(shot_paths, dtype=object),
    "bytes": np.array(shot_bytes, dtype="int64"),
    "ctype": np.array(shot_types, dtype=object),
    "sha256": np.array(shot_digests, dtype=object),
}

# The compact base frame: every column is a code, a flag, or a timestamp. This
# is the whole dataset's worth of DECISIONS, holdable in memory at 50M.
events = pd.DataFrame(
    {
        "event_id": np.arange(1, n_rows + 1, dtype=np.int64),  # sequential, reproducible
        "player_id": player_id,
        "game_id": game_id,
        "event_type_code": event_type_code,
        "event_timestamp": event_timestamp,
        "ingestion_timestamp": ingestion_timestamp,
        "producer_id": producer_id,
        "device_code": device_code,
        "network_code": network_code,
        "is_new_schema": is_new_schema,
        "exc_code": exc_code,  # -1 = not a crash
        "shot_idx": shot_idx,  # -1 = no screenshot
    }
)

# per-producer monotonic counter in emission order. GLOBAL (needs the full
# producer column) but only over an int8 array. Paired with producer_id, this is
# the dedup ordering key: 3 producers, 3 clocks. DDIA Ch 8.
events["source_sequence_number"] = events.groupby("producer_id").cumcount().astype(np.int64)

# ----------------------------
# Duplicates
# ----------------------------
# Sampled GLOBALLY over all N rows: a duplicate can come from anywhere, so this
# cannot be done per-partition. Cheap here because we only copy compact codes.
n_dup = int(n_rows * dup["duplicate_fraction"])
dup_idx = rng.choice(n_rows, size=n_dup, replace=False)
dups = events.iloc[dup_idx].reset_index(drop=True).copy()

# Tail of the duplicate set are corrections: same event_id, redrawn payload, and
# a strictly higher sequence so dedup keeps the correction over the original.
n_identical = int(n_dup * dup["byte_identical_ratio"])
is_correction = np.arange(n_dup) >= n_identical
if is_correction.sum() > 0:
    dups.loc[is_correction, "event_type_code"] = rng.integers(
        0, len(EVENT_TYPES), size=int(is_correction.sum())
    ).astype(np.int8)
    dups.loc[is_correction, "source_sequence_number"] += 1_000_000

# A few duplicates land past the 3-day dedup window on purpose, so they survive
# dedup and the bounded guarantee is visible, not theoretical. Only ingestion
# time moves; event_time (and thus the partition) is unchanged.
escape = np.arange(n_dup)[: dup["out_of_window_count"]]
col = dups.columns.get_loc("ingestion_timestamp")
dups.iloc[escape, col] = (
    dups.iloc[escape, col]
    + pd.Timedelta(hours=dup["dedup_window_hours"])
    + pd.Timedelta(hours=1)
)

events = pd.concat([events, dups], ignore_index=True)

# ----------------------------
# Phase 2: streaming write (expand heavy columns per file)
# ----------------------------
# Partition = event time, file = processing time. event_date directory from
# event_timestamp; within it one file per (producer, 5-min flush window) keyed on
# ingestion_timestamp. A late event lands in an old event_date via a new flush.
# DDIA Ch 11. Both grouping keys stay as cheap datetime64, never object strings.
flush = f"{small['flush_minutes']}min"
events["event_day"] = events["event_timestamp"].dt.floor("D")
events["flush_window"] = events["ingestion_timestamp"].dt.floor(flush)

out = REPO_ROOT / "data" / "landing"
if out.exists():
    shutil.rmtree(out)  # clean stale runs so file counts stay verifiable

def expand(group):
    # Build the wide string columns for THIS file's rows only. event_type,
    # metadata JSON and stack_trace exist for a few hundred rows here, never for
    # all 50M at once. This function is pure: it consumes no rng.
    g = group.reset_index(drop=True)
    n = len(g)

    event_type = EVENT_TYPES[g["event_type_code"].to_numpy()].astype(object)
    is_crash = g["exc_code"].to_numpy() >= 0
    event_type[is_crash] = "crash"  # a crash overrides whatever type was drawn

    # Closed vocabulary, no quotes to escape, so we interpolate the JSON string
    # vectorized instead of a json.dumps call per row.
    dev = pd.Series(DEVICES[g["device_code"].to_numpy()])
    net = pd.Series(NETWORKS[g["network_code"].to_numpy()])
    before = '{"device": "' + dev + '", "app_version": "' + drift["version_before"] + '"}'
    after = (
        '{"device": "' + dev + '", "app_version": "' + drift["version_after"]
        + '", "' + drift["new_key"] + '": "' + net + '"}'
    )
    metadata = np.where(g["is_new_schema"].to_numpy(), after, before)

    stack_trace = np.full(n, None, dtype=object)
    stack_trace[is_crash] = TRACES[g["exc_code"].to_numpy()[is_crash]]

    # Screenshot reference columns default to null; a valid shot_idx pulls the
    # pointer (path, size, type, hash) from the small shot table.
    si = g["shot_idx"].to_numpy()
    has = si >= 0
    screenshot_path = np.full(n, None, dtype=object)
    screenshot_bytes = pd.array([pd.NA] * n, dtype="Int64")  # nullable int, real column
    screenshot_content_type = np.full(n, None, dtype=object)
    screenshot_sha256 = np.full(n, None, dtype=object)
    if has.any():
        screenshot_path[has] = shot_table["path"][si[has]]
        screenshot_bytes[has] = shot_table["bytes"][si[has]]
        screenshot_content_type[has] = shot_table["ctype"][si[has]]
        screenshot_sha256[has] = shot_table["sha256"][si[has]]

    # Widen the compact ints back to int64 so the on-disk schema is unchanged.
    out = pd.DataFrame(
        {
            "event_id": g["event_id"],
            "player_id": g["player_id"].astype(np.int64),
            "game_id": g["game_id"].astype(np.int64),
            "event_type": event_type,
            "event_timestamp": g["event_timestamp"],  # tz-aware preserved
            "ingestion_timestamp": g["ingestion_timestamp"],
            "producer_id": g["producer_id"].astype(np.int64),
            "source_sequence_number": g["source_sequence_number"],
            "metadata": metadata,
            "stack_trace": stack_trace,
            "screenshot_path": screenshot_path,
            "screenshot_bytes": screenshot_bytes,
            "screenshot_content_type": screenshot_content_type,
            "screenshot_sha256": screenshot_sha256,
        }
    )
    # Producer owns schema stability: force a string dtype on the nullable
    # text columns so a batch with no crashes/screenshots writes them as
    # parquet STRING, not a null/int type. DDIA Ch 4.
    for col in ("stack_trace", "screenshot_path", "screenshot_content_type", "screenshot_sha256"):
        out[col] = out[col].astype("string")
    return out

n_files = 0
# One file per (event_date, producer, flush window). At 50M nearly every cell
# fills, plus late arrivals open extra cells in already-passed partitions.
# Deliberately many tiny files: compaction is a W3 platform capability, not the
# producer's job.
for (event_day, producer, flush_window), group in events.groupby(
    ["event_day", "producer_id", "flush_window"], sort=False
):
    event_date = pd.Timestamp(event_day).strftime("%Y-%m-%d")  # format once per file
    part_dir = out / f"event_date={event_date}"  # single-level Hive partition
    part_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp(flush_window).strftime("%Y%m%dT%H%M")
    fname = f"part-p{producer}-{stamp}.parquet"
    expand(group).to_parquet(part_dir / fname, index=False)
    n_files += 1

print("files written:", n_files)

# ----------------------------
# Inspect
# ----------------------------
# Crashes split across text in-table and binary out-of-table. Rows referencing a
# screenshot exceed files on disk: duplicate crashes point at the same blob, one
# file, many references. That is the reference pattern.
n_crash = int((events["exc_code"] >= 0).sum())
n_shot_rows = int((events["shot_idx"] >= 0).sum())
n_shot_files = len(list((REPO_ROOT / "data" / "screenshots").glob("*.png")))
print("total rows:", len(events))
print("crash rows (stack_trace in-table):", n_crash)
print("rows referencing a screenshot:", n_shot_rows)
print("screenshot files on disk:", n_shot_files)
# %%