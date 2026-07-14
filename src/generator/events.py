# %%
import json
from pathlib import Path

import pandas as pd
import numpy as np

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

events = pd.DataFrame({
    # Sequential ints: reproducible event_id across runs.
    "event_id": range(1, n_rows + 1),
    "player_id": rng.integers(1, 20, size=n_rows),
    # Skew lives in the key distribution, driven by the manifest weights.
    "game_id": rng.choice(skew["game_ids"], size=n_rows, p=weights),
    "event_type": rng.choice(
        ["login", "logout", "purchase", "match_start", "match_end"],
        size=n_rows
    ),
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
# event_timestamp (when it happend). Event time vs processing time, DDIA Ch 11.
# Drawn AFTER the DataFrame so the earlier columns keep their rng stream.
late = manifest["defects"]["late_arrivals"]
normal_max = late["normal_max_delay_seconds"]
max_late_s = late["max_lateness_hours"] * 3600

is_late = rng.random(n_rows) < late["late_fraction"]
normal_delay = rng.integers(0, normal_max, size=n_rows)
# Late delays start where normal ends, so a single threshold separates the two
# populations with no overlap and the late fraction stays verificable.
late_delay = rng.integers(normal_max, max_late_s, size=n_rows)
delay_seconds = np.where(is_late, late_delay, normal_delay)

# event_timestamp still drives the partition, not this. A late event lands in a
# partition whose event_date already passed. That is the whole defect.
events["ingestion_timestamp"] = events["event_timestamp"] + pd.to_timedelta(
    delay_seconds, unit="s"
)

# ----------------------------
# Inspect
# ----------------------------

# W1 exit criterion: late fraction and max lateness match the manifest.
delay = (events["ingestion_timestamp"] - events["event_timestamp"]).dt.total_seconds()
print("late rate:", (delay >= normal_max).mean())
print("max lateness hours:", delay.max() / 3600)
# %%
