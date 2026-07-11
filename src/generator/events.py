# %%
import json
from pathlib import Path

import pandas as pd
import numpy as np

# ----------------------------
# Load manifest
# ----------------------------
# The manifest is the spec: written before generation, single source of truth.
# The generator reads from it and holds no distribution numbers of its own.
MANIFEST_PATH = Path("config/manifest.json") # assumes CWD is the repo root
manifest = json.loads(MANIFEST_PATH.read_text())

seed = manifest["seed"]
n_rows = manifest["rows"]
skew = manifest["defects"]["skew"]

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
    "event_timestamp": pd.date_range(start="2026-01-01", periods=n_rows, freq="min")
})

# ----------------------------
# Inspect
# ----------------------------

print(events)
print(events.dtypes)
print(events["game_id"].value_counts(normalize=True))
# %%
