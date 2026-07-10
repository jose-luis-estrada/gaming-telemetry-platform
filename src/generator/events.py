# %%
import pandas as pd
import numpy as np

rng = np.random.default_rng(42)

events = pd.DataFrame({
    "event_id": range(1, 101),
    "player_id": rng.integers(1,20, size=100),
    "game_id": rng.choice(
        [1, 2, 3, 4], 
        size=100, 
        p=[0.37, 0.21, 0.21, 0.21]
    ),
    "event_type": rng.choice(
        ["login", "logout", "purchase", "match_start", "match_end"],
        size=100
    ),

    "event_timestamp": pd.date_range(
        start="2026-01-01",
        periods=100,
        freq="min"
    )
})

print(events)
print(events.dtypes)
events["game_id"].value_counts()
# %%
