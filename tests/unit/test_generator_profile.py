"""Generators honor TELEMETRY_MANIFEST and TELEMETRY_DATA_ROOT.

Runs both real generators (events, purchases) in subprocesses (they are
module-level scripts, not functions) against a tiny copy of the dev manifest,
writing under tmp_path.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("profile")

    # Derive from the dev manifest so the test tracks it. Tiny row count, and a
    # one-day flush window so the run writes a few dozen files instead of
    # thousands: this test is about paths and the drift cutover, not small files.
    manifest = json.loads((REPO_ROOT / "config" / "manifest.dev.json").read_text())
    manifest["rows"] = 20_000
    manifest["defects"]["small_files"]["flush_minutes"] = 1440
    manifest_path = tmp / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    # Absolute paths on purpose: exercises the "absolute values are used as is" rule.
    data_root = tmp / "data"
    env = {
        **os.environ,
        "TELEMETRY_MANIFEST": str(manifest_path),
        "TELEMETRY_DATA_ROOT": str(data_root),
    }
    # Both generators, same env: proves one TELEMETRY_MANIFEST drives both
    # sources. purchases keeps its own volume (7 x 50k rows here), still fast.
    for module in ("src.generator.events", "src.generator.purchases"):
        subprocess.run(
            [sys.executable, "-m", module],
            cwd=REPO_ROOT,
            env=env,
            check=True,
            capture_output=True,
        )
    return manifest, data_root


def test_landing_written_under_data_root(generated):
    manifest, data_root = generated
    landing = data_root / "landing" / "player_events"
    partitions = [p for p in landing.iterdir() if p.name.startswith("event_date=")]
    assert len(partitions) == manifest["event_window"]["days"]


def test_screenshots_written_under_data_root(generated):
    _, data_root = generated
    assert any((data_root / "screenshots").glob("*.png"))


def test_drift_cutover_lands_on_drift_day(generated):
    manifest, data_root = generated
    df = pd.read_parquet(
        data_root / "landing" / "player_events",
        columns=["event_timestamp", "metadata"],
    )
    has_key = df["metadata"].str.contains('"network_type"', regex=False)

    window = manifest["event_window"]
    drift_day = manifest["defects"]["schema_drift"]["drift_day"]
    cutover = pd.Timestamp(window["start_utc"]) + pd.Timedelta(days=drift_day)

    assert df.loc[has_key, "event_timestamp"].min() >= cutover
    assert df.loc[~has_key, "event_timestamp"].max() < cutover


def _partitions(landing):
    return sorted(p.name for p in landing.iterdir() if p.name.startswith("event_date="))


def test_purchases_follow_event_window(generated):
    # Same partitions as player_events: Gold joins revenue to events by date,
    # so a source on its own calendar would silently produce empty joins.
    _, data_root = generated
    landing = data_root / "landing"
    assert _partitions(landing / "purchases") == _partitions(landing / "player_events")


def test_purchase_timestamps_match_their_partition(generated):
    # The path says which day a row belongs to; the timestamp must agree, or
    # partition pruning would return the wrong rows.
    _, data_root = generated
    df = pd.read_parquet(data_root / "landing" / "purchases")
    ts_date = df["purchase_timestamp"].dt.strftime("%Y-%m-%d")
    assert (ts_date == df["event_date"].astype(str)).all()
