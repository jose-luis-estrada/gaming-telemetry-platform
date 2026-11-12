"""Environment seam: addressing and resolution. No SparkSession is built."""

import pytest

from src.ingestion.config import SourceConfig
from src.ingestion.environment import LocalEnvironment, get_environment


def cfg(landing_path="player_events"):
    return SourceConfig(
        name="player_events",
        format="parquet",
        landing_path=landing_path,
        bronze_table="bronze.player_events",
        checkpoint_path="player_events",
    )


def test_local_paths_follow_data_root(monkeypatch):
    monkeypatch.setenv("TELEMETRY_DATA_ROOT", "/mnt/lake/")
    env = LocalEnvironment()
    assert env.landing_uri(cfg()) == "/mnt/lake/landing/player_events"
    assert env.checkpoint_uri(cfg()) == "/mnt/lake/checkpoints/player_events"
    assert env.table_uri("gold", "player_daily") == "/mnt/lake/gold/player_daily"
    assert env.table_uri("quarantine", "purchases") == "/mnt/lake/quarantine/purchases"


@pytest.mark.parametrize("rel", ["", ".", "/"])
def test_empty_landing_path_means_root(rel):
    assert LocalEnvironment("data").landing_uri(cfg(rel)) == "data/landing"


def test_unknown_layer_fails():
    with pytest.raises(ValueError, match="unknown layer"):
        LocalEnvironment("data").table_uri("platinum", "x")


def test_get_environment(monkeypatch):
    monkeypatch.delenv("TELEMETRY_ENV", raising=False)
    assert isinstance(get_environment(), LocalEnvironment)
    monkeypatch.setenv("TELEMETRY_ENV", "databricks")
    with pytest.raises(ValueError, match="unknown TELEMETRY_ENV"):
        get_environment()
