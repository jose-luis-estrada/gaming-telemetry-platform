"""Unit tests for Silver dedup. Small hand-built frames, no Spark cluster needed
beyond local: each test targets ONE seeded duplicate flavor so a failure names
the exact behavior that broke."""
import pytest
from pyspark.sql import SparkSession

from src.ingestion.config import SourceConfig
from src.ingestion.silver import deduplicate, build_silver


@pytest.fixture(scope="module")
def spark():
    # Minimal local session. No Delta extension: these tests operate on in-memory
    # DataFrames, they never touch storage.
    s = SparkSession.builder.master("local[1]").appName("test-silver").getOrCreate()
    yield s
    s.stop()


@pytest.fixture
def cfg():
    # Mirrors player_events.yaml: identity by event_id, order by producer sequence.
    return SourceConfig(
        name="player_events", format="parquet", landing_path="player_events",
        bronze_table="bronze.player_events", checkpoint_path="player_events",
        identity_key=["event_id"],
        ordering_key=["producer_id", "source_sequence_number"],
        dedup_window_hours=72,
    )


def _rows(spark, data):
    # cols match the dedup contract; ingestion_timestamp drives the window split.
    return spark.createDataFrame(
        data,
        "event_id long, producer_id long, source_sequence_number long, "
        "payload string, ingestion_timestamp timestamp",
    )


def test_byte_identical_retry_collapses(spark, cfg):
    # Two identical rows for one event_id must become one.
    from datetime import datetime
    t = datetime(2026, 1, 15, 12, 0, 0)
    df = _rows(spark, [
        (1, 1, 100, "a", t),
        (1, 1, 100, "a", t),  # exact retry
    ])
    out = deduplicate(df, cfg)
    assert out.count() == 1  # collapsed to a single winner


def test_correction_beats_original(spark, cfg):
    # Same event_id, higher sequence, different payload: the correction wins.
    from datetime import datetime
    t = datetime(2026, 1, 15, 12, 0, 0)
    df = _rows(spark, [
        (1, 1, 100, "original", t),
        (1, 1, 200, "corrected", t),  # higher source_sequence_number
    ])
    out = deduplicate(df, cfg).collect()
    assert len(out) == 1
    assert out[0]["payload"] == "corrected"  # ordering_key picked the correction


def test_out_of_window_duplicate_survives(spark, cfg):
    # A duplicate that lands past the 72h window is NOT reconciled: it survives,
    # making the bounded guarantee visible. This is the load-bearing W5 test.
    from datetime import datetime
    df = _rows(spark, [
        (1, 1, 100, "a", datetime(2026, 1, 15, 12, 0, 0)),
        (1, 1, 200, "a", datetime(2026, 1, 12, 11, 0, 0)),  # >72h older, escapes
    ])
    out = build_silver(df, cfg)
    # Both survive: one wins in-window dedup, the other is out-of-window untouched.
    assert out.count() == 2


def test_distinct_events_both_kept(spark, cfg):
    # Sanity: dedup must not collapse DIFFERENT events. Two identities, two rows.
    from datetime import datetime
    t = datetime(2026, 1, 15, 12, 0, 0)
    df = _rows(spark, [(1, 1, 100, "a", t), (2, 1, 100, "b", t)])
    assert deduplicate(df, cfg).count() == 2