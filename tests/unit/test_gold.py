"""Gold builders and the late-data horizon split."""

from datetime import date, datetime, timedelta

import pytest

from src.ingestion.gold import (
    HORIZON_HOURS,
    build_game_health_daily,
    build_player_daily,
    past_horizon,
    within_horizon,
)

T0 = datetime(2026, 1, 16, 12, 0, 0)
DAY = date(2026, 1, 16)


@pytest.fixture
def events(spark):
    rows = [
        # (event_date, player_id, game_id, event_type, event_ts, ingestion_ts)
        (DAY, 1, 1, "login", T0, T0 + timedelta(seconds=30)),
        (DAY, 1, 2, "crash", T0, T0 + timedelta(hours=10)),
        (DAY, 2, 1, "login", T0, T0 + timedelta(hours=HORIZON_HOURS)),
        (DAY, 2, 1, "crash", T0, T0 + timedelta(hours=HORIZON_HOURS + 1)),
    ]
    return spark.createDataFrame(
        rows,
        "event_date date, player_id long, game_id long, event_type string, "
        "event_timestamp timestamp, ingestion_timestamp timestamp",
    )


def test_horizon_split_is_a_partition(events):
    inside, outside = within_horizon(events), past_horizon(events)
    assert inside.count() + outside.count() == events.count()
    # Exactly at the horizon is still inside; one hour past is outside.
    assert inside.count() == 3
    assert outside.count() == 1


def test_past_horizon_keeps_lateness_as_evidence(events):
    [row] = past_horizon(events).collect()
    assert row["_lateness_hours"] == pytest.approx(HORIZON_HOURS + 1)


def test_player_daily_grain_and_counts(events):
    rows = {r["player_id"]: r for r in build_player_daily(within_horizon(events)).collect()}
    assert set(rows) == {1, 2}
    assert rows[1]["total_events"] == 2
    assert rows[1]["games_played"] == 2
    assert rows[1]["crash_count"] == 1
    assert rows[2]["login_count"] == 1


def test_game_health_crash_rate(events):
    rows = {r["game_id"]: r for r in build_game_health_daily(events).collect()}
    assert rows[1]["total_events"] == 3
    assert rows[1]["crash_rate"] == pytest.approx(1 / 3)
