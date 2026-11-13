"""Quality engine: hard rules fail loud, soft rules report, quarantine drops nothing."""

import pytest

from src.ingestion.quality import QualityError, run_quality_checks, split_quarantine

ROW_RULES = [
    {"name": "id_not_null", "type": "row", "column": "id", "check": "not_null"},
    {"name": "ts_not_null", "type": "row", "column": "ts", "check": "not_null"},
]


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [(1, "2026-01-01"), (None, "2026-01-01"), (3, None), (None, None)],
        "id long, ts string",
    )


def test_quarantine_keeps_every_row(df):
    clean, rejects = split_quarantine(df, ROW_RULES, run_id="run-1")
    assert clean.count() + rejects.count() == df.count()
    assert clean.count() == 1


def test_quarantine_tags_first_violated_rule_and_run(df):
    _, rejects = split_quarantine(df, ROW_RULES, run_id="run-1")
    tags = sorted(r["_reject_rule"] for r in rejects.collect())
    # The row failing both rules reports one reason, not two rows.
    assert tags == ["id_not_null", "id_not_null", "ts_not_null"]
    assert {r["_run_id"] for r in rejects.collect()} == {"run-1"}


def test_no_row_rules_means_empty_rejects_with_context_columns(df):
    clean, rejects = split_quarantine(df, [], run_id="run-1")
    assert clean.count() == df.count()
    assert rejects.count() == 0
    assert {"_reject_rule", "_rejected_at", "_run_id"} <= set(rejects.columns)


def test_hard_rule_failure_raises(df):
    rule = {
        "name": "no_null_ids",
        "type": "aggregate",
        "severity": "hard",
        "query": "SELECT COUNT(*) AS observed FROM bronze WHERE id IS NULL",
        "assert": {"op": "eq", "value": 0},
    }
    with pytest.raises(QualityError, match="no_null_ids"):
        run_quality_checks(df, [rule])


def test_soft_rule_reports_without_raising(df):
    rule = {
        "name": "null_id_share",
        "type": "aggregate",
        "severity": "soft",
        "query": "SELECT AVG(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS observed FROM bronze",
        "expected": 0.1,
        "tolerance": 0.05,
    }
    [result] = run_quality_checks(df, [rule])
    assert result.observed == pytest.approx(0.5)
    assert result.passed is False
