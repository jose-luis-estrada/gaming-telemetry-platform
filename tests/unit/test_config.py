"""Source config loader: every shipped config loads, and broken ones fail loud."""

from pathlib import Path

import pytest
import yaml

from src.ingestion import quality
from src.ingestion.config import ASSERT_OPS, ROW_CHECKS, load_source_config

SOURCES = sorted(Path("config/sources").glob("*.yaml"))

BASE = {
    "name": "src",
    "format": "parquet",
    "landing_path": "src",
    "bronze_table": "bronze.src",
    "checkpoint_path": "src",
}


def write(tmp_path, data) -> Path:
    path = tmp_path / "source.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.stem)
def test_shipped_configs_load_with_active_rules(path):
    cfg = load_source_config(path)
    assert cfg.quality_rules, f"{path.stem} declares no quality rules"


def test_vocabularies_match_quality_engine():
    # The loader validates against these sets; the engine must implement them.
    assert set(quality._ROW_CHECKS) == ROW_CHECKS
    assert set(quality._OPS) == ASSERT_OPS


def test_empty_landing_path_is_legal(tmp_path):
    cfg = load_source_config(write(tmp_path, {**BASE, "landing_path": ""}))
    assert cfg.landing_path == ""


@pytest.mark.parametrize("field", ["name", "format", "bronze_table"])
def test_empty_required_string_fails(tmp_path, field):
    with pytest.raises(ValueError, match="missing required fields"):
        load_source_config(write(tmp_path, {**BASE, field: ""}))


def test_missing_key_fails(tmp_path):
    data = {k: v for k, v in BASE.items() if k != "checkpoint_path"}
    with pytest.raises(ValueError, match="checkpoint_path"):
        load_source_config(write(tmp_path, data))


def test_unknown_field_fails(tmp_path):
    with pytest.raises(ValueError, match="unknown config fields"):
        load_source_config(write(tmp_path, {**BASE, "dedup_key": ["id"]}))


def test_unknown_format_fails(tmp_path):
    with pytest.raises(ValueError, match="format"):
        load_source_config(write(tmp_path, {**BASE, "format": "avro"}))


@pytest.mark.parametrize(
    "rule, message",
    [
        # The pre-fix purchases shape: silently skipped by the engine.
        ({"column": "id", "rule": "not_null"}, "missing 'name'"),
        ({"name": "r", "column": "id", "rule": "not_null"}, "type"),
        ({"name": "r", "type": "row", "column": "id", "check": "unique"}, "check"),
        ({"name": "r", "type": "row", "column": "id"}, "missing keys"),
        (
            {"name": "r", "type": "aggregate", "severity": "medium", "query": "SELECT 1"},
            "severity",
        ),
        (
            {
                "name": "r",
                "type": "aggregate",
                "severity": "hard",
                "query": "SELECT 1",
                "assert": {"op": "ne", "value": 0},
            },
            "op",
        ),
        (
            {"name": "r", "type": "aggregate", "severity": "soft", "query": "SELECT 1"},
            "missing keys",
        ),
    ],
)
def test_malformed_rule_fails(tmp_path, rule, message):
    with pytest.raises(ValueError, match=message):
        load_source_config(write(tmp_path, {**BASE, "quality_rules": [rule]}))


def test_duplicate_rule_names_fail(tmp_path):
    rule = {"name": "r", "type": "row", "column": "id", "check": "not_null"}
    with pytest.raises(ValueError, match="duplicate"):
        load_source_config(write(tmp_path, {**BASE, "quality_rules": [rule, rule]}))
