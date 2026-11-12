"""Source contract: the public interface of the platform.

A source is onboarded by writing one YAML file in ``config/sources/``, never by
editing framework code. Fields split by when they act: ``format``,
``landing_path`` and ``bronze_table`` act at ingestion, while ``schema``,
``identity_key``, ``ordering_key`` and ``quality_rules`` are declared up front
and enforced downstream.

The loader fails loud. A missing field, an unknown format, a typo'd key or a
malformed quality rule raises at load time instead of being silently ignored at
run time.
"""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

SUPPORTED_FORMATS = {"parquet", "csv", "json"}

# Closed vocabularies for quality rules. quality.py implements exactly these;
# tests/unit/test_config.py asserts the two stay in sync.
ROW_CHECKS = {"not_null"}
ASSERT_OPS = {"eq", "le", "ge"}
SEVERITIES = {"hard", "soft"}

# Required fields that must be non-empty strings.
_REQUIRED_NON_EMPTY = ("name", "format", "bronze_table")
# Required fields where an empty string is a legal value (it means "the root").
_REQUIRED_PRESENT = ("landing_path", "checkpoint_path")


@dataclass
class SourceConfig:
    name: str
    format: str
    landing_path: str
    bronze_table: str
    # Stable per-(source, target) location for incremental-ingest state. Losing
    # it makes every landed file look new, so it is versioned, not derived.
    checkpoint_path: str
    read_options: dict = field(default_factory=dict)
    # Declared contract. Bronze is schema-on-read and does not enforce it, so a
    # drifted source lands instead of failing the ingest. Silver enforces it.
    schema: list = field(default_factory=list)
    # Dedup is two questions, so two fields:
    #   identity_key: which rows are the same logical record (collapse these)
    #   ordering_key: which duplicate wins (keep the max on this key)
    identity_key: list = field(default_factory=list)
    ordering_key: list = field(default_factory=list)
    # Bounded dedup window in hours. None means unbounded.
    dedup_window_hours: int | None = None
    # Optional Z-ORDER columns for compaction. Empty means bin-packing only.
    zorder_by: list = field(default_factory=list)
    quality_rules: list = field(default_factory=list)


def load_source_config(path: str | Path) -> SourceConfig:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: source config must be a mapping")

    missing = [k for k in _REQUIRED_PRESENT if raw.get(k) is None]
    missing += [k for k in _REQUIRED_NON_EMPTY if not _non_empty_str(raw.get(k))]
    if missing:
        raise ValueError(f"{path}: source config missing required fields: {sorted(missing)}")

    if raw["format"] not in SUPPORTED_FORMATS:
        raise ValueError(f"{path}: format {raw['format']!r} not in {sorted(SUPPORTED_FORMATS)}")

    known = set(SourceConfig.__dataclass_fields__)
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"{path}: unknown config fields: {sorted(unknown)}")

    _validate_quality_rules(path, raw.get("quality_rules") or [])
    return SourceConfig(**raw)


def _validate_quality_rules(path, rules: list) -> None:
    """Reject any rule the quality engine would not execute.

    A rule that does not match a known shape used to be skipped silently, so a
    source could ship with zero active checks and every run would pass.
    """
    if not isinstance(rules, list):
        raise ValueError(f"{path}: quality_rules must be a list")

    seen = set()
    for i, rule in enumerate(rules):
        where = f"{path}: quality_rules[{i}]"
        if not isinstance(rule, dict):
            raise ValueError(f"{where}: must be a mapping")

        name = rule.get("name")
        if not _non_empty_str(name):
            raise ValueError(f"{where}: missing 'name'")
        if name in seen:
            raise ValueError(f"{where}: duplicate rule name {name!r}")
        seen.add(name)
        where = f"{path}: rule {name!r}"

        kind = rule.get("type")
        if kind == "row":
            _require(where, rule, {"name", "type", "column", "check"})
            if rule["check"] not in ROW_CHECKS:
                raise ValueError(f"{where}: check {rule['check']!r} not in {sorted(ROW_CHECKS)}")
        elif kind == "aggregate":
            severity = rule.get("severity")
            if severity not in SEVERITIES:
                raise ValueError(f"{where}: severity {severity!r} not in {sorted(SEVERITIES)}")
            if severity == "hard":
                _require(where, rule, {"name", "type", "severity", "query", "assert"})
                assertion = rule["assert"]
                if not isinstance(assertion, dict) or set(assertion) != {"op", "value"}:
                    raise ValueError(f"{where}: 'assert' must have exactly 'op' and 'value'")
                if assertion["op"] not in ASSERT_OPS:
                    raise ValueError(f"{where}: op {assertion['op']!r} not in {sorted(ASSERT_OPS)}")
            else:
                _require(where, rule, {"name", "type", "severity", "query", "expected", "tolerance"})
        else:
            raise ValueError(f"{where}: type {kind!r} must be 'row' or 'aggregate'")


def _require(where: str, rule: dict, expected: set) -> None:
    missing = expected - set(rule)
    extra = set(rule) - expected
    if missing:
        raise ValueError(f"{where}: missing keys {sorted(missing)}")
    if extra:
        raise ValueError(f"{where}: unknown keys {sorted(extra)}")


def _non_empty_str(value) -> bool:
    return isinstance(value, str) and value.strip() != ""
