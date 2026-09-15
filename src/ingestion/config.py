# %%
import yaml
from dataclasses import dataclass, field
from pathlib import Path

# ----------------------------
# Source contract
# ----------------------------
# This is the public interface of the platform. A source is onboarded by writing
# one of these as YAML, never by editing framework code. Fields split in two:
# what Bronze acts on NOW (format, landing_path, bronze_table, read_options) and
# what is declared now but enforced downstream (schema in Silver, dedup_key in
# W5, quality_rules in W4). Declaring early keeps the contract in one file; the
# framework simply ignores the downstream fields until those weeks.
SUPPORTED_FORMATS = {"parquet", "csv", "json"}

@dataclass
class SourceConfig:
    name: str
    format: str
    landing_path: str
    bronze_table: str
    checkpoint_path: str    # Stable per-(source,target) dir; env resolves the root.
                            # Ignored locally (batch), THE idempotency mechanism on cloud.
    read_options: dict = field(default_factory=dict)
    # Declared, not acted on in Bronze. Schema-on-read: Bronze never enforces
    # these, so a source can drift without the ingest breaking. Silver's job. DDIA Ch 4.
    schema: list = field(default_factory=list)
    # W5 dedup contract. dedup is TWO questions, so TWO fields:
    #   identity_key: which rows are the same logical record (collapse these)
    #   ordering_key: which duplicate wins (keep the max on this key)
    # Kept separate because they mean different things per source: for
    # player_events the winner is a producer sequence, never a clock (3 producers,
    # 3 clocks, DDIA Ch 8); for purchases one producer makes the wall clock a
    # valid order. One dedup_key field hid that difference.
    identity_key: list = field(default_factory=list)
    ordering_key: list = field(default_factory=list)
    # Bounded dedup window in hours. None = unbounded (fine for a source with no
    # duplicates). For player_events it bounds reconciliation to recent history
    # instead of reshuffling all of it every run. DDIA Ch 3.
    dedup_window_hours: int | None = None
    quality_rules: list = field(default_factory=list)  # used in W4

def load_source_config(path: str | Path) -> SourceConfig:
    raw = yaml.safe_load(Path(path).read_text())
    # Fail loud on a broken contract instead of ingesting garbage silently: an
    # unnamed source or an unknown format is an author error, not a runtime guess.
    required = ["name", "format", "landing_path", "bronze_table", "checkpoint_path"]
    missing = [k for k in required if not raw.get(k)]
    if missing:
        raise ValueError(f"{path}: source config missing required fields: {missing}")
    if raw["format"] not in SUPPORTED_FORMATS:
        raise ValueError(f"{path}: format '{raw['format']}' not in {SUPPORTED_FORMATS}")
    # Only pass keys the dataclass knows, so a typo'd field surfaces as an error.
    known = {f for f in SourceConfig.__dataclass_fields__}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"{path}: unknown config fields: {unknown}")
    return SourceConfig(**raw)