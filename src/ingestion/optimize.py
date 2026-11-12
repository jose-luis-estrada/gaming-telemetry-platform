"""Delta compaction as a platform capability.

Every source gets compaction from its config alone: no source-specific code.
"""

from src.ingestion.config import SourceConfig
from src.ingestion.environment import Environment


def optimize_source(env: Environment, cfg: SourceConfig, layer: str = "bronze"):
    """Compact one source's table and return (before, after, metrics).

    ``before`` and ``after`` are rows of DESCRIBE DETAIL (numFiles, sizeInBytes),
    read from the transaction log rather than the data, so they are cheap and
    exact. numFiles is the small-files cost being reduced.
    """
    table = env.delta_table(cfg, env.spark(), layer)
    before = table.detail().select("numFiles", "sizeInBytes").first()

    builder = table.optimize()
    if cfg.zorder_by:
        # Z-ORDER co-locates rows with similar values of the given columns so a
        # filtered read can skip whole files. Opt-in per source: it is a decision
        # about that source's read pattern, not a compaction default.
        result = builder.executeZOrderBy(*cfg.zorder_by)
    else:
        # Bin-packing: merge small files into larger ones.
        result = builder.executeCompaction()

    after = table.detail().select("numFiles", "sizeInBytes").first()
    return before, after, result
