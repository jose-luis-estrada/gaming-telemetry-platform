# 0011. Compaction is a platform capability

Status: Accepted

## Context

Producers flush every 5 minutes, producing 102,056 files of about 200 KB. With
the 4 MB `spark.sql.files.openCostInBytes` floor, reading the landing spawns
3,190 tasks for 2.1 GB.

## Decision

Compaction is delivered by the platform, not by producers: `optimize_source`
runs Delta OPTIMIZE on every source's Bronze and Silver tables as a pipeline
stage, with optional per-source Z-ORDER columns (`zorder_by`). Before and after
file counts are read from DESCRIBE DETAIL.

## Consequences

- The small-files cost is born in the landing. When the writer already packs
  output (as the previous managed environment did), OPTIMIZE on Bronze moves few
  files; the measurement that matters is landing file count and read task count
  before and after.
