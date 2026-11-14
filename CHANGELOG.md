# Changelog

Notable changes to the platform. Design rationale lives in
[`docs/adr/`](docs/adr/README.md).

## [Unreleased]

### Changed
- Databricks Free Edition environment and notebooks removed from `main`
  (preserved at `v1-databricks`, ADR-0013).
- Environment storage roots come from `TELEMETRY_DATA_ROOT`; tables are
  addressed by layer through `Environment.table_uri`.
- `make pipeline` now runs ingest, quality gate, Silver, Gold and compaction.
- `run_gold` only builds Gold and checks invariants; the late-data reprocess
  check moved to `scripts/verify_late_data.py`.
- Dependencies and tooling moved to `pyproject.toml`; ruff, pre-commit and
  GitHub Actions CI added; Docker image runs as a non-root user.
- Project history consolidated into ADRs; the dated work log is preserved at
  `v1-databricks` in `PROGRESS.md`.
- Generators write landing data and screenshots under `TELEMETRY_DATA_ROOT`,
  the same root the pipeline reads, so dataset profiles never overwrite each
  other. `verify-repro` hashes the landing under that root.
- Hard rule `schema_drift_before_boundary` replaced by
  `schema_drift_clean_cutover`: no old-schema event may follow the first
  drift-key event. No hardcoded date, so one contract serves every profile.

### Fixed
- `purchases` quality rules were in an unsupported shape and never executed.
  The loader now validates every rule and fails on anything the engine would
  skip.
- The loader treated an empty string as a missing field.
- Quarantined rows now carry the `_run_id` of the run that rejected them.
- Screenshots were written to a hardcoded `data/screenshots`, so generating a
  second profile deleted the full dataset's screenshots.
- The drift rule's boundary (`2026-01-15`) was one day before the actual
  cutover (`drift_day` is an offset from 2026-01-01), leaving a day unchecked.

### Added
- Quality gate entrypoint writing a quarantine table per source.
- Compaction entrypoint for Bronze and Silver.
- Unit tests for the config loader, quality engine, Gold builders and
  environment addressing (4 to 37 tests).
- Dev dataset profile (`config/manifest.dev.json`: 5M rows, 7 days, drift on
  day 3), selected with `TELEMETRY_MANIFEST`. `make generate-dev`,
  `verify-repro-dev` and `pipeline-dev` run it under `data/dev`.
- Generator test covering data-root isolation and the drift cutover day.

## [0.1.0] - 2026-10-06 (`v1-databricks`)

- Deterministic generator: 50M events over 30 days with five seeded defects
  recorded in `config/manifest.json`; bit-identical output for a fixed seed.
- Config-driven Bronze ingestion for two sources with lineage columns.
- Incremental ingestion with Autoloader on Databricks; held-back partition
  picked up alone.
- Quality framework with hard and soft rules and row-level quarantine.
- Bounded Silver dedup (50,000,005 rows: 50M distinct plus 5 out-of-window).
- Skew observed and resolved with salting; broadcast and sort-merge joins
  confirmed in plans.
- Three Gold tables with a 48h late-data horizon, partition-scoped
  reprocessing and `late_after_close`; Unity Catalog lineage end to end.
