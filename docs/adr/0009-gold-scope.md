# 0009. Three Gold tables; config-driven design stops at Gold

Status: Accepted

## Decision

- Exactly three tables, each with a declared grain and a distinct reason to
  exist: `player_daily` (player x event_date, engagement), `game_health_daily`
  (game_id x event_date, crash rate) and `revenue_daily` (item_category x
  purchase_date, from purchases, the clean baseline with no late data).
- Gold is bespoke business logic in pure functions. Bronze and Silver are
  config-driven because ingestion and dedup have the same shape for every source;
  aggregates do not.
- Gold is partitioned on `event_date` to scope late-data rewrites, not for scan
  pruning (about 19 rows per partition).

## Rejected alternatives

- **A generic aggregate DSL in YAML.** No reuse across three tables, and each
  metric becomes harder to reason about.

## Consequences

- Every run asserts grain uniqueness (row count equals distinct grain keys).
- Full-scale row counts: 570 / 120 / 20.
