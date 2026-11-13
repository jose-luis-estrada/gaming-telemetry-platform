# Architecture Decision Records

Each record captures one decision: the context that forced it, what was decided,
the alternative that lost and why, and the consequences accepted. Records are
immutable once accepted. A decision is changed by writing a new record that
supersedes the old one, never by editing it in place.

| # | Decision | Status |
|---|---|---|
| [0001](0001-config-driven-source-contracts.md) | Sources are onboarded through YAML contracts, validated fail-loud | Accepted |
| [0002](0002-environment-seam.md) | Deployment differences live behind one Environment seam | Accepted |
| [0003](0003-event-time-partitioning.md) | Partition on event time; dataset shape and landing layout | Accepted |
| [0004](0004-bronze-contract.md) | Bronze is as-landed, schema-on-read, append-only, with lineage | Accepted |
| [0005](0005-bronze-idempotency.md) | Bronze idempotency through incremental-ingest state, not overwrite | Accepted, local gap open |
| [0006](0006-bounded-dedup.md) | Bounded dedup ordered by producer sequence, never wall clock | Accepted |
| [0007](0007-quality-rules-and-quarantine.md) | Hard and soft quality rules; row failures quarantined | Accepted |
| [0008](0008-late-data-horizon.md) | 48h late-data horizon, partition-scoped rewrite, late_after_close | Accepted |
| [0009](0009-gold-scope.md) | Three Gold tables; config-driven design stops at Gold | Accepted |
| [0010](0010-skew-salting.md) | Resolve key skew with salting; AQE does not apply at this scale | Accepted |
| [0011](0011-compaction.md) | Compaction is a platform capability | Accepted |
| [0012](0012-landing-is-system-of-record.md) | Landing is the system of record; derived data is never copied out | Accepted |
| [0013](0013-retire-databricks-environment.md) | Retire the Databricks Free Edition environment | Accepted |
| [0014](0014-self-hosted-lakehouse-on-kind.md) | Self-hosted lakehouse on KIND with Airflow and dbt | Accepted, in progress |
