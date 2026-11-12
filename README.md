# gaming-telemetry-platform

A config-driven lakehouse for gaming telemetry: ingestion, data quality, Silver
deduplication and Gold aggregates on Delta Lake. A new source is onboarded with
one YAML file and zero new framework code.

The platform is exercised against a deterministic 50M-event dataset with five
production failure modes seeded on purpose and recorded as ground truth before
any data exists. Every number the platform reports is reconciled against that
ground truth.

## Guarantees

| Layer | Guarantee |
|---|---|
| Landing | Immutable system of record. Every table downstream can be rebuilt from it. |
| Bronze | As landed, schema-on-read, partitioned by event date. Lineage on every row: `_source_name`, `_source_file`, `_ingested_at`, `_batch_id`. |
| Quarantine | Rows failing a row-level rule, with the rule, time and run that rejected them. Nothing is dropped silently. |
| Silver | One row per business key within a bounded dedup window, ordered by producer sequence, never by wall clock. |
| Gold | Three aggregates with declared grains. Late data within a 48h horizon corrects only the affected partitions; later arrivals are routed to `late_after_close`. |

Rerunning any stage for the same input is a no-op on row counts.

## Failure modes the platform must handle

| Defect | Seeded as | Handling |
|---|---|---|
| Skew | One `game_id` holds 37% of events | Observed in task metrics, resolved with salting ([ADR-0010](docs/adr/0010-skew-salting.md)) |
| Late arrivals | 2 to 3% of events arrive up to 72h late | Event-time partitions, 48h horizon, partition-scoped rewrite ([ADR-0008](docs/adr/0008-late-data-horizon.md)) |
| Duplicates | 1% repeat: byte-identical retries and same-key corrections | Bounded dedup on `(producer_id, source_sequence_number)` ([ADR-0006](docs/adr/0006-bounded-dedup.md)) |
| Schema drift | A key appears inside the `metadata` JSON from day 15 | Absorbed by schema-on-read Bronze, enforced by a hard quality rule ([ADR-0007](docs/adr/0007-quality-rules-and-quarantine.md)) |
| Small files | 102,056 files at ~200 KB | Compaction as a platform stage ([ADR-0011](docs/adr/0011-compaction.md)) |

Four of the five are silent: they produce plausible numbers with no failure
signal. Each is covered by a check that reconciles against
`config/manifest.json`.

## Architecture

```
landing (parquet, per-source subtree, event_date partitions)
   |  ingest      PySpark, config-driven, lineage stamped
   v
bronze (Delta) --quality gate--> quarantine (Delta)
   |  silver      bounded dedup per source contract
   v
silver (Delta)
   |  gold        pure builders, 48h horizon
   v
gold (Delta): player_daily, game_health_daily, revenue_daily, late_after_close
   |  optimize    Delta compaction, optional Z-ORDER
```

All deployment differences live behind one `Environment` seam
([ADR-0002](docs/adr/0002-environment-seam.md)). The current implementation runs
local PySpark against the filesystem. The target deployment is a self-hosted
lakehouse on Kubernetes (KIND) with SeaweedFS for S3-compatible storage, Airflow
for orchestration and dbt for Silver and Gold
([ADR-0014](docs/adr/0014-self-hosted-lakehouse-on-kind.md)).

## Onboarding a source

Add `config/sources/<name>.yaml`:

```yaml
name: purchases
format: parquet
landing_path: purchases
bronze_table: bronze.purchases
checkpoint_path: purchases
schema: [...]
identity_key: [purchase_id]
ordering_key: [purchase_timestamp]
quality_rules:
  - { name: purchase_id_not_null, type: row, column: purchase_id, check: not_null }
```

The loader fails at load time on missing fields, unknown keys, unknown formats
and any quality rule the engine would not execute
([ADR-0001](docs/adr/0001-config-driven-source-contracts.md)).

## Running locally

Requires Python 3.11 and Java 17.

```bash
make install        # pip install -e ".[dev]"
make generate       # build the dataset from config/manifest.json
make verify-repro   # prove the same seed produces a bit-identical dataset
make pipeline       # ingest -> quality -> silver -> gold -> optimize
make verify-late-data
make test
make lint
```

Data lives under `TELEMETRY_DATA_ROOT` (default `data/`, gitignored). The full
dataset needs about 7.5 GB of peak memory to generate and a 6 GB Spark driver
heap for Silver in local mode (`SPARK_DRIVER_MEMORY`).

Or in Docker:

```bash
docker build -t gaming-telemetry-platform .
docker run --rm -it -p 4040:4040 -v "$PWD/data:/app/data" gaming-telemetry-platform make pipeline
```

## Reference results (full scale)

| Check | Expected | Observed |
|---|---|---|
| Bronze rows | 50,500,000 (50M + 1% duplicates) | 50,500,000 |
| Silver rows | 50M distinct + 5 out-of-window duplicates | 50,000,005 |
| Drift key before drift day | 0 | 0 |
| Hot game share | 0.37 +/- 0.02 | 0.3700 |
| Late-arrival rate | 0.025 +/- 0.01 | 0.0250 |
| Gold within horizon + past horizon | 50,000,005 | 49,583,401 + 416,604 |
| Gold rows (player / game / revenue) | stable across runs | 570 / 120 / 20 |

## Repository layout

```
config/
  manifest.json        ground truth for the seeded defects, written before generation
  sources/*.yaml       one contract per source
src/
  generator/           deterministic, memory-bounded data generation
  ingestion/           config loader, environment seam, Bronze, quality, Silver, Gold
scripts/               operational verification scripts
tests/unit/            unit tests on small in-memory fixtures
docs/
  adr/                 architecture decision records
  incidents/           incident reports for the seeded defects
```

## History

The first implementation, on local PySpark plus Databricks Free Edition
(Autoloader, Unity Catalog), is preserved at tag
[`v1-databricks`](https://github.com/jose-luis-estrada/gaming-telemetry-platform/tree/v1-databricks). See [CHANGELOG.md](CHANGELOG.md).
