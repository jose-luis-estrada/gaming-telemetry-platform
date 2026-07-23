# gaming-telemetry-platform

A config-driven ingestion and data quality framework, built on PySpark, Delta
Lake and Unity Catalog, and demonstrated on 50 million rows of synthetic gaming
telemetry.

The telemetry pipeline is the client of the platform, not the product. A new
source is onboarded by adding one YAML file, with zero new ingestion code.

## What this project is actually for

Most portfolio pipelines run clean on the first try, which means they answer no
questions. This one ships with five defects seeded on purpose, each one recorded
as ground truth in a manifest before any data is generated, and each one
verifiable against that manifest with a single query.

| Defect | How it is seeded | Why it is here |
|---|---|---|
| Skew | One `game_id` holds 37% of 50M events | Stragglers and shuffle partition imbalance. Fixed with salting and AQE. DDIA Ch 6. |
| Late arrivals | 2 to 3% of events arrive up to 72 hours late | Event time versus processing time. A late event lands in an already-closed partition. DDIA Ch 11. |
| Duplicates | 1% of events repeat, in two flavors | Byte-identical retries and same-key-different-payload corrections. Ordering cannot use a wall clock: three producers means three clocks. DDIA Ch 8. |
| Schema drift | A key appears inside a JSON blob from day 15 on | Bronze is schema-on-read, so drift lands instead of failing the job. Enforcement belongs downstream. |
| Small files | Producers flush every 5 minutes, 102,056 files at ~200 KB | The small-files tax, measured on four axes: compute, API calls, latency, disk slack. Collapsed with Delta OPTIMIZE. DDIA Ch 3. |

Four of these are silent. Skew announces itself in the Spark UI: slow, but
correct. Drift, late arrivals and duplicates produce plausible-looking numbers
with no failure signal at all, which is the harder and more interesting class of
bug.

Each defect gets a written postmortem: what was expected, what broke, how it was
diagnosed, what changed, and what would be different at 100x scale.

## The design thesis

A pipeline solves one movement problem. A platform onboards the next source
without new code.

The source contract lives entirely in `config/sources/*.yaml`. Fields are split
by when they bite: `format`, `landing_path` and `bronze_table` act at ingestion
time, while `schema`, `dedup_key` and `quality_rules` are declared up front but
enforced downstream. `run.py` is a loop over that directory, so source number
four is a config change rather than a code change.

The config loader fails loud. A missing required field, an unknown format, or a
typo'd key raises instead of ingesting garbage silently.

## Architecture

Landing (parquet) to Bronze (Delta) to Silver to Gold, with the layer boundaries
defined by guarantees rather than by transformations.

Bronze is as-landed and append-only. `metadata` stays a raw JSON string, nothing
is parsed, nothing is deduplicated. Every row carries lineage: `_source_name`,
`_source_file`, `_ingested_at`, `_batch_id`. That is the first thing on-call
reads at 3 AM, because it answers which file and which run.

The project runs in two environments on purpose, because neither one alone does
the job:

| Environment | Purpose | Limitation |
|---|---|---|
| Local PySpark in Docker | Spark internals. Readable DAG, stage boundaries, task counts, shuffle behavior, Spark UI at localhost:4040. Every scale measurement here was taken locally. | No Delta on cloud storage, no Unity Catalog, no Autoloader. |
| Databricks Free Edition | Delta, Unity Catalog, Autoloader, OPTIMIZE. | Serverless only, so there is no Spark UI, only the query profile. |

The landing zone is S3, synced with the AWS CLI under a dedicated IAM user.
Databricks Free Edition cannot read it: serverless egress to an arbitrary bucket
requires a network allowlist through an account-level API that Free Edition does
not expose. So the cloud landing that Databricks reads is a Unity Catalog Volume
holding a deliberate subset. Both landings are real, and the difference between
them is stated rather than hidden.

## Status

Built:

- **W1. Synthetic data generator.** 50M events across 30 days, five seeded
  defects recorded in a manifest, each verifiable with a single query.
  Bit-identical output for a fixed seed (`make verify-repro`, which generates
  twice in separate processes and compares a hash of the whole output tree). A
  two-phase generator holds peak memory near 7.5 GB instead of the ~36 GB a
  naive single-frame approach needed.
- **W2. Config-driven ingestion, part 1.** A YAML declares a source; the
  framework validates it, reads it schema-on-read, stamps lineage, and writes
  Bronze as a Delta table partitioned by `event_date`. Ingestion is idempotent:
  two separate runs produce an identical row count of 50,500,000, which includes
  the seeded duplicates by design.
- **W3, in progress.** Landing synced to S3 (102,056 objects, verified against
  the manifest) and a subset mirrored to a Unity Catalog Volume.

Roadmap:

- **W3.** Autoloader for incremental ingest, Delta OPTIMIZE, multi-source.
- **W4 to W5.** Data quality framework, skew resolution, join strategies.
- **W6.** Gold layer (three tables), late-arriving data, lineage.
- **W7.** CI/CD, tests in GitHub Actions, `RUNBOOK.md`.
- **W8.** Architecture diagrams and documentation polish.

The full dated decision log, exit criteria, and parked decisions live in
[`PROGRESS.md`](PROGRESS.md).

## Design notes

A few decisions worth calling out. The rest are in `PROGRESS.md`, each with the
alternative that was rejected and why.

**The load-bearing number is 50M rows, not 2.1 GB on disk.** Columnar
compression over closed vocabularies shrinks the bytes (DDIA Ch 3), but shuffle
spill, stragglers, and broadcast-versus-sort-merge join behavior all track row
distribution, not file size. 50M is the smallest scale where those effects are
observable.

**Partitioning is on event time, not ingestion time.** `event_timestamp` drives
the `event_date` partition, so a late-arriving event lands in an already-closed
partition and the event-time versus processing-time boundary stays verifiable
with one query (DDIA Ch 11).

**The small-files tax was measured, not assumed.** Ingesting 2.1 GB spawns 3,190
tasks, because 102,056 files at ~200 KB each hit the 4 MB
`spark.sql.files.openCostInBytes` floor, so bin-packing lands about 32 files per
128 MB partition. That number is the baseline OPTIMIZE has to beat.

**Bronze uses `overwrite` for idempotency in the W2 local demo, and that is
temporary.** Overwrite guarantees idempotency by rewriting everything: O(total)
per run, and it destroys history. An Autoloader checkpoint guarantees it by
remembering what was already read: O(new) per run, and Bronze stays append-only.
The checkpoint is an append-only log, which is the same structure that makes
Delta time travel possible (DDIA Ch 3).

## Repository layout

```
config/
  manifest.json              ground truth for all seeded defects, written before generation
  sources/player_events.yaml source contract, one file per source
src/
  generator/                 synthetic data generation, two-phase and memory-bounded
  ingestion/                 config loader, Bronze writer, entrypoint
notebooks/                   scratch inspection, not part of the pipeline
tests/
PROGRESS.md                  dated decision log and weekly exit criteria
```

## Running it

Requires Python 3.11 and Java 17 for local PySpark.

```bash
pip install -r requirements.txt

make generate       # build the synthetic dataset from config/manifest.json
make verify-repro   # prove the same seed produces a bit-identical dataset
make ingest         # landing to Bronze Delta, driven by config/sources/*.yaml
make test
```

Generated data is gitignored. `manifest.json` is versioned, because the manifest
is the specification and the dataset is its output.
