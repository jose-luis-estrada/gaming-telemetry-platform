# PROGRESS

Ship date: 2026-09-03
Current week: 3
Hours logged: 34

## How to read this file

This is the source of truth for where the project is. Four sections carry
weight and they are not interchangeable:

- **Settled decisions** are closed. Each one states the alternative that lost
  and why, so the decision can be defended without re-deriving it. Reopening
  one requires a dated entry in the Log first.
- **Exit criteria** are per week, binary, and checked only when verified, not
  when implemented.
- **Open decisions** are things genuinely undecided. If it is in here, do not
  build around it yet.
- **Log** is dated and append-only. It is never edited in place. Entries record
  what changed, what broke, and what the reason was.

Everything here assumes the reader knows nothing about the project. If a line
requires a conversation to understand, the line is wrong.

## What the project is

A config-driven ingestion and data quality framework, demonstrated on synthetic
gaming telemetry. The telemetry pipeline is the client of the platform, not the
product. A new source is onboarded by adding one YAML file with zero new
ingestion code.

The dataset carries five deliberately seeded defects (skew, late arrivals,
duplicates, schema drift, small files). The point is not that the pipeline runs
clean. The point is that each defect produces a diagnosis story that can be
defended under questioning. Five postmortems and a RUNBOOK are the deliverable;
the working pipeline is the scaffolding that makes them credible.

## Environments and where the data lives

Two environments, on purpose, because neither one alone does the job.

| Environment | What it is for | What it cannot do |
|---|---|---|
| Local PySpark in Docker | Learning Spark internals. Readable DAG, stages, task counts, shuffle behavior, Spark UI at localhost:4040. Every scale measurement in this file was taken here. | No Delta on cloud storage, no Unity Catalog, no Autoloader against object storage. |
| Databricks Free Edition | Delta, Unity Catalog, Autoloader, OPTIMIZE. This is where the demo runs. | No Spark UI, only the query profile. Serverless-only compute, one workspace, one metastore, no account console, no account-level REST API. |

There are three copies of the landing data. They are not redundant. They have
different jobs, and confusing them is the easiest way to misread this project.

| Copy | Location | Contents | Role |
|---|---|---|---|
| Local | `data/landing/`, gitignored | 102,056 parquet, 30 days, 50.5M rows | Origin. Everything else is a copy. Regenerable bit-identically from seed 42 via `make generate`. |
| S3 | `s3://gaming-telemetry-landing-jlestrada/` | `landing/` 102,056 parquet at 1.8 GiB, `screenshots/` 49,920 blobs at 243 MiB | The production-shaped landing zone. Full fidelity. Nothing reads it yet. |
| UC Volume | `/Volumes/workspace/telemetry/landing/` | 10,211 parquet, 3 days | The only cloud landing Databricks can actually read. Deliberate subset. |

Why three and not one: Databricks Free Edition cannot read the S3 bucket. See
the settled decision "Cloud landing zone" below. S3 is the documented
production shape and the real IAM and CLI story; the Volume is the executable
path. Both are honest as long as the difference is stated, which is what this
table is for.

## Definition of done

The project is finished when all six hold. Items 4, 5 and 6 are the project.
Items 1 through 3 are scaffolding.

- [ ] `make pipeline` runs end to end from the cloud landing to Gold, no manual
      intervention
- [ ] Two consecutive runs produce identical Gold row counts
- [ ] Tests pass in GitHub Actions
- [ ] 5 postmortems written
- [ ] RUNBOOK.md written
- [ ] Every partitioning decision defensible in 90 seconds without notes

## Status

- [X] W1  Data generator
- [X] W2  Ingestion framework, part 1
- [ ] W3  Ingestion framework, part 2 (in progress)
- [ ] W4  Data quality framework
- [ ] W5  Skew and joins
- [ ] W6  Gold, late data, lineage
- [ ] W7  CI/CD, runbook
- [ ] W8  README, diagrams, mock interview

## Settled decisions

Closed. Each states the option that lost. Do not reopen without a written
reason in the Log.

### Dataset shape

- **Volume and shape.** 50M events across 30 days, ~2.1 GB compressed parquet,
  ~1.67M events/day. Row count is the load-bearing number, not disk size. 50M
  rows is the minimum scale where spill, stragglers, and broadcast vs sort merge
  join are observable. 5M hides all three. 500M shows the same three at 6x the
  runtime. The 2.1 GB on disk is a columnar-compression artifact: closed
  vocabularies, dictionary encoding, DDIA Ch 3. It decompresses several-fold in
  the shuffle, so the join and skew effects track the 50M rows, not the GB.
- **Time window: 30 days.** The 72-hour max lateness touches 3 of 30
  partitions. That gives two populations: 27 closed partitions and 3 open ones.
  A 7-day window leaves 43 percent of history always open and the idea of a
  closed partition stops existing.
- **Partition layout.** `event_date=YYYY-MM-DD/`, single level, 30 partitions.
  Range partitioning on a time key. Write hot spot accepted in exchange for
  read-side partition pruning. DDIA Ch 6.
- **game_id is not in the path.** Skew lives in the key distribution, not the
  disk layout. Fix belongs in the shuffle. Deferred to W5. Nesting
  `event_date/game_id` would produce ~6,000 directories of small files.
- **Write format.** Parquet. 3 producers, 5-minute flush, 25,920 files at
  200 KB. Deliberately bad. 21:1 accounted-size ratio.
- **Landing file layout: partition on event time, split file on processing time.**
  `event_date=YYYY-MM-DD/` directory from event_timestamp; within it one file per
  (producer_id, 5-min flush window), flush window keyed on ingestion_timestamp.
  Producer and flush live in the filename, not the path, so the partition stays
  single-level. A late event lands in an old event_date directory via a new flush.
  DDIA Ch 11.
- **Unstructured data.** Stack traces stay in the table (column pruning makes
  them cheap). Screenshots go to a Volume as pointer + size + content type +
  sha256. The blob is named by its content hash, so identical bytes dedup to one
  file. Reference pattern, content-addressed.

### Seeded defects

- **Reprocess window: 3 days.** Derived from the 72-hour max lateness. 27
  partitions are closed and never rewritten.
- **Dedup window: 3 days.** Same derivation. Global dedup would reopen every
  closed partition and shuffle full history on every run. Bounded uniqueness
  guarantee, stated openly, not apologized for.
- **Dedup ordering key: `(producer_id, source_sequence_number)`**, with
  `ingestion_timestamp` as tiebreak. Not last-write-wins on wall clocks:
  3 producers means 3 clocks. DDIA Ch 8.
- **Duplicates are seeded in two flavors.** Byte-identical retries, and
  same-key-different-payload. A few land outside the 3-day window on purpose,
  to make the bounded guarantee visible instead of theoretical.
- **Schema drift: a JSON metadata key gated on event_timestamp.** `metadata` is a
  raw JSON string in landing; drift is a KEY (`network_type`) appearing inside the
  blob from `drift_day` 15 on, not a new table column. Gated on event_timestamp,
  not ingestion, so the boundary is clean in partition space and one query verifies
  it (new key before drift_day must be 0). Global flip across producers for
  verifiability; staggered is a one-line manifest change. Physical-column drift is
  a write-step variant, parked.

### Platform

- **Compaction is a platform capability**, not a producer concern. Delivered by
  the framework. Delta OPTIMIZE, W3.
- **Bronze idempotency in W2 is `mode("overwrite")`, and that is temporary.**
  Overwrite guarantees idempotency by rewriting everything: O(total) per run,
  and it destroys history, which contradicts the append-only Bronze contract.
  It was accepted in W2 because local PySpark has no Autoloader and the
  alternative was blocking the week on cloud setup. It is replaced in W3 by an
  Autoloader checkpoint, which guarantees idempotency by remembering what it
  already read: O(new) per run, Bronze stays append-only. DDIA Ch 3, the
  checkpoint is an append-only log.
- **Checkpoint ownership: the checkpoint path belongs to the (source, target)
  pair.** It is declared in the source YAML next to `landing_path` and
  `bronze_table`, versioned in git, at a stable path. It is not derived at
  runtime and it does not live in a temp directory. Reason: losing the
  checkpoint makes Autoloader treat every file as new, so the next run
  re-ingests the full landing and silently doubles Bronze. The job still exits
  green. This is an operational silent defect and a RUNBOOK entry.

### Cloud landing zone

- **The Databricks landing is a UC Volume, not an S3 external location.**
  The rejected option was pointing Unity Catalog at
  `s3://gaming-telemetry-landing-jlestrada/` via a storage credential and
  external location. It lost because Free Edition is serverless-only and
  reaching an arbitrary S3 bucket from serverless requires adding it to a
  network egress allowlist through an account-level REST API that Free Edition
  does not expose. Discovering that mid-week while debugging IAM trust policies
  was the failure mode worth avoiding. S3 is still built and still real: it
  carries the dedicated IAM user, the CLI sync, and the verified object counts.
  The interview answer is the honest one: in production the landing is S3 with
  a UC external location; on Free Edition it is mirrored to a Volume because
  serverless egress cannot be allowlisted without account-level access.
- **The Volume holds a 3-day subset, uncompacted.** The rejected option was
  compacting locally to one file per date and uploading 30 files, which would
  have taken minutes instead of an hour. It lost because compacting first
  erases the small-files property in the cloud, and with it the OPTIMIZE demo
  and half of postmortem #2. The subset preserves file count, which is the
  property W3 depends on. Row count at 50M stays a local measurement, which is
  where the Spark UI can defend it anyway.
- **The 3 dates straddle the drift boundary: 2026-01-14, 15, 16.** `drift_day`
  is 15 and the dataset starts at 2026-01-01, so the drift lands on either
  2026-01-15 or 2026-01-16 depending on whether the manifest counts from 0 or
  1. Picking those three covers both interpretations, so the schema drift
  boundary survives into the cloud subset and stays verifiable with the same
  single query used in W1. Cost of this choice: zero.
- **2026-01-17 is held back on purpose.** It is not missing, it is the
  incremental ingest demo. Uploading it after a clean Autoloader run proves the
  checkpoint picks up only new files. Without a held-back partition there is
  nothing left to add and incremental ingestion can only be asserted, not shown.

## Week 1 exit criteria

- [X] `make generate` produces 50M events across 30 days
- [X] `manifest.json` records exact ground truth for all 5 defects
- [X] Each defect verifiable against the manifest with a single query
- [X] Same seed produces a bit-identical dataset (`make verify-repro`)
- [X] Crashes carry text stack traces in-table, binary screenshots out-of-table
- [X] I can defend all four Week 1 decisions in 90 seconds, no notes

Note: the original criterion said "4-6 GB". It was rewritten to a row count
during W1 because the GB figure was a proxy for "a shuffle hurts" and columnar
compression broke the proxy. See Log 2026-07-17.

## Week 2 exit criteria

All met 2026-07-20.

- [X] A config file declares one source: landing path, format, expected
      schema, partition column, dedup key. Framework reads config, nothing
      hardcoded in the notebook.
- [X] Framework reads the W1 landing parquet and writes Bronze as a Delta
      table partitioned by event_date. Bronze is as-landed: metadata stays a
      raw JSON string, no parsing, no dedup. Bronze = append-only landing
      contract, DDIA Ch 3.
- [X] Ingestion is idempotent: re-running the same batch does not re-append
      the same source files. Verified by running `make ingest` twice in
      separate processes: both landed 50,500,000 rows. The seeded semantic
      duplicates are data and survive into Bronze untouched (they get resolved
      in Silver, W4-W5).
- [X] Runs in local PySpark (Docker), DAG readable in the Spark UI at
      localhost:4040.
- [X] PROGRESS.md records this section.

## Week 3 exit criteria

- [X] Landing data lives in AWS S3, uploaded via AWS CLI under a dedicated IAM
      user (`jlestrada-cli`), not root. Object counts verified against the W1
      manifest: 102,056 parquet and 49,920 screenshots, both exact.
- [X] A cloud landing that Databricks can actually read exists: UC Volume
      `workspace.telemetry.landing`, 3 event_date partitions, 10,211 files,
      per-partition counts verified against local (3401, 3408, 3402).
- [ ] The W2 Bronze ingestion code runs on Databricks Free Edition against the
      cloud landing. Only the path resolver changes between local and cloud, no
      branching in the ingestion logic itself.
- [ ] Incremental idempotent ingestion via Autoloader (Trigger.AvailableNow),
      replacing the W2 overwrite hack. Re-running ingests only new files; row
      count stable across runs; the checkpoint is the idempotency mechanism,
      not overwrite. DDIA Ch 3.
- [ ] The incremental claim is demonstrated, not asserted: uploading the
      held-back partition 2026-01-17 after a clean run causes Autoloader to
      pick up that partition and nothing else.
- [ ] OPTIMIZE compaction runs as a framework capability and collapses the
      small-files tax. Before and after file count and task count recorded.
      Feeds postmortem #2. DDIA Ch 3 (LSM compaction).
- [ ] A second source is onboarded by adding one YAML file, zero new ingestion
      code. Platform thesis demonstrated, not asserted.
- [X] PROGRESS.md records this section and the Free Edition landing-zone
      decision in the Log.

## Postmortems

One page each: what I expected, what broke, how I diagnosed it, what I changed,
what I would do differently at 100x scale. Written by hand, not drafted.

| # | Failure | Status | Evidence collected so far |
|---|---------|--------|---------------------------|
| 1 | Skew | not started | Seeded at 37% on one game_id. Straggler not yet observed. |
| 2 | Small files | evidence gathered, not written | 102,056 files. 3,190 tasks for 2.1 GB from the 4 MB openCostInBytes floor. Four taxed resources documented (compute, API, latency, disk slack). Awaiting OPTIMIZE before/after. |
| 3 | Duplicates | not started | 1% seeded, two flavors, 5 escaping the 3-day window. Survive into Bronze by design. |
| 4 | Late-arriving data | not started | 2-3% up to 72h late, verified against manifest. |
| 5 | Schema drift | not started | `network_type` key appears from drift_day 15. Boundary verified at 0 before. |

## Open decisions

Things genuinely undecided. Do not build around these.

- Migrate the full game distribution (`[0.37, 0.21, 0.21, 0.21]`) and
  corresponding game ID list from hardcoded values in `events.py` into
  `manifest.json`.

## Open loose ends

Not decisions, just unfinished chores. Each is small and each is verifiable.

- [X] AWS budget alert created (Billing and Cost Management, monthly cost
      budget at $5, plus a zero-spend budget). First two budgets are free.
- [X] Block Public Access confirmed on the bucket. Verify with
      `aws s3api get-public-access-block --bucket gaming-telemetry-landing-jlestrada`;
      all four flags must be true. Matters more than usual here because the
      uploaded screenshots contain account and IAM identifiers.
- [ ] `jlestrada-cli` scoped down from `AmazonS3FullAccess` to an inline policy
      targeting only this bucket. Parked deliberately during setup so it would
      not block the week; it is a least-privilege story worth having.
- [X] `src/ingestion/__init__.py` is missing.

## Parked

Things decided NOT to do, and why. This section exists to stop scope creep from
coming back through the side door.

- **Streaming implementation.** The JD asks for "exposure," not expertise.
  Databricks Free Edition is serverless-only and supports Trigger.AvailableNow()
  only, so it cannot demonstrate continuous streaming anyway. Streaming talking
  points (event time vs processing time, watermarks, late data, exactly-once,
  idempotency) get extracted from the batch work instead.
- **Scala and Java.** Python satisfies the language requirement on its own. I
  prepare an answer for why PySpark and when the JVM would matter, but I do not
  write Scala.
- **Airflow, dbt, Iceberg, OpenLineage, full Great Expectations.** Defensibility
  rule: nothing in this repo that I cannot explain from memory in 90 seconds.
- **Uploading Bronze or test data to S3.** The landing zone is the system of
  record and everything downstream is derived and recomputable. Two Bronzes can
  diverge with no arbiter. Rule: if the pipeline can regenerate it, it does not
  live in S3.
- **Uploading screenshots to the UC Volume.** The unstructured-data story is
  already carried by S3 and the local dataset. Autoloader and OPTIMIZE do not
  need them.

## Log

### 2026-07-09
Project created. Week 1 decisions settled: volume, partition layout, write
format, defect seeding. Repo structure defined: `src/generator/`, `notebooks/`,
`tests/`, `data/` gitignored. Nothing built yet.

### 2026-07-10
- Implemented the first seeded defect (game_id skew) using `rng.choice()` with a fixed probability distribution.
- Replaced global `np.random` usage with a local `np.random.default_rng(42)` to ensure reproducible, testable random number generation.
- Adopted the manifest-driven generation approach (Path 2): the manifest is written before data generation and serves as the declarative specification for the dataset.
- Defined the initial manifest structure:
  - Global fields: `seed`, `rows`
  - Defects grouped under `defects`
  - `skew` includes `hot_game_id`, `hot_game_probability`, and `tolerance`
- Decided that `manifest.json` is version-controlled under `config/`, while generated datasets remain under `data/`, which is intentionally ignored by Git.

### 2026-07-13
Closed event_timestamp: uniform draw over a semi-open [start, start+30d) window,
tz-aware UTC, offset in seconds for intraday resolution. Drawn last in the rng
stream to keep the prior columns bit-identical. Flat by design: skew lives on the
game_id axis, not the time axis, so event_date partitions stay balanced and the
W5 straggler reads cleanly. Acceptance query in Inspect.

### 2026-07-14
Closed late arrivals. New ingestion_timestamp column, separate from
event_timestamp: event time vs processing time, DDIA Ch 11. event_timestamp
still drives the partition, so late events land in an already-closed event_date.
Delay is a mixture: normal uniform [0, 300s), late uniform [300s, 72h). Late
population starts where normal ends so a single threshold separates them and the
late fraction is verifiable against the manifest, not approximate. Uniform over
exponential: the 72h bound is the invariant the watermark depends on, the shape
is a one-line knob.

Closed duplicates. New producer_id and source_sequence_number columns: the dedup
ordering key is (producer_id, source_sequence_number), not wall-clock, because 3
producers means 3 clocks. DDIA Ch 8. Two flavors seeded: byte-identical retries
(0.7) and same-key-different-payload corrections that carry a strictly higher
sequence so dedup keeps the correction. A handful (5) land past the 3-day window
on purpose, so the bounded guarantee is visible, not theoretical.

### 2026-07-15
Closed schema drift. New metadata column: a raw JSON string, the way telemetry
lands. Drift is a key (network_type) appearing inside the blob from day 15 on,
gated on event_timestamp so the boundary is clean in partition space and one
query verifies it. Global flip across producers. JSON string over physical
column: pyarrow would union a struct schema and null the missing field, erasing
the drift; raw JSON is also what the framework parses on read, giving the
contract-violation story for W2-W3.

Closed small files. Write step added. event_date directory from event_timestamp,
single level; within it one file per (producer_id, 5-min flush window), flush
window keyed on ingestion_timestamp. Partition = event time, file = processing
time, DDIA Ch 11: a late event lands in an old event_date directory via a new
flush, visible on disk. Producer and flush in the filename, not the path, so the
partition stays single-level. File count is not exactly 25,920: at 50M nearly
every cell fills but late arrivals open extra (event_date, flush_window) files in
already-passed partitions and push it slightly above, plus the escaped
duplicates; at test scale most cells are empty and it lands well below.

Closed crash artifacts. event_type "crash" carries a free-text stack_trace in the
table (semi-structured, cheap under column pruning) and, for a fraction of
crashes, a screenshot reference: path, bytes, content type, sha256. The binary
lands in a Volume-like dir named by its content hash, so identical bytes dedup to
one file and duplicate crashes point at the same blob, many references one file.
Not every crash carries a screenshot, which bounds binary volume regardless of
row count. This is the structured / semi-structured / unstructured coverage the
JD asks for, in one dataset.

### 2026-07-16
Closed the acceptance layer: each seeded defect verified against the manifest in
one query. Split by kind, not by taste: exact invariants assert and fail loud
(duplicate counts, the drift boundary at 0 before drift_day), statistical
fractions report expected-vs-observed and assert only above 100k rows, where
sampling noise falls under tolerance. These same checks become the W4 quality
framework in pytest against the full 50M. Ran at 200k: all six pass.

Added make verify-repro. Runs the generator twice in separate processes and
compares a root hash of the whole data tree. Separate processes on purpose: a
fresh rng each time proves "anyone with seed 42 gets this dataset", not just that
the rng object was untouched between calls. Content hashed, paths sorted, so the
result is order-independent. Passed at 50k: identical hash both runs.

### 2026-07-17
Generator materialized the full dataset as one pandas frame in the driver.
Measured the memory curve at 1M and 2M: ~720 MB per 1M rows, extrapolating to
~36 GB at 50M. That is the OOM ceiling. Split the generator into two phases: a
global decision phase holding only compact codes (int8 game/type/device, flags,
per-producer sequence, duplicate sampling over all N) and a streaming write
phase that expands the wide JSON and text columns one output file at a time. rng
stays entirely in phase 1 in fixed order, so bit-identical holds and verify-repro
passes with new hashes. Peak RAM at 50M dropped from ~36 GB to ~7.5 GB. Same
principle that motivates distributed processing: the driver is not where the
data lives. DDIA Ch 6, Ch 10.

Ran make generate at full 50M: 50.5M rows (50M + 1% duplicates), 252,408 crash
rows with in-table stack traces, 50,394 rows referencing 49,920 screenshot files
(the gap is duplicate crashes pointing at the same blob, one file many
references), 102,056 parquet files. Structured, semi-structured and unstructured
in one dataset.

Disk came in at 2.1 GB, under the old 4-6 GB target. The target was wrong, not
the dataset. 4-6 GB was a proxy for "a shuffle hurts", and the property that
carries that is 50M rows, not bytes on disk. Columnar compression on closed
vocabularies (DDIA Ch 3) shrinks the on-disk size, but it decompresses in the
shuffle and the hot key, 37% of 50M in one partition, is untouched by disk size.
Rewrote the exit criterion from "4-6 GB" to "50M rows, ~2.1 GB compressed". A
round number I cannot defend is worse than an odd one I can.

Parked, still open: 102,056 files exceed the 25,920 clean cross-product because
late arrivals open extra (event_date, flush_window) cells in already-passed
partitions. Small-files tax, feeds postmortem #2. Compaction is a W3 capability.

### 2026-07-18
Opened W2. First cut of the config-driven ingestion framework: one source
onboarded by YAML, read into Bronze Delta, zero notebook code. The thesis lives
in run.py, a loop over config/sources/*.yaml, so source #4 is a config change,
not a code change.

Source contract in config/sources/player_events.yaml. Fields split by when they
bite: format, landing_path and bronze_table act now; schema, dedup_key and
quality_rules are declared now but enforced downstream (schema in Silver,
dedup_key in W5, quality_rules in W4). One file holds the whole contract. Loader
(config.py) fails loud: a missing required field, an unknown format, or a typo'd
key raises instead of ingesting garbage silently.

Bronze is schema-on-read. No .schema() on the reader, so a drifted source lands
instead of failing the job. Enforcement is Silver's problem. DDIA Ch 4. This is
why the W1 metadata drift survives ingestion untouched.

Idempotency by overwrite, not append. mode("overwrite") means a re-run replaces
the table, so running ingest twice does not duplicate. Doctrine says Bronze is
append-only; true incremental idempotent ingest (Autoloader checkpoint or MERGE)
is W3, deferred on purpose. Trade-off stated, not hidden. The seeded semantic
duplicates from W1 are data and pass through into Bronze; they get resolved in
Silver.

Lineage on every row: _source_name, _source_file (input_file_name), _ingested_at,
_batch_id. This is the cataloging/lineage capability the JD names, and the first
thing on-call reads at 3 AM: which file, which run. bronze_path maps the logical
contract name to a local Delta path; on Databricks the same name registers as a
Unity Catalog table and only the resolver changes, not the config.

Local Spark: local[*] with the Delta extension via configure_spark_with_delta_pip,
the environment where the DAG is readable in the Spark UI.

### 2026-07-20
Closed W2. Two things landed on top of the 07-18 framework cut.

partitionBy("event_date") on the Bronze write, completing exit criterion #2.
event_date is recovered from the Hive-partitioned landing path via Spark
partition discovery, not derived from event_timestamp, so nothing is computed and
Bronze stays as-landed. Read-side pruning, DDIA Ch 6. overwrite still replaces the
whole table, which is the W2 idempotency mechanism; incremental replaceWhere /
Autoloader is W3.

Idempotency verified, not just implemented. Ran make ingest twice in separate
processes: both landed 50,500,000 rows (50M + the 1% seeded duplicates, which
Bronze keeps by design). Same discipline as W1 verify-repro: separate processes
prove the result does not depend on in-memory state. _batch_id and _ingested_at
change between runs by design (provenance) and do not affect count idempotency.

First read of the local Spark UI. The ingest save spawned 3,190 tasks for 2.1 GB.
Not the volume: 102,056 landing files at ~200 KB each hit the 4 MB open cost
(spark.sql.files.openCostInBytes), so bin-packing lands ~32 files per 128 MB
partition and 102,056 / 32 = 3,190. The small-files tax made visible, the number
to beat for postmortem #2. OPTIMIZE / compaction is the W3 capability that
collapses it. DDIA Ch 3.

W2 exit criteria all met. Current week -> 3.

### 2026-07-22
Landing moved to AWS S3. Bucket gaming-telemetry-landing-jlestrada, two
prefixes: landing/ (102,056 parquet, 1.8 GiB) and screenshots/ (49,920 blobs,
243 MiB). Both counts match the W1 manifest exactly, which is the verification:
sync neither dropped nor duplicated. The 50,394 rows to 49,920 files gap
survives the move, so content-addressed dedup holds in object storage.

Only landing data goes to S3. Bronze and test data stay out on purpose: the
landing zone is the system of record and everything downstream is derived and
recomputable. Uploading Bronze would create two Bronzes that can diverge with
no arbiter. Rule: if the pipeline can regenerate it, it does not live in S3.

Small-files tax now has four independent faces, not one. Compute: 3,190 tasks
for 2.1 GB from the 4 MB openCostInBytes floor. API: 102,056 PUTs to write and
~103 paginated LIST calls just to enumerate once, which Autoloader pays on
every directory-listing run. Latency: one round trip per 8.9 KiB file, network
overhead dominating actual reads. Disk slack: APFS 4 KiB block allocation over
102k files, ~10% wasted, which is the local-vs-S3 size gap. Same defect, four
taxed resources. Postmortem #2.

Upload verified against local file counts per partition: 3401, 3408, 3402.
Checkpoint ownership settled: the checkpoint path belongs to the (source,
target) pair and is declared in the source YAML, not derived at runtime.
Losing it silently reprocesses the full landing and duplicates Bronze.

### 2026-07-23
Cloud landing settled, and it is not what W3 assumed. Databricks Free Edition
is serverless-only and cannot reach an arbitrary S3 bucket: serverless egress
to a customer bucket has to be allowlisted through an account-level REST API
that Free Edition does not expose. So the "S3 external location plus Autoloader"
path in the W3 criteria was not available. Rejected it before spending the week
on IAM trust policies rather than after. The landing Databricks reads is a UC
Volume, `workspace.telemetry.landing`, under catalog `workspace` and schema
`telemetry`. S3 stays exactly as built: dedicated IAM user, CLI sync, verified
object counts. Two landings, two jobs, difference stated openly. That trade-off
is a better interview answer than pretending everything lives in S3, because it
requires knowing what an external location is, what a Volume is, and why
serverless egress is restricted.

Uploaded a 3-day subset to the Volume, uncompacted: 2026-01-14, 15, 16, 10,211
files. Compacting first would have uploaded in minutes instead of an hour but
would have erased the small-files property in the cloud, and with it the
OPTIMIZE demo and half of postmortem #2. File count is the property W3 depends
on; the 50M row count is a local measurement where the Spark UI can defend it.
Dates chosen to straddle the drift boundary: `drift_day` is 15 over a dataset
starting 2026-01-01, so the flip lands on either the 15th or the 16th depending
on whether the manifest counts from 0 or 1, and those three cover both readings.
Schema drift survives into the cloud subset for free.

2026-01-17 held back on purpose. It is the incremental ingest demo: after a
clean Autoloader run, uploading it proves the checkpoint picks up only new
files. Without a held-back partition, incremental ingestion can only be
asserted.

Upload verified per partition against local counts: 3401, 3408, 3402, all exact.
That local-versus-remote count check is the first cloud quality gate and belongs
in RUNBOOK.md: how you confirm a landing is complete before ingesting it.

Incidental confirmation from the per-partition listing: all 30 partitions carry
~3,400 files each, within 0.6 percent of one another. That is the W1 design
holding. Skew lives on the game_id axis, not the time axis, so event_date
partitions stay balanced and the W5 straggler will have exactly one cause
instead of two mixed together.

Checkpoint failure mode written down while it was fresh. Deleting the checkpoint
makes Autoloader treat all 10,211 files as new and append them again, doubling
Bronze, and the job exits green. No exception, no alert; the error only surfaces
downstream when a count is wrong. Same silent-defect pattern as the seeded data
defects, except the source is operational rather than the data. Three realistic
causes: checkpoint in a temp directory cleaned between runs, someone deleting it
by hand to unstick a job at 3 AM, or a changed path in the YAML. Recovery is
`DESCRIBE HISTORY` plus `RESTORE` to the last good version, which works only
because the Delta transaction log is append-only and old versions still exist.
The checkpoint and time travel are the same idea applied twice: an append-only
log lets you know what you already did, and lets you undo it. DDIA Ch 3.
