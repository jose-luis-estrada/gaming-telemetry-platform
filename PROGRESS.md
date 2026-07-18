# PROGRESS

Ship date: 2026-09-03
Current week: 2
Hours logged: 16

## Status
- [X] W1  Data generator
- [ ] W2  Ingestion framework, part 1
- [ ] W3  Ingestion framework, part 2
- [ ] W4  Data quality framework
- [ ] W5  Skew and joins
- [ ] W6  Gold, late data, lineage
- [ ] W7  CI/CD, runbook
- [ ] W8  README, diagrams, mock interview

## Settled decisions
Closed. Do not reopen without a written reason in the Log.

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
- **Compaction is a platform capability**, not a producer concern. Delivered by
  the framework. Delta OPTIMIZE, W3.
- **Unstructured data.** Stack traces stay in the table (column pruning makes
  them cheap). Screenshots go to a Volume as pointer + size + content type +
  sha256. The blob is named by its content hash, so identical bytes dedup to one
  file. Reference pattern, content-addressed.
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
  blob from day 15 on, not a new table column. Gated on event_timestamp, not
  ingestion, so the boundary is clean in partition space and one query verifies it
  (new key before drift_day must be 0). Global flip across producers for
  verifiability; staggered is a one-line manifest change. Physical-column drift is
  a write-step variant, parked.
- **Landing file layout: partition on event time, split file on processing time.**
  `event_date=YYYY-MM-DD/` directory from event_timestamp; within it one file per
  (producer_id, 5-min flush window), flush window keyed on ingestion_timestamp.
  Producer and flush live in the filename, not the path, so the partition stays
  single-level. A late event lands in an old event_date directory via a new flush.
  DDIA Ch 11.

## Definition of done
- [ ] `make pipeline` runs end to end from S3 to Gold, no manual intervention
- [ ] Two consecutive runs produce identical Gold row counts
- [ ] Tests pass in GitHub Actions
- [ ] 5 postmortems written
- [ ] RUNBOOK.md written
- [ ] Every partitioning decision defensible in 90 seconds without notes

## Week 1 exit criteria
- [X] `make generate` produces 50M events, 30 days, 4-6 GB
- [X] `manifest.json` records exact ground truth for all 5 defects
- [X] Each defect verifiable against the manifest with a single query
- [X] Same seed produces a bit-identical dataset
- [X] Crashes carry text stack traces in-table, binary screenshots out-of-table
- [X] I can defend all four Week 1 decisions in 90 seconds, no notes

## Week 2 exit criteria
- [ ] A config file declares one source: landing path, format, expected
      schema, partition column, dedup key. Framework reads config, nothing
      hardcoded in the notebook.
- [ ] Framework reads the W1 landing parquet and writes Bronze as a Delta
      table partitioned by event_date. Bronze is as-landed: metadata stays a
      raw JSON string, no parsing, no dedup. Bronze = append-only landing
      contract, DDIA Ch 3.
- [ ] Ingestion is idempotent: re-running the same batch does not re-append
      the same source files. The seeded semantic duplicates are data and
      survive into Bronze untouched (they get resolved in Silver, W4-W5).
- [ ] Runs in local PySpark (Docker), DAG readable in the Spark UI at
      localhost:4040. Same code path runs on Databricks against the S3 landing.
- [ ] PROGRESS.md records this section (this).

## Postmortems
| # | Failure | Status |
|---|---------|--------|
| 1 | Skew | not started |
| 2 | Small files | not started |
| 3 | Duplicates | not started |
| 4 | Late-arriving data | not started |
| 5 | Schema drift | not started |

## Open decisions
Things I have not settled yet.

- Migrate the full game distribution (`[0.37, 0.21, 0.21, 0.21]`) and corresponding game ID list from hardcoded values in `events.py` into `manifest.json`.

## Parked
Things I decided NOT to do, and why. This section exists to stop scope creep
from coming back through the side door.

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