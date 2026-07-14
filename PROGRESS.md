# PROGRESS

Ship date: 2026-09-03
Current week: 1
Hours logged: 0

## Status
- [ ] W1  Data generator
- [ ] W2  Ingestion framework, part 1
- [ ] W3  Ingestion framework, part 2
- [ ] W4  Data quality framework
- [ ] W5  Skew and joins
- [ ] W6  Gold, late data, lineage
- [ ] W7  CI/CD, runbook
- [ ] W8  README, diagrams, mock interview

## Settled decisions
Closed. Do not reopen without a written reason in the Log.

- **Volume and shape.** 50M events across 30 days. 4 to 6 GB parquet,
  ~1.67M events/day, 120 to 200 MB per date partition. Minimum scale where
  spill, stragglers, and broadcast vs sort merge join are observable. 5M hides
  all three. 500M shows the same three at 6x the runtime.
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
  them cheap). Screenshots go to UC Volumes as pointer + size + content type +
  hash. Reference pattern.
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

## Definition of done
- [ ] `make pipeline` runs end to end from S3 to Gold, no manual intervention
- [ ] Two consecutive runs produce identical Gold row counts
- [ ] Tests pass in GitHub Actions
- [ ] 5 postmortems written
- [ ] RUNBOOK.md written
- [ ] Every partitioning decision defensible in 90 seconds without notes

## Week 1 exit criteria
- [ ] `make generate` produces 50M events, 30 days, 4-6 GB
- [ ] `manifest.json` records exact ground truth for all 5 defects
- [ ] Each defect verifiable against the manifest with a single query
- [ ] Same seed produces a bit-identical dataset
- [ ] Crashes carry text stack traces in-table, binary screenshots out-of-table
- [ ] I can defend all four Week 1 decisions in 90 seconds, no notes

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

- Exact seeding mechanics for schema drift: which key appears, on which day
- Exact seeding mechanics for late arrivals: shape of the delay distribution
- How many duplicates land outside the 3-day window
- Ratio of byte-identical duplicates to same-key-different-payload duplicates
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