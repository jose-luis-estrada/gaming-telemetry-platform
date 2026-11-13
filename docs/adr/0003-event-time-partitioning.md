# 0003. Partition on event time; dataset shape and landing layout

Status: Accepted

## Context

The synthetic dataset must exercise the failure modes of a real telemetry feed
at a scale where they are observable: shuffle spill, stragglers and join
strategy changes track row counts, not bytes on disk.

## Decision

- **Shape.** 50M events over 30 days (~1.67M per day, ~2.1 GB compressed
  parquet). Row count is the load-bearing number: 5M hides spill and stragglers,
  500M shows the same effects at many times the runtime. Smaller fixed-seed
  profiles are allowed for constrained environments as long as every seeded
  defect remains present and countable.
- **Window.** 30 days. With 72h maximum lateness, 27 partitions are closed and 3
  are open, so a closed partition exists as a concept. A 7-day window would leave
  most of history permanently open.
- **Partitioning.** `event_date=YYYY-MM-DD/`, single level, derived from
  `event_timestamp`. A late event lands in an already-closed partition, which is
  the property late-data handling must deal with.
- **game_id is not in the path.** Skew lives in the key distribution; the fix
  belongs in the shuffle. Nesting `event_date/game_id` would create thousands of
  directories of small files.
- **Landing files.** Within each event_date, one file per (producer, 5-minute
  flush window) keyed on ingestion time. Deliberately many small files.
- **Unstructured data.** Stack traces stay in the table (column pruning makes them
  cheap). Screenshots are stored out of table, content-addressed by sha256, with
  pointer, size and content type in the row.

## Rejected alternatives

- **Partition on ingestion date.** Late data would never touch a closed
  partition, which hides the event-time versus processing-time problem.

## Consequences

- One canonical `event_date` is set by the generator and carried through every
  layer. Re-deriving it downstream with `to_date(event_timestamp)` once produced
  a phantom 31st date, because the generator's day bucket is offset from
  midnight. Downstream layers must use the column, never re-derive it.
