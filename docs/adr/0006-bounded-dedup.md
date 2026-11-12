# 0006. Bounded dedup ordered by producer sequence, never wall clock

Status: Accepted

## Context

1% of events repeat in two flavors: byte-identical retries and same-key
corrections with a different payload. Three producers means three clocks.

## Decision

- Dedup is two questions in two fields: `identity_key` (which rows are the same
  event) and `ordering_key` (which copy wins, max wins).
- player_events orders on `(producer_id, source_sequence_number)`. purchases
  orders on `purchase_timestamp`, which is valid only because it has a single
  producer and therefore a single clock.
- Dedup uses `row_number`, not `rank`: rank keeps both rows of a byte-identical
  tie.
- The window is bounded at 72h, derived from the maximum lateness, and measured
  between copies of the same identity, anchored on the earliest ingestion.
  Copies landing beyond the window survive.

## Rejected alternatives

- **Last-write-wins on a timestamp.** Picks whichever producer's clock runs
  ahead.
- **Global dedup.** Reopens every closed partition and reshuffles full history on
  every run.
- **Window anchored on the batch maximum ingestion.** Shipped once and left
  499,106 of 500,000 duplicates alive: only the last three days were deduped.

## Consequences

- Silver holds 50,000,005 rows at full scale: 50M distinct events plus the 5
  seeded out-of-window duplicates. The +5 is the bounded guarantee expressed as a
  number; a global dedup would show exactly 50M and hide the bound.
- In local mode the window sort over 50M identities needs a 6 GB driver heap; on
  a cluster the sort distributes across executors.
