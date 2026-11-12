# 0010. Resolve key skew with salting; AQE does not apply at this scale

Status: Accepted

## Context

One game_id holds 37% of events (18.6M of 50M).

## Decision

Salt the hot side of joins keyed on game_id: add a random salt in [0, N), replicate
the small side N times, join on (game_id, salt), drop the salt. N = 8.

## Evidence

- An aggregation on game_id shows no straggler: the count is pre-aggregated map
  side, so the hot key never crosses the shuffle.
- A join keyed on game_id does: 18,686,484 records in one reduce task, max task
  5s versus median 5ms, 374 MiB spill.
- AQE skew-join handling did not trigger. AQE judges skew in bytes, and the hot
  partition was about 1.9 MiB compressed, far under the default threshold. Even
  with the threshold lowered, the median of four non-empty partitions is
  contaminated by the hot partition itself.
- After salting: max shuffle read per task 18,686,484 to 3,662,918 records,
  spill 374 to 100 MiB.

## Rejected alternatives

- **Rely on AQE.** Designed for GB-scale skew; this skew is in rows, not bytes.
- **Broadcast the dimension.** Valid when the other side is small, but it hides
  rather than resolves skew for fact-to-fact joins.

## Consequences

- Salting replicates the small side N times. In production, salt only the hot
  keys so a large dimension is not multiplied.
