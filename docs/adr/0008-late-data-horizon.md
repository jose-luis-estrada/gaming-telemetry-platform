# 0008. 48h late-data horizon, partition-scoped rewrite, late_after_close

Status: Accepted

## Context

2 to 3% of events arrive up to 72h late and belong to already-closed event
dates. Gold must be corrected, but no partition can stay open forever.

## Decision

- Gold accepts an event if it arrived within 48h of its own event time. Past the
  horizon, the partition is frozen and the event is routed to
  `gold.late_after_close`: neither dropped nor merged.
- Late data rewrites only the affected `event_date` partitions with Delta
  `replaceWhere`, never the whole table.
- 48h is deliberately below the seeded 72h maximum, so the 48 to 72h band is
  non-empty and the trade-off is observable.

## Rejected alternatives

- **No horizon.** Any partition could be rewritten indefinitely; Gold consumers
  can never treat a day as final.
- **Full-table recompute.** O(table) per late batch.

## Consequences

- Invariant: within horizon plus past horizon equals all Silver
  (49,583,401 + 416,604 = 50,000,005 at full scale).
- Verified from the Delta log: a reprocess removed and added exactly one file of
  30. A Gold value that was wrong-low before reprocessing matched Silver's
  within-horizon truth afterwards, and a second identical reprocess changed
  nothing. `scripts/verify_late_data.py` reruns this check.
