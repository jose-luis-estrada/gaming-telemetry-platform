# 0007. Hard and soft quality rules; row failures quarantined

Status: Accepted

## Context

Four of the five seeded defects produce plausible numbers with no failure
signal. Checks must distinguish invariants from statistical expectations, and
bad rows must stay inspectable.

## Decision

- **Aggregate rules** are plain SQL over Bronze returning one `observed` value.
  `hard` rules assert an exact invariant and fail the run (for example, the drift
  key count before the drift day must be 0). `soft` rules report observed versus
  expected within a tolerance and pass (hot game share, late-arrival rate).
  Severity follows the defect: an invariant with a tolerance is a lie, a sampled
  proportion without one is fragile.
- **Row rules** split data into clean and rejects. Rejects keep the full row plus
  `_reject_rule` (first violated rule), `_rejected_at` and `_run_id`, and the
  Bronze lineage columns. Nothing is dropped; clean plus rejects equals total.
- The quality gate runs between Bronze and Silver in the pipeline. A failed hard
  rule stops downstream stages.

## Rejected alternatives

- **Dropping bad rows.** Silent loss is the anti-goal.
- **Failing the run on a single bad row.** One malformed event would block all
  data.

## Consequences

- The quarantine table is recomputed by overwrite so reruns are idempotent. At
  production volume it should append, partitioned by run, each run replacing its
  own slice, so history accumulates without duplication.
- Verified at full scale: drift before boundary 0, hot game share 0.3700,
  late-arrival rate 0.0250, all matching the manifest.
