# 0001. Sources are onboarded through YAML contracts, validated fail-loud

Status: Accepted

## Context

A pipeline that solves one movement problem has to be rewritten for the next
source. The platform must onboard a new source without new framework code, and
a broken contract must never degrade into a run that passes while doing nothing.

## Decision

Each source is one file in `config/sources/`. Fields split by when they act:
`format`, `landing_path` and `bronze_table` act at ingestion; `schema`,
`identity_key`, `ordering_key`, `dedup_window_hours` and `quality_rules` are
declared up front and enforced downstream. Every entrypoint loops over that
directory.

The loader rejects at load time: missing required fields, unknown formats,
unknown keys, and any quality rule whose shape, type, check, severity or operator
the engine does not implement. Rule dispatch is explicit (`type: row` or
`type: aggregate`); there is no default type.

## Rejected alternatives

- **Per-source Python modules.** Every source becomes code to review and test.
- **Implicit rule types (no `type` means aggregate).** A malformed rule would
  pass as green. This happened twice: aggregate rules on player_events without
  `type` were skipped until fixed, and purchases shipped with rules in an
  unsupported shape that were skipped entirely. Validation in the loader closes
  the class of bug instead of the instance.

## Consequences

- Onboarding `purchases` (different schema, different dedup semantics) required
  zero changes to `src/ingestion/`.
- The partition column is still a platform-wide literal (`event_date`), so a
  source with no event date cannot be onboarded yet. Open question: declare it
  per source, or make `event_date` a platform contract every source carries.
