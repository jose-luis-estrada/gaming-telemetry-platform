# 0005. Bronze idempotency through incremental-ingest state, not overwrite

Status: Accepted. Local implementation gap open.

## Context

Rerunning ingestion for the same landing must not duplicate Bronze, and Bronze
must stay append-only so its history is auditable.

## Decision

Idempotency comes from durable state that records which landing files were
already ingested (a streaming checkpoint with an available-now trigger, or an
equivalent processed-files ledger). Runs are O(new files). The state location
belongs to the (source, target) pair, is declared in the source YAML as
`checkpoint_path`, and is never derived at runtime or placed in a temp directory.

## Rejected alternatives

- **`mode("overwrite")` on every run.** Idempotent, but O(total) per run and it
  destroys history, contradicting the append-only contract.

## Consequences

- Losing the checkpoint makes every landed file look new: the next run silently
  doubles Bronze and still exits green. This is a runbook entry and a
  reconciliation check (Bronze rows equal landed rows).
- Verified on the previous managed environment: with three dates ingested,
  landing a held-back fourth date added exactly that partition and nothing else.
- **Known gap:** `LocalEnvironment.write_bronze` still uses overwrite. It is
  replaced when the lakehouse environment lands (ADR-0014).
