# 0012. Landing is the system of record; derived data is never copied out

Status: Accepted

## Decision

The landing zone is immutable and is the only copy that matters. Every table
downstream is derived and can be rebuilt from landing alone. Derived data
(Bronze, Silver, test data) is never uploaded to the landing object store.

## Rejected alternatives

- **Uploading Bronze alongside landing.** Two Bronzes can diverge with no arbiter.

## Consequences

- The generator is deterministic for a fixed seed (`make verify-repro` hashes the
  whole output tree across two processes), so even landing can be regenerated.
- `config/manifest.json` is versioned as the specification; the dataset is its
  output and is not committed.
