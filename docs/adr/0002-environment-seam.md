# 0002. Deployment differences live behind one Environment seam

Status: Accepted

## Context

The same layer logic must run in more than one place (local PySpark today, the
KIND lakehouse next). Read APIs, write verbs, storage roots and even how a row's
source file is exposed differ between them.

## Decision

`src/ingestion/environment.py` defines an `Environment` strategy that owns the
Spark session, storage roots, source reads, terminal writes and the source-file
column expression. Layer code holds an `Environment` and never branches on
where it runs. The only conditional is `get_environment()`, which reads
`TELEMETRY_ENV` once at the program edge. Storage roots come from configuration
(`TELEMETRY_DATA_ROOT`), not constants.

## Rejected alternatives

- **An `if env == ...` inside ingestion functions.** Every new environment would
  touch every layer, and the difference between environments would be spread
  across the codebase instead of stated in one file.

## Consequences

- Moving Bronze from local paths to S3A on SeaweedFS is a new subclass, not a
  rewrite. The Gold builders are pure functions and already ran unchanged in two
  environments.
- Logical names (source name, layer) stay stable while physical bindings swap.
