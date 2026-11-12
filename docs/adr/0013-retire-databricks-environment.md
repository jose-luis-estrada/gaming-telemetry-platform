# 0013. Retire the Databricks Free Edition environment

Status: Accepted. Supersedes the Unity Catalog Volume landing decisions.

## Context

The first managed environment was Databricks Free Edition: Autoloader for
incremental ingest, Unity Catalog for tables and lineage. Free Edition is
serverless only and cannot reach an arbitrary S3 bucket (egress allowlisting
needs an account-level API it does not expose), so the cloud landing was a Unity
Catalog Volume holding a 4-day subset, and the full S3 landing was never read.

## Decision

Remove `DatabricksEnvironment` and the Databricks notebooks from `main`. The full
implementation is preserved at tag `v1-databricks`.

## Consequences

- Autoloader and Unity Catalog lineage are no longer available. Their
  replacements are part of ADR-0014: incremental-ingest state for Bronze and dbt
  docs for lineage.
- Results measured there remain valid evidence (incremental ingest of a
  held-back partition, Silver dedup of ~1% in the subset, four-hop lineage).
