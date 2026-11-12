# 0004. Bronze is as-landed, schema-on-read, append-only, with lineage

Status: Accepted

## Context

Bronze must be rebuildable, debuggable and must not fail when a producer changes
what it sends.

## Decision

- Bronze stores rows as landed: no parsing, no dedup, no type enforcement.
- `metadata` stays a raw JSON string. Schema drift arrives as a new key inside
  that string, so Bronze absorbs it without failing; enforcement and surfacing
  belong to Silver.
- Every row carries lineage: `_source_name`, `_source_file`, `_ingested_at`,
  `_batch_id`. These answer "which file and which run" during an incident.
- Each source owns its landing subtree, `landing_root/<source>/`, declared in its
  YAML `landing_path`.

## Rejected alternatives

- **Source at the landing root (`landing_path: "."`).** The recursive reader
  swept a second source into the first and failed on conflicting directory
  structures. The fix was a config change plus a data relocation, no code.
- **Enforcing the declared schema in Bronze.** A drifted producer would fail
  ingestion and lose data instead of landing it for downstream handling.

## Consequences

- Seeded duplicates and drifted rows survive into Bronze by design; Silver row
  counts are where they are resolved.
