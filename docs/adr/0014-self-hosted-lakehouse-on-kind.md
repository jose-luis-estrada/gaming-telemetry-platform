# 0014. Self-hosted lakehouse on KIND with Airflow and dbt

Status: Accepted. Implementation in progress.

## Context

Managed services hide the parts of the platform that fail in production:
scheduling, object-store semantics, catalog wiring, single-writer guarantees.
The platform should run end to end on infrastructure we operate.

## Decision

| Concern | Component |
|---|---|
| Object storage | SeaweedFS with its S3-compatible gateway (`s3a://`) |
| Compute | Spark on a multi-node KIND (Kubernetes in Docker) cluster |
| Orchestration | Airflow, replacing `make pipeline` |
| Silver and Gold transformations | dbt over the Spark Thrift Server |
| Table format | Delta Lake at every layer |
| Images | Docker, versioned tags, loaded into KIND |

PySpark keeps ingestion (landing to Bronze) where programmatic control is
needed. dbt owns Silver and Gold as versioned, tested SQL and provides lineage.

## Consequences

- `silver.py` and `gold.py` are removed only after the dbt models reproduce
  their results exactly (50,000,005 Silver rows; 570 / 120 / 20 Gold rows at
  full scale).
- Decisions still to record as their own ADRs: how Spark runs on Kubernetes,
  Bronze idempotency without Autoloader, single-writer safety on S3-compatible
  storage, the shared table catalog, the dbt boundary, DAG shape and backfills,
  secrets and configuration.
- The seeded defects remain the acceptance tests. A reduced dev profile must keep
  all five countable: with `drift_day` 15 and 72h maximum lateness, a profile
  shorter than about 18 days loses the drift boundary.
