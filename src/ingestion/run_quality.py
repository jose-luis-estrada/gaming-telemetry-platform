"""Entrypoint for the Bronze quality gate.

For every source: run the aggregate rules (a failed hard rule fails the run)
and write rows that fail a row-level rule to the source's quarantine table.
Nothing is dropped silently.
"""

import uuid
from pathlib import Path

from src.ingestion.config import load_source_config
from src.ingestion.environment import Environment, get_environment
from src.ingestion.quality import print_report, run_quality_checks, split_quarantine

SOURCES_DIR = Path("config/sources")


def main(env: Environment | None = None) -> None:
    env = env or get_environment()
    run_id = uuid.uuid4().hex

    configs = sorted(SOURCES_DIR.glob("*.yaml"))
    if not configs:
        raise FileNotFoundError(f"no source configs in {SOURCES_DIR}")

    for path in configs:
        cfg = load_source_config(path)
        bronze = env.delta_table(cfg, env.spark()).toDF()

        print(f"--- {cfg.name}")
        print_report(run_quality_checks(bronze, cfg.quality_rules))

        _, rejects = split_quarantine(bronze, cfg.quality_rules, run_id)
        # Overwrite: quarantine is recomputed from Bronze on every run, so two
        # runs over the same Bronze produce the same rejects.
        rejects.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            env.table_uri("quarantine", cfg.name)
        )
        n = env.delta_table(cfg, env.spark(), "quarantine").toDF().count()
        print(f"{cfg.name}: quarantined {n} rows")

    env.stop()


if __name__ == "__main__":
    main()
