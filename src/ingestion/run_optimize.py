"""Entrypoint for compaction: OPTIMIZE every source's Bronze and Silver table."""

from pathlib import Path

from src.ingestion.config import load_source_config
from src.ingestion.environment import Environment, get_environment
from src.ingestion.optimize import optimize_source

SOURCES_DIR = Path("config/sources")


def main(env: Environment | None = None) -> None:
    env = env or get_environment()
    for path in sorted(SOURCES_DIR.glob("*.yaml")):
        cfg = load_source_config(path)
        for layer in ("bronze", "silver"):
            before, after, _ = optimize_source(env, cfg, layer)
            print(
                f"{layer}.{cfg.name}: files {before['numFiles']} -> {after['numFiles']}, "
                f"bytes {before['sizeInBytes']} -> {after['sizeInBytes']}"
            )
    env.stop()


if __name__ == "__main__":
    main()
