# %%
from pathlib import Path

from src.ingestion.config import load_source_config
from src.ingestion.bronze import ingest_source
from src.ingestion.environment import Environment, get_environment

SOURCES_DIR = Path("config/sources")

def main(env: Environment | None = None) -> None:
    # Resolved from TELEMETRY_ENV unless injected (tests inject one directly).
    env = env or get_environment()

    configs = sorted(SOURCES_DIR.glob("*.yaml"))
    if not configs:
        raise FileNotFoundError(f"no source configs in {SOURCES_DIR}")

    for path in configs:
        cfg = load_source_config(path)
        n = ingest_source(env, cfg)
        print(f"{cfg.name}: ingested {n} rows")

    env.stop()

if __name__ == "__main__":
    main()