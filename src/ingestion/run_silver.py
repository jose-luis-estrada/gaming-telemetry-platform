# %%
from pathlib import Path

from src.ingestion.config import load_source_config
from src.ingestion.silver import ingest_silver
from src.ingestion.environment import Environment, get_environment

SOURCES_DIR = Path("config/sources")

def main(env: Environment | None = None) -> None:
    # Mirror of run.py, but for the Bronze -> Silver stage. Same loop over the same
    # source configs, so a new source gets Silver dedup for free, zero new code.
    env = env or get_environment()

    configs = sorted(SOURCES_DIR.glob("*.yaml"))
    if not configs:
        raise FileNotFoundError(f"no source configs in {SOURCES_DIR}")

    for path in configs:
        cfg = load_source_config(path)
        # ingest_silver reads Bronze, dedups within the window, writes Silver.
        n = ingest_silver(env, cfg)
        print(f"{cfg.name}: silver rows {n}")

    env.stop()

if __name__ == "__main__":
    main()