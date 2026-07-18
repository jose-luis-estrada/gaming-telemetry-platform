# %%
from pathlib import Path

from src.ingestion.config import load_source_config
from src.ingestion.bronze import build_spark, ingest_source, bronze_path

# ----------------------------
# Run all declared sources
# ----------------------------
# The whole thesis of the platform lives in this loop: onboarding a new source is
# dropping a YAML into config/sources/, not editing this file. Every file here is
# read, validated, and ingested the same way. Adding source #4 is a config change.
SOURCES_DIR = Path("config/sources")

def main() -> None:
    configs = sorted(SOURCES_DIR.glob("*.yaml"))
    if not configs:
        raise FileNotFoundError(f"no source configs in {SOURCES_DIR}")

    spark = build_spark()
    for path in configs:
        cfg = load_source_config(path)
        n = ingest_source(spark, cfg)
        print(f"{cfg.name}: ingested {n} rows -> {bronze_path(cfg)}")
    spark.stop()

if __name__ == "__main__":
    main()