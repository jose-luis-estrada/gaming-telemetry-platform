# %%
import sys
import os
from pathlib import Path

# El kernel arranca en src/ingestion/, pero los imports son from src.*, que
# necesitan la raiz del repo en el path. Subimos 2 niveles: ingestion -> src -> raiz.
ROOT = Path.cwd()
while not (ROOT / "src").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

os.chdir(ROOT) 

from src.ingestion.config import load_source_config
from src.ingestion.quality import run_quality_checks, print_report
from src.ingestion.environment import get_environment   # ajusta el import a tu ruta real

env = get_environment()                                  # LocalEnvironment por TELEMETRY_ENV
cfg = load_source_config(ROOT / "config" / "sources" / "player_events.yaml")

# Reusa el MISMO seam que write/optimize: delta_table(cfg, spark) direcciona la
# tabla por path local. .toDF() la vuelve un DataFrame legible por SQL.
bronze_df = env.delta_table(cfg, env.spark()).toDF()   # spark() con parentesis

# cfg.quality_rules es una lista de dicts (viene directo del YAML). Atributo, no key.
print_report(run_quality_checks(bronze_df, cfg.quality_rules))

