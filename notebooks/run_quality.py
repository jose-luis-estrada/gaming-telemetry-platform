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
# %%

# %%
import sys
import os
from pathlib import Path

ROOT = Path.cwd()
while not (ROOT / "src").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

os.chdir(ROOT) 

from src.ingestion.quality import split_quarantine

run_id = "manual-" + __import__("datetime").datetime.now().strftime("%Y%m%dT%H%M%S")
clean, rejects = split_quarantine(bronze_df, cfg.quality_rules, run_id)

# Count both. clean + rejects must equal the original: nothing dropped.
n_clean, n_rejects = clean.count(), rejects.count()
print(f"clean={n_clean}  rejects={n_rejects}  total={n_clean + n_rejects}")

# What got rejected and by which rule. This is the 3 AM view.
rejects.groupBy("_reject_rule").count().show(truncate=False)
# %%
