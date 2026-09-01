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
# %%

# %%
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
from src.ingestion.quality import split_quarantine

run_id = "manual-" + __import__("datetime").datetime.now().strftime("%Y%m%dT%H%M%S")
clean, rejects = split_quarantine(bronze_df, cfg.quality_rules, run_id)

n_clean, n_rejects = clean.count(), rejects.count()
print(f"clean={n_clean}  rejects={n_rejects}  total={n_clean + n_rejects}")
rejects.groupBy("_reject_rule").count().show(truncate=False)
# %%

# %%
# ----------------------------
# W4 rejects demo: probar que el quarantine ATRAPA data mala, no solo que no
# rechaza la buena. Bronze real esta limpio (rejects=0), asi que inyectamos
# filas malas DERIVADAS de Bronze para ejercitar el split sin tocar Bronze.
# ----------------------------
from pyspark.sql import functions as F

# Derivar las filas malas de bronze_df garantiza que el schema (incluidas las
# columnas de lineage de W2) calce exacto, asi el union no alinea nada a mano.
good    = bronze_df.limit(4)  # control: filas validas, deben salir clean
bad_id  = bronze_df.limit(3).withColumn("event_id", F.lit(None).cast("long"))
bad_ts  = bronze_df.limit(2).withColumn("event_timestamp", F.lit(None).cast("timestamp"))
# Una fila viola AMBAS reglas: prueba que coalesce tagea solo la PRIMERA (event_id).
bad_both = (bronze_df.limit(1)
            .withColumn("event_id", F.lit(None).cast("long"))
            .withColumn("event_timestamp", F.lit(None).cast("timestamp")))

# unionByName, no union: matchea por nombre de columna, no por posicion, que es
# fragil si el orden de columnas cambia entre los DataFrames.
injected = good.unionByName(bad_id).unionByName(bad_ts).unionByName(bad_both)

run_id = "inject-" + __import__("datetime").datetime.now().strftime("%Y%m%dT%H%M%S")
clean, rejects = split_quarantine(injected, cfg.quality_rules, run_id)

# Invariante W4: nada se cae. clean + rejects tiene que ser igual al total inyectado.
n_clean, n_rejects, n_total = clean.count(), rejects.count(), injected.count()
print(f"clean={n_clean}  rejects={n_rejects}  total={n_total}  "
      f"(clean+rejects == total: {n_clean + n_rejects == n_total})")

# La vista de las 3 AM: que regla disparo y cuantas veces.
rejects.groupBy("_reject_rule").count().show(truncate=False)
# %%


# %%
# ----------------------------
# Persistir rejects a Delta: la tabla de quarantine debe quedar inspectable en
# disco, no vivir solo en memoria. Esto cierra "quarantined to a rejects table".
# ----------------------------
rejects_path = "data/rejects/player_events"

# overwrite, no append: el demo es re-corrible e idempotente, una corrida fresca
# reemplaza y no acumula. overwriteSchema porque rejects trae _reject_rule y
# _rejected_at que Bronze no tiene, asi la primera escritura define el schema nuevo.
(rejects.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(rejects_path))

# Read-back: probar que aterrizo en DISCO, no que el DataFrame seguia en memoria.
landed = env.spark().read.format("delta").load(rejects_path)
print("rejects landed to Delta:", landed.count())
landed.select("event_id", "event_timestamp", "_reject_rule",
              "_rejected_at", "_source_file").show(truncate=False)
# %%

# %%
# W4 criterion 5: idempotency of the quality pass, demonstrated not assumed.
# Aggregates are read-only over a fixed Bronze Delta version, so identical
# results are guaranteed by construction. The criterion says demonstrate it.
from src.ingestion.quality import run_quality_checks

def snapshot(df, rules):
    # Reduce each run to the tuple that must be stable: name, observed, pass/fail.
    # Round observed so float noise never fakes a difference.
    return sorted((r.name, round(r.observed, 6), r.passed)
                  for r in run_quality_checks(df, rules))

run_a = snapshot(bronze_df, cfg.quality_rules)
run_b = snapshot(bronze_df, cfg.quality_rules)

print("run A:", run_a)
print("run B:", run_b)
print("identical:", run_a == run_b)
assert run_a == run_b, "quality pass is NOT idempotent"
# %%