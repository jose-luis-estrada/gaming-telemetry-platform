.PHONY: generate test verify-repro ingest silver gold pipeline

generate:
	python -m src.generator.events

test:
	pytest tests/ -v

# Bit-identical guarantee: generate twice in separate processes (fresh rng each
# time), hash the whole output tree after each, fail if the two hashes differ.
verify-repro:
	@$(MAKE) --no-print-directory generate >/dev/null
	@python -m src.generator.tree_hash data > .repro_a
	@$(MAKE) --no-print-directory generate >/dev/null
	@python -m src.generator.tree_hash data > .repro_b
	@diff .repro_a .repro_b >/dev/null \
		&& echo "REPRODUCIBLE: $$(cat .repro_a)" \
		|| { echo "NOT REPRODUCIBLE"; diff .repro_a .repro_b; exit 1; }
	@rm -f .repro_a .repro_b

ingest:
	python -m src.ingestion.run

silver:
	python -m src.ingestion.run_silver

gold:
	python -m src.ingestion.run_gold

# Full local pipeline: landing -> Bronze -> Silver -> Gold, in order, no manual
# steps. Each stage is its own sub-make so ordering and fail-fast do not depend
# on -j. If a stage exits non-zero, make aborts here and later stages never run:
# a broken Bronze must not feed Silver. The banner printed last tells you which
# stage was entering when it died, which is the first thing you want at 3 AM.
# Assumes the landing already exists (run `make generate` once); regenerating
# 50M rows on every pipeline run is not the job. This is the LOCAL pipeline; the
# cloud path stays the two Databricks UC notebooks from W6.
# No cleanup between stages on purpose: idempotency is the property we want to
# be able to verify, and cleaning would hide it.
pipeline:
	@echo "=== [1/3] INGEST: landing -> Bronze ==="
	@$(MAKE) --no-print-directory ingest
	@echo "=== [2/3] SILVER: Bronze -> Silver ==="
	@$(MAKE) --no-print-directory silver
	@echo "=== [3/3] GOLD:   Silver -> Gold ==="
	@$(MAKE) --no-print-directory gold
	@echo "=== PIPELINE OK: landing -> Gold complete ==="