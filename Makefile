.PHONY: install generate verify-repro ingest quality silver gold optimize pipeline \
	verify-late-data test lint format

PYTHON ?= python

install:
	$(PYTHON) -m pip install -e ".[dev]"

# Rebuild every source's landing subtree from its fixed seed.
generate:
	$(PYTHON) -m src.generator.events
	$(PYTHON) -m src.generator.purchases

# Bit-identical guarantee: generate twice in separate processes (fresh rng each
# time), hash the whole output tree after each, fail if the two hashes differ.
verify-repro:
	@$(MAKE) --no-print-directory generate >/dev/null
	@$(PYTHON) -m src.generator.tree_hash data/landing > .repro_a
	@$(MAKE) --no-print-directory generate >/dev/null
	@$(PYTHON) -m src.generator.tree_hash data/landing > .repro_b
	@diff .repro_a .repro_b >/dev/null \
		&& echo "REPRODUCIBLE: $$(cat .repro_a)" \
		|| { echo "NOT REPRODUCIBLE"; diff .repro_a .repro_b; exit 1; }
	@rm -f .repro_a .repro_b

ingest:
	$(PYTHON) -m src.ingestion.run

quality:
	$(PYTHON) -m src.ingestion.run_quality

silver:
	$(PYTHON) -m src.ingestion.run_silver

gold:
	$(PYTHON) -m src.ingestion.run_gold

optimize:
	$(PYTHON) -m src.ingestion.run_optimize

# Full local pipeline: landing -> Bronze -> quality gate -> Silver -> Gold ->
# compaction. Each stage is its own sub-make so ordering and fail-fast do not
# depend on -j: if a stage exits non-zero, later stages never run, so a broken
# Bronze or a failed hard quality rule never feeds Silver. The banner printed
# last names the stage that was running when it failed.
# Assumes the landing exists (run `make generate` once). No cleanup between
# stages: rerunning must be a no-op on row counts, and cleaning would hide that.
pipeline:
	@echo "=== [1/5] INGEST:   landing -> Bronze ==="
	@$(MAKE) --no-print-directory ingest
	@echo "=== [2/5] QUALITY:  Bronze checks + quarantine ==="
	@$(MAKE) --no-print-directory quality
	@echo "=== [3/5] SILVER:   Bronze -> Silver ==="
	@$(MAKE) --no-print-directory silver
	@echo "=== [4/5] GOLD:     Silver -> Gold ==="
	@$(MAKE) --no-print-directory gold
	@echo "=== [5/5] OPTIMIZE: compact Bronze and Silver ==="
	@$(MAKE) --no-print-directory optimize
	@echo "=== PIPELINE OK ==="

verify-late-data:
	$(PYTHON) -m scripts.verify_late_data

test:
	$(PYTHON) -m pytest

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .
	ruff check --fix .
