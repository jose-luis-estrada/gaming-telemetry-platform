.PHONY: generate test verify-repro

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