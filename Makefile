.PHONY: generate test

generate:
	python -m src.generator.events

test:
	pytest tests/ -v