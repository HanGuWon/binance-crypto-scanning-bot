.PHONY: sync test lint typecheck verify run dry-run replay outcomes
sync:
	uv sync --extra dev
test:
	uv run pytest -q
lint:
	uv run ruff check .
typecheck:
	uv run pyright
verify: lint typecheck test
	python -m compileall -q src tests
run:
	uv run signalbot run --config config/settings.example.yaml
dry-run:
	uv run signalbot run --config config/settings.example.yaml --dry-run
replay:
	uv run signalbot replay --config config/settings.example.yaml --market spot --input tests/fixtures/replay/sample_events.jsonl

outcomes:
	uv run signalbot evaluate-outcomes --config config/settings.example.yaml --input tests/fixtures/outcomes/sample.json --horizons 900 3600
