.PHONY: install test lint format clean

install:
	uv pip install -e ".[dev]"

test:
	uv run pytest tests/ -v -m "not integration"

lint:
	uv run ruff check src/

format:
	uv run ruff format src/

clean:
	rm -rf build/ dist/ *.egg-info src/*.egg-info .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
