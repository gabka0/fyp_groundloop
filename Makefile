.PHONY: install install-ml test lint format check db-up db-down m2-stress validate-postgres

install:
	python3 -m pip install -e '.[dev]'

install-ml:
	python3 -m pip install -e '.[ml,dev]'

test:
	python3 -m pytest

lint:
	python3 -m ruff check .
	python3 -m mypy src

format:
	python3 -m ruff format .
	python3 -m ruff check --fix .

check: test lint

db-up:
	docker compose up -d db

db-down:
	docker compose down

m2-stress:
	PYTHONPATH=src python3 experiments/streams/run_m2_differential.py --events 100000 --events-per-stream 100 --seed 20260718

validate-postgres:
	python3 scripts/validate_m2_postgres.py
