.PHONY: install test lint typecheck db-up db-down migrate run audit

install:
	pip install -e .[dev]

test:
	python -m pytest -q

lint:
	ruff check app tests

typecheck:
	mypy app

db-up:
	docker compose up -d db baserow

db-down:
	docker compose down

migrate:
	alembic upgrade head

run:
	uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

audit:
	PYTHONPATH=. python scripts/audit_providers.py
