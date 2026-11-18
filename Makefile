# Run inside the WSL2 Ubuntu distro from the repo root (/mnt/d/.../tick-data-platform).
# The virtualenv lives on WSL ext4, not in the repo on /mnt/d (spec 16).
SHELL := /bin/bash
export UV_PROJECT_ENVIRONMENT ?= $(HOME)/.cache/tickdb/venv
DATA_ROOT ?= $(shell grep -E '^DATA_ROOT=' .env 2>/dev/null | cut -d= -f2-)
COMPOSE := TICKDB_UID=$(shell id -u) TICKDB_GID=$(shell id -g) docker compose

.PHONY: help env sync up down ps logs lint test test-integration smoke clock docs data-link check-env check-monitoring dashboards dq-site

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1 | sort | tr '\n' ' '; echo

env:            ## create .env and fill empty local passwords (never printed)
	bash scripts/gen_env.sh

sync:
	uv sync --locked

check-env: sync
	uv run tickdb config check

data-link:      ## data/ -> $DATA_ROOT (git-ignored symlink)
	@test -n "$(DATA_ROOT)" || (echo "DATA_ROOT not set in .env" >&2; exit 3)
	mkdir -p "$(DATA_ROOT)"/{raw,interim,meta,meta/clock,clickhouse,prometheus,grafana}
	@test -e data || ln -s "$(DATA_ROOT)" data

up: check-env data-link
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail=100

lint: sync
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy src

test: sync
	uv run pytest --cov --cov-fail-under=85

test-integration: sync
	set -a; source .env; set +a; uv run pytest tests/integration -m integration

smoke: sync
	uv run tickdb smoke --venue all --seconds 30

clock: sync
	uv run tickdb clock check

docs: sync
	uv run mkdocs build --strict

dashboards: sync  ## regenerate docker/grafana/dashboards/*.json
	uv run python scripts/gen_dashboards.py

check-monitoring: sync  ## AC-W-09: Prometheus targets and rules, Grafana dashboards and panel queries
	set -a; source .env; set +a; uv run python scripts/check_monitoring.py

dq-site: sync  ## AC-H-04: public DQ site (aggregates only) into site-dq/
	uv run tickdb dq site --out site-dq
