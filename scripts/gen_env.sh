#!/usr/bin/env bash
# Create .env from .env.example if it is missing, then fill EMPTY local passwords with random
# values. Never prints a value, never changes a value that is already set, and sets DATA_ROOT
# to an absolute path if it still starts with "~" (docker compose does not expand "~").
# Run inside WSL from the repo root:  bash scripts/gen_env.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "created .env from .env.example"
fi
chmod 600 .env

fill() {  # fill KEY with a random value if its value is empty
  local key="$1"
  if grep -qE "^${key}=\s*$" .env; then
    local value
    value="$(openssl rand -base64 33 | tr -d '/+=\n' | cut -c1-32)"
    sed -i "s|^${key}=.*$|${key}=${value}|" .env
    echo "set ${key} (value not shown)"
  else
    echo "kept ${key} (already set or absent)"
  fi
}

fill CLICKHOUSE_PASSWORD
fill GRAFANA_ADMIN_PASSWORD
fill CLICKHOUSE_READONLY_PASSWORD

if grep -qE '^DATA_ROOT=~' .env; then
  sed -i "s|^DATA_ROOT=~|DATA_ROOT=${HOME}|" .env
  echo "DATA_ROOT made absolute under ${HOME}"
fi
