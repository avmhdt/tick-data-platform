# Quickstart

Everything below runs inside the WSL2 Ubuntu distro from the repository root, which lives on
`/mnt/d`. Data never goes there: it lives under `DATA_ROOT` on the WSL ext4 filesystem.

## 1. Environment

```bash
bash scripts/gen_env.sh      # creates .env, fills empty local passwords, never prints them
make check-env               # validates config/tickdb.yaml and .env (exit 3 if something is missing)
```

`.env.example` lists every key. Secrets live only in `.env`, which git ignores.

## 2. Venue access

```bash
uv run tickdb smoke --venue all --seconds 30
```

Each venue gets a websocket for 30 s plus one REST snapshot. The command reports frames per
channel, REST status, geo-blocks (HTTP 403/451) and auth requirements, and exits 0 only if every
venue passes. It writes nothing.

## 3. The stack

```bash
make up      # ClickHouse, Prometheus, Grafana on 127.0.0.1, data bind-mounted from DATA_ROOT
make ps
make test-integration
make down
```

| Service | URL (local only) |
|---|---|
| ClickHouse HTTP | `http://127.0.0.1:8123` (user `tickdb`) |
| Prometheus | `http://127.0.0.1:9090` |
| Grafana | `http://127.0.0.1:3000` (user `admin`) |

## 4. Other commands

```bash
uv run tickdb schema print trades   # canonical schema of a dataset
uv run tickdb clock check           # host clock offset against NTP (P-08: |offset| < 50 ms)
uv run tickdb config check          # config and .env validation, secrets never shown
```
