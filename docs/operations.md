# Operations

## Host (P-01, recorded 2026-11-09)

| Item | Value |
|---|---|
| `lsb_release -a` | Ubuntu 22.04.5 LTS (jammy), WSL version 2 |
| `systemctl is-system-running` | `running` (systemd is PID 1) |
| Kernel | 6.18.54.1-microsoft-standard-WSL2 |
| CPU / RAM seen from WSL | 32 logical CPUs, 62 GB |
| Docker | Docker Desktop, engine 29.9.2, Compose v5.6.1 |
| uv | 0.12.23 (WSL), 0.12.21 (Windows) |
| Python | 3.12 (`uv`), not the system 3.10 |
| Repo | `D:\trading-strategy-brainstorm\tick-data-platform` = `/mnt/d/trading-strategy-brainstorm/tick-data-platform` |
| Data root | `~/data/tick-data-platform` on WSL ext4 (`/dev/sdd`, the distro's `ext4.vhdx` on `D:`) |

`D:` is a spinning SATA HDD shared by every project. Check `D:`'s own free space from Windows,
which WSL's `df` overstates, and keep it above 150 GB:

```bash
powershell.exe -c "(Get-PSDrive D).Free/1GB"
```

## Running the stack

From the WSL Ubuntu distro, in the repo root:

```bash
make up       # creates DATA_ROOT subfolders, links data/ -> DATA_ROOT, compose up --wait
make ps
make down
```

All services use `restart: unless-stopped`, Docker JSON logs rotate at 50 MB × 5, and every
port binds to 127.0.0.1. ClickHouse keeps `max_server_memory_usage_to_ram_ratio` at 0.5.

After a host reboot Docker Desktop starts at sign-in and restarts the containers. The boot task
that starts WSL is part of the shared always-on setup (`setup/always-on.ps1`).

## Live data source

Live crypto data comes from crypto-lob-alpha's `lob-recorder` systemd service, which is the one
shared capture on this PC (see [decisions](decisions.md)). This project only reads its files
under `~/data/crypto-lob-alpha`, and must keep that I/O light because the recorder writes to the
same HDD. Never stop, restart or reconfigure `lob-recorder` from this project.

## Raw mirror of the shared capture

The capture window for AC-W-01 is 2026-11-08 to 2026-12-07 (owner ruling, see
[decisions](decisions.md)). The shared recorder deletes raw files after 7 days, so an hourly
job copies each closed hour into `$DATA_ROOT/raw/external/lob-alpha/`.

- Deploy clone: `~/tick-data-platform-deploy` (WSL ext4), branch and commit noted in the log
  below. Update it with `git pull && ~/.local/bin/uv sync --locked`.
- Log: `~/data/tick-data-platform/meta/mirror/run.log`, one JSON summary per run.
- Manifests: `~/data/tick-data-platform/meta/mirror/date=<D>.jsonl` (file, bytes, SHA-256).
- Check completeness (exit 2 if a closed hour is missing):

```bash
cd ~/tick-data-platform-deploy && ~/.local/bin/uv run --no-sync tickdb mirror status \
  --source ~/data/crypto-lob-alpha --dest ~/data/tick-data-platform \
  --from 2026-11-08 --to 2026-12-07 --exclude binance-usdm:ticker
```

The cron entry (WSL crontab of user `ze`; remove it after 2026-12-09):

```cron
# tdp-mirror: tick-data-platform read-only copy of the shared raw archive, window 2026-11-08..12-07. Remove after 2026-12-09.
25 * * * * cd /home/ze/tick-data-platform-deploy && nice -n 10 ionice -c3 /home/ze/.local/bin/uv run --no-sync tickdb mirror run --source /home/ze/data/crypto-lob-alpha --dest /home/ze/data/tick-data-platform --from 2026-11-08 --to 2026-12-07 --exclude binance-usdm:ticker --guard-path /mnt/d --floor-gb 150 --rate-mb 8 >> /home/ze/data/tick-data-platform/meta/mirror/run.log 2>&1 # tdp-mirror
```

## Incidents and owner steps

| Date (UTC) | Event |
|---|---|
| 2026-11-09 | `docker` inside Ubuntu cannot reach the daemon: `/var/run/docker.sock` (dated 2026-11-03) has no engine behind it, while `docker.exe` on Windows works. Docker Desktop's WSL integration for Ubuntu needs re-enabling by the owner (Settings → Resources → WSL integration → Ubuntu, Apply & restart). `make up` waits on this. |
| 2026-11-10 02:21 | Raw mirror backlog run (deploy clone at `ae7e4eb`): 365 files, 5.16 GB copied, no errors, 71 Binance `bookTicker` files skipped by design. `tickdb mirror status` shows 2026-11-08 and 11-09 complete on all three venues; every 11-08 copy (173 files) matches its source by SHA-256. Deploy clone updated to `e0712ef`. Installing the hourly cron entry from this session was refused, so the owner adds it (line above). Until then nothing copies new hours; each closed hour must be copied within 7 days of its date. |
| 2026-11-09 | M1 stack check through Docker Desktop from Windows (`docker.exe compose`, a throwaway project, throwaway passwords in the shell, an empty test data folder, since removed): ClickHouse, Prometheus and Grafana all reached `healthy`; the 4 ClickHouse integration tests passed (user `tickdb` works, `default` refused, memory ratio 0.5); Prometheus target up; Grafana 13.2.5 healthy with `grafana-clickhouse-datasource` 4.23.4 installed and anonymous API access refused (401). Bind mounts through the `\\wsl.localhost` path need the containers' user set to root, so they are not representative: the real run is `make up` from Ubuntu once the integration is back. |

## Tests that need local resources

```bash
make test-integration            # needs the stack up and .env loaded
uv run pytest -m live            # real venue connections (none yet beyond `tickdb smoke`)
```
