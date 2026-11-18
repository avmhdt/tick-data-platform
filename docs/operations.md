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
  --from 2026-11-08 --to 2026-12-17 --exclude binance-usdm:ticker
```

The cron entry (WSL crontab of user `ze`; remove it after 2026-12-19):

```cron
# tdp-mirror: tick-data-platform read-only copy of the shared raw archive from 2026-11-08 (window 2026-11-18..12-17). Remove after 2026-12-19.
25 * * * * cd /home/ze/tick-data-platform-deploy && nice -n 10 ionice -c3 /home/ze/.local/bin/uv run --no-sync tickdb mirror run --source /home/ze/data/crypto-lob-alpha --dest /home/ze/data/tick-data-platform --from 2026-11-08 --to 2026-12-17 --exclude binance-usdm:ticker --guard-path /mnt/g --floor-gb 150 --rate-mb 8 >> /home/ze/data/tick-data-platform/meta/mirror/run.log 2>&1 # tdp-mirror
```

## Capture window

**Day 1 is 2026-11-18** (owner ruling, 2026-11-17). The 30-day AC-W-01 window runs from
2026-11-18 00:00 to 2026-12-17 24:00 UTC. The shared recorder keeps running to at least
2026-12-18 02:00 UTC; the Windows Update hold is extended to about 2026-12-22.

Failed windows, published as findings:

| Window | Ruled | Failed on | Cause |
|---|---|---|---|
| 2026-11-08 to 12-07 | 2026-11-10 | 2026-11-11 | Whole PC crashed; WSL down about 07:00 to 19:37:54 UTC (about 12.6 h) |
| 2026-11-12 to 12-11 | 2026-11-11 | 2026-11-12 | WSL down 2026-11-12 19:02:42 to 11-15 17:43:45 UTC (about 70.7 h) |

## Ingestor tests (M2)

The ingestors never run live (one shared capture). They are verified against mock venues:

```bash
uv run pytest tests/integration/test_ingest_faults.py tests/integration/test_crash.py
TICKDB_STRESS_FACTOR=5 TICKDB_STRESS_SECONDS=20 \
  TICKDB_STRESS_OUT=results/stress/<date>-x5.jsonl uv run pytest tests/integration/test_stress.py
```

The 5x run on 2026-11-10 (WSL, reference PC, `nice 10`) is in
`results/stress/2026-11-10-x5.jsonl`:

| Venue | Target msg/s (5x peak) | Achieved | Frames sent = spooled | Loop lag p99 | Queued within 5 ms |
|---|---|---|---|---|---|
| binance-usdm | 5,080 | 5,085 | 101,708 = 101,708 | 1.13 ms | 100 % |
| kraken (depth 1000) | 7,795 | 7,803 | 156,065 = 156,065 | 1.16 ms | 100 % |
| coinbase (5,000-level book) | 1,770 | 1,774 | 35,488 = 35,488 | 1.15 ms | 99.992 % |

## Daily job (M3)

`tickdb scheduler run --once daily` compacts yesterday (UTC) per venue, runs REST backfill of
trade gaps (with the data.binance.vision fallback), re-compacts if rows arrived, loads
ClickHouse (skipped while it isn't configured), checks the replay hash, writes the gap report
and summary, and applies retention. It runs from the ext4 deploy clone
`~/tick-data-platform-deploy` (like the mirror), never from `/mnt/d`, whose drvfs mount breaks
on a Windows sign-out and whose branch changes during development. Logs go to
`~/data/tick-data-platform/meta/scheduler/` (`job_log.csv` per step, `cron.log`) and reports to
`~/data/tick-data-platform/reports/dq/`; the aggregates are copied into the repo's
`reports/dq/` when published. Without a `.env` in the deploy clone the data root is
`~/data/tick-data-platform` and the load step is skipped; once the ClickHouse password exists,
copy `.env` into the deploy clone (`chmod 600`).

Cron line for the owner (WSL crontab of `ze`; 01:40 UTC, after the first hour of the next day
has closed and been mirrored):

```cron
# tdp-daily: tick-data-platform daily job (compaction, backfill, replay, gap reports, retention). Runs from the ext4 deploy clone.
40 1 * * * mkdir -p /home/ze/data/tick-data-platform/meta/scheduler && cd /home/ze/tick-data-platform-deploy && nice -n 10 ionice -c3 /home/ze/.local/bin/uv run --no-sync tickdb scheduler run --once daily >> /home/ze/data/tick-data-platform/meta/scheduler/cron.log 2>&1 # tdp-daily
```

The deploy clone tracks the branch that holds the scheduler (`m3-archive` until M3 merges,
then `main`); it is updated only between runs, with `git pull` and `uv sync --locked`.

## Incidents and owner steps

| Date (UTC) | Event |
|---|---|
| 2026-11-09 | `docker` inside Ubuntu cannot reach the daemon: `/var/run/docker.sock` (dated 2026-11-03) has no engine behind it, while `docker.exe` on Windows works. Docker Desktop's WSL integration for Ubuntu needs re-enabling by the owner (Settings → Resources → WSL integration → Ubuntu, Apply & restart). `make up` waits on this. |
| 2026-11-10 02:21 | Raw mirror backlog run (deploy clone at `ae7e4eb`): 365 files, 5.16 GB copied, no errors, 71 Binance `bookTicker` files skipped by design. `tickdb mirror status` shows 2026-11-08 and 11-09 complete on all three venues; every 11-08 copy (173 files) matches its source by SHA-256. Deploy clone updated to `e0712ef`. Installing the hourly cron entry from this session was refused, so the owner adds it (line above). Until then nothing copies new hours; each closed hour must be copied within 7 days of its date. |
| 2026-11-09 | M1 stack check through Docker Desktop from Windows (`docker.exe compose`, a throwaway project, throwaway passwords in the shell, an empty test data folder, since removed): ClickHouse, Prometheus and Grafana all reached `healthy`; the 4 ClickHouse integration tests passed (user `tickdb` works, `default` refused, memory ratio 0.5); Prometheus target up; Grafana 13.2.5 healthy with `grafana-clickhouse-datasource` 4.23.4 installed and anonymous API access refused (401). Bind mounts through the `\\wsl.localhost` path need the containers' user set to root, so they are not representative: the real run is `make up` from Ubuntu once the integration is back. |
| 2026-11-10 19:25 | **First cron mirror run** (deploy clone `4f3fe19`): started 19:25:01, 267 s, 263 files, 2.13 GB copied (11-10 hours 02 to 18), 365 already present, no errors. `tickdb mirror status`: every closed hour of 11-08 to 11-10 copied. Recorder ingest p99 (`scripts/recorder_impact.py`, run minutes against the 30 minutes before): median binance-usdm 215 vs 207 µs, coinbase 230 vs 266 µs, kraken 167 vs 162 µs; every maximum during the run lower than before it. **24 h soak of the mirror starts with this run** and ends after the 2026-11-11 19:25 run. |
| 2026-11-11 07:00-19:38 | **Host crash.** WSL down about 12.6 h; the recorder restarted at 19:38:56 UTC and the mirror cron came back with it (no hour lost: the recorder was down too). The mirror's 24 h soak, 12 clean runs in (11-10 19:25 to 11-11 06:25), was cut by the host, not by a job error, and restarts with the 20:25 run; judged after the 2026-11-12 20:25 run. The interrupted 11-07 copy was finished at 19:49 (163 files, 0.95 GB, no errors). |
| 2026-11-12 19:02 to 11-15 17:43 | **Second host outage, about 70.7 h** (WSL boot history). No raw exists for the shared capture in that span; the mirror's cron did not run (72 h hole in its log); the soak was cut again. |
| 2026-11-17 | Project moved off the failing D: drive to `G:\trading-strategy-brainstorm\tick-data-platform` (`/mnt/g/...`); WSL runs from `G:\WSL\Ubuntu`, Docker's disk from `G:\Docker`. WSL down 18:17:46 to 18:58:01 UTC for the move. The mirror's guard path is `/mnt/g`. The stack was recreated from `/mnt/g` so its config mounts no longer point at D:. The deploy clone moved to `m3-archive` (the daily job had failed since 11-13 with "No such command 'scheduler'"). The copy-job soak restarts with the 20:25 run. |
| 2026-11-17 | **ClickHouse loaded for 2026-11-07 to 11-11** (AC-W-03): first pass 14 of 15 venue-days matched; Kraken 11-11 SOL/USD trades were 265 rows short in ClickHouse. Cause: REST backfill pages held trades outside the requested id range from the previous day, which sat in the day's Parquet partition but ClickHouse dates by `ts_recv`. Fixed (only missing ids that fall on the day are kept, DQ-02); the affected venue-days were re-compacted and replayed (all identical), and all 15 venue-days then loaded with row counts and fingerprints equal to Parquet. |
| 2026-11-18 20:39 | **Mirror soak passed** (2026-11-17 20:25 to 11-18 20:25 UTC, deploy clone at `m3-archive`): 25 hourly runs, 0 with errors or a stop, at most 1.0 h apart, each at most 31.4 s; 282 files (3.6 GB) copied; `tickdb mirror status` shows every closed hour of 11-17 and 11-18 copied; `tickdb mirror verify` re-hashed all 437 files of those days, 0 mismatches (118 files end without a zstd frame footer: the recorder's own still-open or dead-connection files, reported not failed). Recorder ingest p99 during mirror runs vs the 30 minutes before (109 runs): median binance-usdm 203.8 vs 198.1 µs, coinbase 274.0 vs 276.9, kraken 184.9 vs 183.7; every maximum during runs lower than before. |

## Query verification (M4, AC-W-04)

`tickdb verify --date D [--venue V]` recomputes the typical queries from the mirrored raw
frames with DuckDB's own JSON functions, compares them with the Parquet archive and ClickHouse,
and appends a row to `results/experiment_log.csv` (every run, failed ones included).
For each venue the run has 18 checks:

- for each of the 3 symbols, the trade aggregates and the 1-minute OHLCV, compared with raw for each of the 2 backends;
- for each symbol, BBO at 20 seeded times and the as-of join's row count and mean spread, compared between the backends.

It needs the day's raw files, so it runs within the 7-day raw retention.

Real-data runs, 2026-11-18 (all at `nice 10`, ionice idle):

| Days | Commit | Result |
|---|---|---|
| 2026-11-08, 11-09, 11-10 | `94bcd3c` | 6 / 18 per venue. The raw files were already gone (retention), so the raw side had 0 trades. The 12 raw comparisons failed and the 6 cross-backend checks passed. These days can't be verified any more. |
| 2026-11-17 | `94bcd3c` | binance-usdm 16 / 18 and coinbase 17 / 18. Counts, ids, first and last trades all matched. Σqty and VWAP differed by about 2-3e-12 relative, from the order of a plain float sum over 1.7 M trades. Fixed in `9579e94` with compensated sums (DuckDB `fsum`, ClickHouse `sumKahan`); the tolerance is unchanged. |
| 2026-11-11, 11-16, 11-17 | `9579e94` | **18 / 18 on every venue**: 3 days × 9 streams, so AC-W-04 passes. |

2026-11-16 had never been archived, because the daily job was down from 11-13 to 11-17. For
this check it was compacted and loaded on 2026-11-18 with `tickdb compact` and `tickdb load`
(load fingerprints OK on all three venues). Its backfill, replay check and gap report have not
run yet; until they do, retention keeps its raw files.

## Tests that need local resources

```bash
make test-integration            # needs the stack up and .env loaded
uv run pytest -m live            # real venue connections (none yet beyond `tickdb smoke`)
```
