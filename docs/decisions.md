# Decisions

Owner decisions first, copied from the portfolio's `DECISIONS.md` (recorded 2026-10-31, updated
since), then the spec's decision table with its status, then every ambiguity resolved during
the build. `DECISIONS.md` overrides the spec, and the spec overrides `README.md`.

## Owner decisions that apply to this project

### Plan and access

- **Scope and pace:** all 15 projects, a few at a time, no fixed date. This is project 13; its
  build started 2026-11-09.
- **Merging:** one milestone per PR, merged with `gh pr merge <n> --auto --squash` as
  `avmhdt` once CI is green. Branch protection and auto-merge are turned on once CI exists
  (M1). If a permission check refuses the merge, the PR waits for the owner.
- **GitHub:** the active `gh` account `avmhdt` has admin on the repository; Actions is on.
- **Country:** Cayman Islands (`OWNER_COUNTRY=KY`). Never use a VPN or proxy to reach a venue.

### Data

- **One shared crypto capture (owner decision, given in this project's brief, 2026-11-09):**
  never run a second recorder of binance-usdm, coinbase or kraken. This project ingests from
  crypto-lob-alpha's live capture and files under `~/data/crypto-lob-alpha`, read-only, with
  light I/O, because that capture is live on the same HDD. If the spec's 30-day capture clock
  (M2) cannot be met this way, the options go to the owner before M2.
- **Downloads:** approved for Nasdaq ITCH samples and Binance public data
  (data.binance.vision). Raw and row-level data stay out of git; only aggregates are published.
- **ITCH (P-12, before M6):** use itch-feed-handler's canonical export of
  `01302019.NASDAQ_ITCH50` from `~/data/itch-feed-handler`.
- **Money:** nothing beyond the listed approvals; for this project USD 0 (D-03, D-04).

### Machine (shared PC)

- **Host:** the owner's Windows 11 PC, WSL2 Ubuntu 22.04.5 with systemd, Docker Desktop with the
  WSL2 backend. Containers bind-mount data under `~/data/tick-data-platform`.
- **Storage:** stay on `D:`, a spinning HDD shared by all projects. Keep `D:`'s own free space
  (not WSL's figure) above 150 GB; size ClickHouse retention to that floor, and report before
  any large load. Free space is checked at each milestone.
- **Shared limits:** downloads at most 10 MB/s; heavy runs at most 8 threads at `nice 10`;
  stop if free memory drops below 30 GB.
- **Never** restart WSL, reboot, or edit `.wslconfig`. Data never goes on `C:` or in git.
- **Multi-week runs** already live until about 2026-11-29: crypto-lob-alpha's `lob-recorder`
  (AC-W-01 window 2026-11-08 to 11-21) and event-driven-backtester's Deribit testnet run (ends
  2026-11-28 20:10 UTC). Any new multi-week run must end before the Windows Update pause
  expires on 2026-12-06, unless the owner extends it, and must run under cron or systemd.
- **Benchmarks:** published benchmark runs wait until no multi-week run is active (after about
  2026-11-29).
- **Sign-out:** lock with Win+L instead of signing out, which breaks WSL's drive access.
- **Windows time sync (2026-11-09):** `MaxPollInterval` set to 10 (1,024 s) against
  time.cloudflare.com. Don't let Ubuntu's chronyd steer the clock.
- **Publishing:** aggregates only. No AI attribution anywhere.

### Owner rulings during the build

| Date | Item | Ruling |
|---|---|---|
| 2026-11-10 | AC-W-01 30-day window (M2) | Owner, quoted: "Yes, I'll extend it (Recommended)". The window is **2026-11-08 to 2026-12-07**. The owner extends the Windows Update pause (or sets notify-only) through about 2026-12-15; crypto-lob-alpha's session keeps `lob-recorder` running to at least 2026-12-08. This project never touches the recorder. A light read-only copy of the shared raw archive starts from 2026-11-08 at once, so those days survive the recorder's 7-day raw cleanup. |
| 2026-11-09 | Retention cuts (D-10, D-11) | Owner, quoted: "Approve the cuts (Recommended)". Raw spool copies are kept **7 days** (spec default 30), still deleted only after that day's verify and replay hash pass. ClickHouse `book_l2` TTL is **14 days** (spec default 90), reloadable from Parquet. Trades, BBO, bars, the Parquet archive, gaps and DQ results keep full retention. Recorded as deviations from the spec defaults. |
| 2026-11-10 | Owner location | The owner is in Brazil, not the Cayman Islands, with no VPN or proxy. The M1 smoke test (2026-11-09) therefore ran from Brazil: all three venues reachable over websocket and REST, no geo-block, no auth. Nothing in this project's venue access depends on the declared country. |
| 2026-11-11 | AC-W-01 window after the host crash | The whole PC crashed: WSL was down from about 07:00 to 19:37:54 UTC on 2026-11-11 (about 12.6 h on every stream), far over the 72 min a day allows, so the window 2026-11-08 to 12-07 cannot pass. It is published as a failed first window (finding: host crash on 11-11). Owner, quoted: "Move both to 11-12 (Recommended)". **The 30-day window is 2026-11-12 to 2026-12-11**; the shared recorder runs to at least 2026-12-12 02:00 UTC (crypto-lob-alpha's session records that), within the Windows Update pause to about 2026-12-15 (3 days of slack). Thresholds unchanged. The mirror's cron line becomes `--to 2026-12-11`, removed after 2026-12-13 (owner edits the crontab). |

## Spec decisions (section 3)

| ID | Decision | Status |
|---|---|---|
| D-01 | Venue set | Resolved 2026-10-31: binance-usdm, coinbase, kraken. M1 smoke test on 2026-11-09 23:50 UTC reached all three over websocket and REST, with no geo-block (HTTP 403/451) and no auth requirement. |
| D-02 | Always-on host | Resolved 2026-10-31: owner's PC, WSL2 + Docker. |
| D-03 | VPS spend | Resolved 2026-10-31: not needed, USD 0. |
| D-04 | Databento spend | Default: not approved. |
| D-05 | Symbols | Default: BTC, ETH, SOL per venue, venue-native symbols (9 streams). |
| D-06 | Book depth | Binance USD-M `depth@100ms` + 1000-level snapshot; Coinbase level2 (`level2_batch` on the Exchange feed, see below); Kraken **1000**, not the default 100, because that is what the shared capture subscribes to (agent decision, M2). |
| D-07 | kdb+ comparison | Default: skip. |
| D-08 | Public DQ dashboard | Default (a): static DQ site on GitHub Pages, aggregates only. |
| D-09 | ITCH fallback | Default: defer FR-25 if itch-feed-handler output is missing. Its output exists (see above). |
| D-10 | Raw spool retention | **7 days** (owner decision 2026-11-09, spec default 30), deleted only after verify and replay pass. |
| D-11 | ClickHouse `book_l2` TTL | **14 days** (owner decision 2026-11-09, spec default 90). |
| D-12 | Docker runtime | Resolved 2026-10-31: Docker Desktop with WSL2 backend. |
| D-13 | Licence | Default: MIT (`LICENSE`, `license = "MIT"` in `pyproject.toml`). |
| D-14 | Ingestor language | Fixed: Python 3.12 asyncio. |
| D-15 | Coinbase L3 | Default: off. |

## Ambiguities resolved during the build

### M1 (2026-11-09)

1. **No recorder containers in `compose.yaml`.** The spec's compose file has one ingestor per
   venue; the owner's one-shared-capture decision forbids running them live. The stack in M1 is
   ClickHouse, Prometheus and Grafana. `config/tickdb.yaml` gains a `capture` section
   (`mode: shared`, `shared_root: ~/data/crypto-lob-alpha`) that makes the choice explicit.
2. **Coinbase uses the Exchange feed.** The spec names the Advanced Trade websocket. The shared
   capture records `wss://ws-feed.exchange.coinbase.com` channels `level2_batch`, `matches` and
   `heartbeat` (no auth needed), with REST at `api.exchange.coinbase.com`, so this project's
   config, smoke test and future normalizer follow the data that actually exists.
3. **Binance USD-M routed websocket paths.** The legacy `/stream` path is retired; trades use
   `wss://fstream.binance.com/market/stream` and depth uses `.../public/stream`
   (Binance's "Important WebSocket Change Notice"). Config key `ws_routes` holds them.
4. **The smoke test is not a recorder.** It holds one short connection per route for N seconds,
   counts frames by channel and keeps nothing, so it does not break the one-capture rule.
   Its REST probe uses the first symbol per venue: Binance `/fapi/v1/depth?limit=1000`,
   Coinbase `/products/{id}/book?level=2`, Kraken `/0/public/Depth?pair=XBTUSD&count=100`
   (Kraken REST names BTC `XBT`).
5. **Image pins.** `clickhouse/clickhouse-server:26.8.36.3` (26.8 LTS), `prom/prometheus:v3.15.1`,
   `grafana/grafana:13.2.5` with `grafana-clickhouse-datasource@4.23.4`. The spec names
   `grafana/grafana-oss`, whose tags stopped at 13.0.2 in June 2026; `grafana/grafana` is the
   same OSS build under its current name.
6. **ClickHouse users.** The image entrypoint creates user `tickdb` from `CLICKHOUSE_USER` and
   `CLICKHOUSE_PASSWORD` and removes the `default` user (NFR-08); an integration test checks
   that `default` cannot log in. The read-only user `tickdb_ro` arrives with the client in M4.
   Only the HTTP port is published, on 127.0.0.1.
7. **`.env.example` comments sit on their own lines**, not after the value, so a value that
   contains `#` can never be cut by a dotenv parser. The keys are exactly the spec's.
8. **CI starts ClickHouse with `docker run` after checkout** instead of a `services:` container,
   so the repo's `config.d`, `users.d` and init SQL are mounted and tested.
9. **Deferred to the milestone that needs them:** `pages.yml` (needs P-13, before M7), the
   quickstart CI job (needs the fixture day and client, M4), Prometheus alert rules and Grafana
   dashboards (M5).
10. **WSL virtualenv.** `make` targets use `UV_PROJECT_ENVIRONMENT=~/.cache/tickdb/venv` on WSL
    ext4, so the repo on `/mnt/d` holds no Linux build tree and Windows and WSL environments
    don't collide.

### M2 (2026-11-10)

1. **The raw spool is a mirror of the shared capture.** `tickdb mirror run` copies each closed
   hour of `~/data/crypto-lob-alpha/raw/ws/venue=<v>/date=<D>/<HH>-<group>-<run>.jsonl.zst`
   byte for byte to `$DATA_ROOT/raw/external/lob-alpha/` (spec 7d's `raw/external/`), with a
   manifest line per file (bytes, SHA-256) in `meta/mirror/date=<D>.jsonl`. The files keep the
   recorder's own line format (`ts_recv	payload` for frames, `ts_recv	!<json>	<body>` for
   control records), which M3's normalizers read directly. Re-encoding them into the spec's
   spool record would double the I/O on the shared HDD for no gain: the payloads are already
   verbatim and stamped before parsing.
2. **What is copied:** everything except Binance `bookTicker` groups (`ticker.*`, about 2 GB a
   day), because `bbo` is derived from the book (FR-08). Only hours that ended at least 10
   minutes ago; a copy with the same size is skipped; copies are fsynced and renamed into place.
3. **Load on the shared PC:** reads capped at 8 MB/s, `nice 10`, `ionice` idle, one run at a
   time (file lock), at hh:25 so it doesn't overlap vol-surface-modeling's hh:15 job. The run
   stops before D: would fall under 150 GB, if D:'s free space can't be read, or if free memory
   falls under 30 GB.
4. **Deployment:** a clone on WSL ext4 at `~/tick-data-platform-deploy` with its own uv venv,
   so the job survives a broken `/mnt/d` mount. It runs under cron; see
   [operations](operations.md).
5. **Ingestors are built and tested, never run live.** FR-01 to FR-04 are implemented in
   `tickdb.ingest` and exercised against local mock venues (`tests/integration/`). While
   `capture.mode` is `shared`, `tickdb ingest run` refuses any websocket or REST endpoint that
   is not on this host, so no second recorder can start by accident.
6. **Hook signatures carry the connection.** The spec's names are kept (`subscribe`,
   `on_frame`, `resync`), each with the `Conn` it applies to: make-before-break rotation has
   two live connections for a moment, and each keeps its own book state.
7. **Parse after the stamp.** `ts_recv = time.time_ns()` is taken before anything else. The
   adapter then parses the frame to label its channel and symbol in the spool record and to
   check sequences, and only then is the record queued. The delay this adds is measured
   (`tickdb_ingest_delay_seconds`): at 5x peak, more than 99.99 % of frames were queued within
   5 ms (NFR-02).
8. **Spool events** (`kind: "event"`, channel `control`) that M3's gap detection reads:
   `connect`, `subscribe`, `disconnect`, `gap_open` (per stream on an unplanned close),
   `silence`, `seq_gap`, `checksum_fail`, `crossed`, `protocol`, `trade_id_gap`,
   `resync_start`, `resync_done`, `rotate`, `rotate_failed`, `reconnect_wait`,
   `connect_failed`, `clock`, `rest_error`.
9. **Coinbase integrity without a sequence.** `level2_batch` carries no sequence number, so
   the checks are: a snapshot before any update, no crossed book after an update (else
   resubscribe), the silence watchdog, and REST checkpoints every 600 s. `matches` trade ids
   are contiguous per product, so a hole is a `trade_id_gap`.
10. **Kraken numbers are read as their wire text** (`json.loads(..., parse_float=str)`),
    because the CRC32 covers that text, trailing zeros included.
11. **Stress rates come from this PC's capture.** The peak 1-minute rate of the streams this
    project subscribes to, read from the shared raw archive on 2026-11-10: binance-usdm 1,016
    msg/s (aggTrade + depth, 2026-11-04 15h), kraken 1,559 (book + trade, 11-05 15h),
    coinbase 354 (11-08 21h). The recorder's own venue rate for Binance (up to 10,571 msg/s)
    is mostly `bookTicker`, which this project doesn't subscribe to. The local run uses 5x
    those rates; CI runs 1x, since shared runners are not the reference hardware.
12. **Book state keeps sorted price lists** per side, so best price, top-N and truncation
    don't scan the book: a full Coinbase book has thousands of levels, and the Kraken book is
    at depth 1000.
13. **The 24 h soak is a soak of the mirror job,** because no ingestor runs live. Pass
    criteria: at least 24 hourly runs in 24 h, none with errors or a stop, no closed hour
    missing (`tickdb mirror status`), every copied file matching its manifest hash with every
    zstd frame decoding (`tickdb mirror verify`; a frame the recorder itself cut in a crash is
    reported, not counted as a failure), and the recorder's per-minute ingest p99 during
    mirror runs no worse than otherwise (`scripts/recorder_impact.py`).
14. **Capture Day 1 is 2026-11-12** (owner ruling of 2026-11-11, after the host crash; the
    first window, 2026-11-08 to 12-07, failed). The 30-day window is 2026-11-12 to 2026-12-11.

### M3 (2026-11-11)

1. **Normalizers read the shared capture's raw format directly** (`tickdb.normalize.rawlog`):
   `ts\tpayload` frames per connection and `ts\t!<json>\t<body>` control records, merged per
   hour by `(ts_recv, connection, line)`. Connection events (`conn_open` with `standby`,
   `promote`, `conn_close`), REST bodies (`snapshot`, `checkpoint`) and the hourly `state`
   record drive the replay, so the day's archive is a pure function of its raw files.
2. **Binance snapshot bridging is stamped at the time the book became known.** The diff that
   straddles a snapshot's `lastUpdateId` can arrive before or after it. Diffs that end before
   the snapshot keep their own receive times; the snapshot batch and the diffs from the
   straddling one on are stamped with the moment both were in hand. The archive stays in
   receive-time order and replays to the correct book; original receive times stay in raw.
3. **Days start from the recorder's state or the previous day's book.** Kraken's `state`
   record carries levels (written as a snapshot batch with `conn_id = "state"`). Coinbase's
   and Binance's don't, so each compaction saves the end-of-day book (`meta/book_end/`) and the
   next day starts from it (`conn_id = "carry"`) when the same connection (Coinbase) or the
   same `u` chain (Binance) continued across midnight. Otherwise the book is unknown until a
   websocket snapshot or, for Binance, the day's first REST checkpoint.
4. **Updates of an in-sync stream are always archived**, even while this replay doesn't know
   the book's levels; derived BBO is flagged `in_resync` until it does. Nothing the venue sent
   on the active connection is dropped.
5. **Book gaps end only with a snapshot** and are then `partial` (book restored, L2 history
   lost on free sources). A Kraken checksum mismatch keeps archiving the stream, opens a
   `checksum` gap from the last message that checked out and waits for the resubscribe
   snapshot.
6. **Trade ids are contiguous on all three venues** (DQ-06, checked on 2026-11-08: no holes
   outside disconnects). Trade gaps get their missing id range and are classified after REST
   backfill: `recovered`, `partial`, `unrecoverable`, or `open` until a backfill is tried.
   Trades are sorted into archive order at the end of the day, since REST rows carry
   `ts_recv = ts_exchange` (DQ-07).
7. **Content hash (FR-10):** SHA-256 of the Arrow IPC stream of the partition in archive
   order, re-chunked into 65,536-row batches with the dataset's schema, so it doesn't depend
   on how rows were batched.
8. **ClickHouse tables are partitioned by `(date, venue)`**, the unit of compaction, so
   `REPLACE PARTITION` swaps one venue-day without touching the others. The post-load check
   compares row count and the wrap-around sum of the time column (exact integers) per symbol.
9. **Checkpoint comparisons (DQ-04):** Binance is compared once the stream reaches
   `lastUpdateId`, excluding levels touched by the straddling diff and by later diffs already
   applied (a replay cannot rewind the book); Coinbase at the response time, top 20 levels,
   excluding levels updated within ±500 ms.
10. **Bars of day D read trades of D and D+1** (trades are partitioned by receive time, bars by
    their `ts`), so bars of D are final once D+1 is compacted; the daily job recomputes them.
11. **The hourly job is the mirror's cron entry.** Intraday interim parts and same-day
    ClickHouse inserts are left to M4, where the client needs them.
12. **Retention** deletes a raw venue-day only past 7 days and with a passing replay record in
    `meta/verify/`; it leaves a tombstone so the mirror never copies the day again.
13. **The daily job runs under cron in WSL** until Docker's WSL integration is back (then the
    compose `scheduler` service). The cron line is in `docs/operations.md`; the owner adds it.
