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

## Spec decisions (section 3)

| ID | Decision | Status |
|---|---|---|
| D-01 | Venue set | Resolved 2026-10-31: binance-usdm, coinbase, kraken. M1 smoke test on 2026-11-09 23:50 UTC reached all three over websocket and REST, with no geo-block (HTTP 403/451) and no auth requirement. |
| D-02 | Always-on host | Resolved 2026-10-31: owner's PC, WSL2 + Docker. |
| D-03 | VPS spend | Resolved 2026-10-31: not needed, USD 0. |
| D-04 | Databento spend | Default: not approved. |
| D-05 | Symbols | Default: BTC, ETH, SOL per venue, venue-native symbols (9 streams). |
| D-06 | Book depth | Default: Binance USD-M `depth@100ms` + 1000-level snapshot; Coinbase level2 (`level2_batch` on the Exchange feed, see below); Kraken depth 100. |
| D-07 | kdb+ comparison | Default: skip. |
| D-08 | Public DQ dashboard | Default (a): static DQ site on GitHub Pages, aggregates only. |
| D-09 | ITCH fallback | Default: defer FR-25 if itch-feed-handler output is missing. Its output exists (see above). |
| D-10 | Raw spool retention | Default: 30 days, deleted only after verify and replay pass. |
| D-11 | ClickHouse `book_l2` TTL | Default: 90 days. |
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
