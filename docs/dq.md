# Data quality

Every archived venue-day gets a row per symbol and dataset (`trades`, `book_l2`, `bbo`) in
`dq_daily`. Flagged trades go to `dq_outliers`, and feed latency per hour goes to `dq_latency`.
The daily job computes them after the replay check (step `dq`) and loads them into ClickHouse.
They are computed from the Parquet archive and the gap table, so a day can be checked again
after its raw files are gone:

```bash
tickdb dq daily --date 2026-11-17               # all venues; --no-load skips ClickHouse
tickdb dq site --out site-dq                    # public static site (aggregates only)
pytest -m data tests/live/test_binance_vision.py  # AC-W-02 id-set check
```

Files: `<data root>/meta/dq_daily/date=D/venue=V.parquet` (and a `.json` of notes),
`meta/dq_outliers/...`, `meta/dq_latency/...`. The client reads them with `db.dq_daily(start, end)`.

## Checks

| Check | Rule | Column |
|---|---|---|
| Duplicates (DQ-03) | Rows removed by the normalizer (compaction counters, per symbol from M5 on). Keys still in the archive must be 0: trades `trade_id`, bbo `(ts, seq)`, book `(batch_id, level_idx)` (an ordered scan, with an exact count if the order breaks). | `dup_rows_removed` (null when only a venue total was recorded, days compacted before M5) |
| Crossed books (DQ-05) | BBO rows with `bid_px >= ask_px` outside resync. | `crossed_bbo` |
| Latency (DQ-07, DQ-08) | `ts_recv - ts_exchange` of websocket rows only, one value per trade or per book update batch (snapshot batches excluded). p50, p99 (linear interpolation) and max. | `latency_*_ms`, `dq_latency` per hour |
| Clock offset (DQ-08) | The host's NTP offset from the exporter's probe (every 60 s, `meta/clock/`). Before the probe existed, the median offset in each hour of the shared recorder's probes of venue server time, using only probes whose round trip is at most 1.5 times the hour's median. An hour above 50 ms is `unreliable` in `dq_latency`. | `clock_offset_max_ms` (largest hourly value) |
| Outliers (DQ-09) | Price or quantity not positive; price off the tick grid (relative 1e-9, point-in-time tick size); more than 10 rolling MADs **and** 50 bps from the 60 s rolling median of price on exchange time; or more than 1 % outside the BBO known at receive time. The quote must be live: not in resync and at most 15 s old (DQ-11), so trades backfilled inside a book gap aren't compared with a stale quote. | `outliers` (distinct trades), `dq_outliers` (one row per rule) |
| Gaps (FR-16) | Count and seconds of the day's gaps on the dataset's channel. Unrecovered seconds: trade gaps still `open`, `partial` or `unrecoverable`. All book gap seconds count, because L2 history inside a gap can't be recovered from free sources. | `gap_count`, `gap_seconds_total`, `gap_seconds_unrecovered` |
| Coverage (DQ-10) | Minutes of the UTC day with at least one websocket message, divided by 1440 minus the maintenance minutes in [maintenance.md](maintenance.md). | `coverage_pct` |
| Content hash | The partition's SHA-256 from compaction (FR-10). | `content_hash` |

**Status** of a row:

- `fail` on any of: a remaining duplicate key; a crossed book outside resync, unless the venue published it (every crossed book passed the Kraken checksum, the owner's ruling of 2026-11-12); an `open` gap; coverage below 95 %; a missing content hash.
- `warn` on any of: coverage below 99 %; any outlier; unrecovered gap seconds; a venue-published crossed book; an unreliable latency hour.
- `pass` otherwise.

The 95 % and 99 % levels are AC-W-01's (`dq.coverage_fail_pct`, `dq.coverage_warn_pct`).

## What the first days show (2026-11-07 to 11-17)

- **Every outage day fails on coverage.** 2026-11-07 had the ISP outage of 13:31 to 16:24 UTC, and 2026-11-11 the host crash of 07:00 to 19:38. On 11-17 the 40-minute stop for the move to G: leaves about 96.5 to 97 %, which is a `warn`.
- **Outliers cluster at market-wide fast moves.** Examples: 2026-11-08 21:55 UTC on all three venues, 2026-11-10 13:30 on Binance ETHUSDT and SOLUSDT, and 2026-11-17 17:05 on Binance SOLUSDT. Almost all are the `outside_bbo` rule. In a sweep the trade message arrives before the book update that reflects it (the streams are separate), so the last quote is a few milliseconds stale. The rule flags these by design, and nothing is deleted.
- **Latency maxima of about 141 to 144 s on 2026-11-17** are the first messages after the WSL restart at 18:58 UTC; the p99 stays under 1 s.

## Binance USD-M against data.binance.vision (AC-W-02)

`tickdb.dq.vision.check` compares the archived BTCUSDT aggregate trade ids with the ids in
data.binance.vision's daily `aggTrades` file, by exchange time:

- The archive side takes trades with `ts_exchange` on the day from the partitions of D and D+1, because a trade late on D can arrive after midnight. Every source counts.
- A day whose gaps were filled from that same file (recovery `binance_vision`) is circular and doesn't count toward the 3 days.

The result is written to `results/binance_vision_check.json` (counts only).
