# Tick data platform

> Capture, store and serve tick data the way a research team would rely on it every day.

**Status:** in progress, milestone M1 (scaffolding, CI, stack, smoke test). Build spec milestones M1 to M7; docs in `docs/`.

| | |
|---|---|
| Track | Quant Developer |
| Estimated effort | 4–6 weeks |
| Stack | Python, Go or Rust ingestors; Parquet; ClickHouse, QuestDB or kdb+ |
| Data cost | Free → Paid |
| Strongest for | Data platform, research infrastructure |

## Requirements

- [ ] Ingestors for 3+ live feeds with reconnects, snapshot resync and backfill.
- [ ] Normalized schemas for trades, quotes, L2 updates and bars, keeping both exchange and local receive timestamps.
- [ ] Storage as Parquet partitioned by date and symbol, plus a query engine (ClickHouse, QuestDB or kdb+).
- [ ] Built-in as-of joins (trades to quotes), bar aggregation and resampling.
- [ ] Data-quality monitoring for gaps, duplicates, outliers and feed latency, with Prometheus and Grafana dashboards.
- [ ] A Python client that returns Arrow or Polars frames.

## Data

Raw and licensed data live in `data/`, which is git-ignored. Only download scripts and small synthetic fixtures are committed.

| Dataset | Cost | What it gives you |
|---|---|---|
| [Binance, Coinbase, Kraken, OKX websockets](https://www.okx.com/docs-v5/en/) | Free | Continuous live capture. |
| [Nasdaq ITCH samples](https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/) | Free | A bulk-load test with equity tick data. |
| [Databento](https://databento.com) | Paid | Usage-based normalized exchange data. Its DBN format is a good reference schema. |
| [kdb+ personal / KDB-X community edition](https://kx.com) | Free | The industry-standard tick database, free for non-commercial use. |

## Success criteria

### Works (correct and reproducible)

- [ ] 30+ days of continuous capture, with an automated gap report and recovered gaps.
- [ ] Idempotent ingestion: replaying a day produces identical stored data.
- [ ] Typical queries return results verified against the raw files.

### Impresses hirers

- [ ] Benchmarks for ingest rate, compression ratio, and query latency on a day of trades (target well under a second).
- [ ] An as-of join of a full day of trades to quotes in seconds.
- [ ] A public data-quality dashboard and docs a researcher could use on day one.

## Questions this project must answer

- Exchange timestamp or receive timestamp: which do you sort by?
- How do you backfill a gap without double counting?
- Why columnar storage for tick data?
