# tickdb

A single-host tick data platform: live crypto order-book and trade data from Binance USD-M
futures, Coinbase and Kraken, kept as a date/symbol-partitioned Parquet archive (the source of
truth), mirrored into ClickHouse for fast queries, watched by data-quality checks in Prometheus
and Grafana, and read through a Python client that returns Polars or Arrow frames.

!!! note "Status"
    Milestone M1: scaffolding, CI, the ClickHouse / Prometheus / Grafana stack and the venue
    data-access smoke test. Ingestion, the archive and the query client follow in M2 to M4.

- [Quickstart](quickstart.md): set up the environment and run the smoke test.
- [Schemas](schemas.md): the canonical datasets and their fields.
- [Operations](operations.md): host facts, the stack and how to run it.
- [Decisions](decisions.md): owner decisions and every ambiguity resolved during the build.
