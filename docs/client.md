# Python client

```python
import tickdb

db = tickdb.connect()  # ClickHouse, read-only user tickdb_ro
# db = tickdb.connect("parquet")    # or read the Parquet archive directly, no server needed

trades = db.trades("binance-usdm", "BTCUSDT", "2026-11-18", "2026-11-19")  # Polars
quotes = db.quotes("kraken", "BTC/USD", "2026-11-18", "2026-11-19", as_="arrow")  # Arrow
bars = db.bars("coinbase", "BTC-USD", "2026-11-18", "2026-11-19", interval="5m", fill="forward")
joined = db.asof_trades_quotes("binance-usdm", "BTCUSDT", "2026-11-18", "2026-11-19")
db.sql(
    "SELECT venue, count() FROM tickdb.trades WHERE date = {d:Date} GROUP BY venue",
    {"d": "2026-11-18"},
)
```

Ranges are half-open `[start, end)`: ISO strings, dates, UTC datetimes or int ns. Results come
in archive order with the canonical schemas ([Schemas](schemas.md)); both backends return
equal frames.

| Method | Returns |
|---|---|
| `trades(venue, symbol, start, end, columns=, sources=)` | trades with `start <= ts_exchange < end` |
| `quotes(..., include_resync=False)` | BBO rows; rows in resync only if asked for |
| `book_l2(venue, symbol, start, end)` | incremental book rows (apply a batch at once; Kraken: keep the best 1000 levels) |
| `bars(..., interval="1m", ts="exchange", fill="none")` | any whole-second interval, from stored 1 s / 1 m bars |
| `asof_trades_quotes(..., on="exchange", allow_exact_matches=False, tolerance="5s", quote_venue=None)` | each trade with the last usable quote before it, plus `mid`, `spread`, `quote_age_ns`, `sign` |
| `sql(query, params)` | ClickHouse SQL (read-only); with `parquet`, DuckDB SQL over views `trades`, `book_l2`, `bbo`, `bars` |
| `gaps(start, end, venue=, status=)` | gap rows |
| `instruments(venue=, as_of=)` | effective-dated tick and lot sizes and fees |

## As-of semantics

The quote is the last one strictly before the trade, or at or before it with
`allow_exact_matches=True`. `on="exchange"` compares the trade's `ts_exchange` with the
quote's exchange time; `on="recv"` compares receive times, which is what was known when the
trade arrived, and is forced for cross-venue joins. Several quotes with the same time keep
the last in archive order; quotes in resync are skipped; quotes older than `tolerance` give
nulls. `sign` is +1 above the mid, -1 below, 0 at the mid or without a quote.

## Checking the data

```bash
tickdb verify --date 2026-11-18    # raw files (DuckDB) vs Parquet vs ClickHouse; exit 2 on a mismatch
tickdb replay --date 2026-11-18    # rebuild from raw and compare content hashes
```
