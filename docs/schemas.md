# Schemas

All archive datasets use the shared canonical schemas by name (spec section 7b). Fields are
added, never renamed. Timestamps are int64 nanoseconds UTC. `symbol` is the venue-native string
(`BTCUSDT`, `BTC-USD`, `BTC/USD`), never normalized.

Print any schema with `tickdb schema print <dataset>`. The source of truth is
`src/tickdb/schemas.py`.

| Dataset | Rows | Dedupe key (DQ-03) |
|---|---|---|
| `trades` | one per venue trade; `source` is `ws`, `rest`, `vendor` or `import` | `(venue, symbol, trade_id)` |
| `book_l2` | one per price-level change; a batch is one venue message, applied atomically | `(venue, symbol, batch_id, level_idx)` |
| `bbo` | top of book, emitted only when it changes, derived from `book_l2` | `(venue, symbol, ts, seq)` |
| `bars` | 1 s and 1 m OHLCV from trades, interval `[ts, ts + Δ)` | — |
| `book_l3` | order-level events (ITCH import) | — |
| `instruments` | effective-dated tick size, lot size and fees | — |
| `gaps` | one per disconnect, sequence gap, checksum failure, silence or trade-id gap | `gap_id` |
| `dq_daily` | one per date, venue, symbol and dataset | — |
| `dq_outliers` | flagged trades, never deleted | — |

Key rules:

- **Partitioning:** `date=` and the hourly file key are the UTC date/hour of `ts_recv`, never of
  `ts_exchange` (DQ-02).
- **Replay order:** stable sort by `ts_recv`, then `batch_id` and `level_idx` (book) or
  `seq`/`trade_id` (trades, bbo), then original row order (DQ-01).
- **Kraken depth:** the book is subscribed at depth 1000 (D-06) and Kraken sends no delete
  for a level that leaves that scope. To rebuild a Kraken book from `book_l2`, keep only the
  best 1,000 levels per side after each batch, as Kraken does; otherwise stale levels survive.
- **Side:** trades `+1` buyer-initiated, `-1` seller-initiated, `0` unknown; book `+1` bid,
  `-1` ask.
