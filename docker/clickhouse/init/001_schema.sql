-- tickdb schema (FR-11). Runs once on an empty data directory; `tickdb load` also applies it
-- (every statement is IF NOT EXISTS). Tables are partitioned by (date, venue) because a
-- venue-day is the unit of compaction: a load replaces exactly that partition (REPLACE
-- PARTITION) from Parquet, so reloading never adds rows. `date` is the UTC date of ts_recv
-- for trades, book_l2 and bbo (DQ-02) and of ts for bars.

CREATE DATABASE IF NOT EXISTS tickdb;

CREATE TABLE IF NOT EXISTS tickdb.trades
(
    ts_exchange Int64 CODEC(Delta, ZSTD(3)),
    ts_recv Int64 CODEC(Delta, ZSTD(3)),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    trade_id String,
    price Float64 CODEC(ZSTD(3)),
    qty Float64 CODEC(ZSTD(3)),
    side Int8,
    source LowCardinality(String),
    ts_fetch Nullable(Int64),
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(ts_recv))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, ts_exchange, trade_id);

CREATE TABLE IF NOT EXISTS tickdb.book_l2
(
    ts_exchange Int64 CODEC(Delta, ZSTD(3)),
    ts_recv Int64 CODEC(Delta, ZSTD(3)),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    seq Int64 CODEC(Delta, ZSTD(3)),
    side Int8,
    price Float64 CODEC(ZSTD(3)),
    qty Float64 CODEC(ZSTD(3)),
    is_snapshot Bool,
    batch_id Int64 CODEC(Delta, ZSTD(3)),
    conn_id LowCardinality(Nullable(String)),
    level_idx Int32,
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(ts_recv))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, batch_id, level_idx)
TTL date + INTERVAL 14 DAY DELETE;  -- D-11 (owner decision 2026-11-09); reloadable from Parquet

CREATE TABLE IF NOT EXISTS tickdb.bbo
(
    ts Int64 CODEC(Delta, ZSTD(3)),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    bid_px Nullable(Float64),
    bid_qty Nullable(Float64),
    ask_px Nullable(Float64),
    ask_qty Nullable(Float64),
    ts_recv Int64 CODEC(Delta, ZSTD(3)),
    seq Int64 CODEC(Delta, ZSTD(3)),
    in_resync Bool,
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(ts_recv))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, ts, seq);

CREATE TABLE IF NOT EXISTS tickdb.bars_1s
(
    ts Int64 CODEC(Delta, ZSTD(3)),
    interval LowCardinality(String),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    open Float64, high Float64, low Float64, close Float64,
    volume Float64, buy_volume Float64, sell_volume Float64,
    notional Float64, vwap Float64,
    n_trades Int64,
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(ts))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, ts);

CREATE TABLE IF NOT EXISTS tickdb.bars_1m AS tickdb.bars_1s;

CREATE TABLE IF NOT EXISTS tickdb.book_l3
(
    ts_exchange Int64 CODEC(Delta, ZSTD(3)),
    ts_recv Int64 CODEC(Delta, ZSTD(3)),
    venue LowCardinality(String),
    symbol LowCardinality(String),
    order_id UInt64,
    event LowCardinality(String),
    side Int8,
    price_ticks Int64,
    qty Int64,
    new_order_id Nullable(UInt64),
    match_id Nullable(UInt64),
    seq Nullable(Int64),
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(ts_recv))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, ts_exchange, order_id, event);

CREATE TABLE IF NOT EXISTS tickdb.gaps
(
    gap_id String,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    channel LowCardinality(String),
    kind LowCardinality(String),
    start_recv Int64,
    end_recv Int64,
    start_exchange Nullable(Int64),
    end_exchange Nullable(Int64),
    seq_from Nullable(Int64),
    seq_to Nullable(Int64),
    recovery LowCardinality(String),
    status LowCardinality(String),
    recovered_rows Int64,
    date Date MATERIALIZED toDate(fromUnixTimestamp64Nano(start_recv))
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, channel, start_recv, gap_id);

CREATE TABLE IF NOT EXISTS tickdb.instruments
(
    venue LowCardinality(String),
    symbol LowCardinality(String),
    venue_symbol String,
    base String,
    quote String,
    tick_size Float64,
    lot_size Float64,
    contract_type LowCardinality(String),
    maker_fee_bps Float64,
    taker_fee_bps Float64,
    valid_from Date,
    valid_to Nullable(Date),
    source String,
    symbol_norm Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (venue, symbol, valid_from);

CREATE TABLE IF NOT EXISTS tickdb.dq_daily
(
    date Date,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    dataset LowCardinality(String),
    rows Int64,
    dup_rows_removed Int64,
    outliers Int64,
    crossed_bbo Int64,
    gap_count Int64,
    gap_seconds_total Float64,
    gap_seconds_unrecovered Float64,
    coverage_pct Float64,
    latency_p50_ms Nullable(Float64),
    latency_p99_ms Nullable(Float64),
    latency_max_ms Nullable(Float64),
    clock_offset_max_ms Nullable(Float64),
    status LowCardinality(String),
    content_hash String
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, dataset);

CREATE TABLE IF NOT EXISTS tickdb.dq_outliers
(
    date Date,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    trade_id String,
    ts_exchange Int64,
    ts_recv Int64,
    price Float64,
    rule LowCardinality(String),
    ref_price Nullable(Float64),
    deviation_bps Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY (date, venue)
ORDER BY (venue, symbol, ts_recv, trade_id, rule);
