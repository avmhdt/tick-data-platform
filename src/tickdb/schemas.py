"""Arrow schemas for every archive dataset (spec section 7b).

Canonical fields come first, in the spec's order, and are never renamed or dropped; added
fields follow. ``venue`` and ``symbol`` are plain strings in Arrow: the Parquet writer
dictionary-encodes them on disk, and readers get strings back, as the sibling projects
(crypto-lob-alpha, itch-feed-handler) expect. Timestamps are int64 nanoseconds UTC.
"""

from __future__ import annotations

import pyarrow as pa


def _f(name: str, typ: pa.DataType, nullable: bool = False) -> pa.Field:
    return pa.field(name, typ, nullable=nullable)


TRADES = pa.schema(
    [
        _f("ts_exchange", pa.int64()),
        _f("ts_recv", pa.int64()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("trade_id", pa.string()),
        _f("price", pa.float64()),
        _f("qty", pa.float64()),
        _f("side", pa.int8()),
        # added
        _f("source", pa.string()),
        _f("ts_fetch", pa.int64(), nullable=True),
    ]
)

BOOK_L2 = pa.schema(
    [
        _f("ts_exchange", pa.int64()),
        _f("ts_recv", pa.int64()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("seq", pa.int64()),
        _f("side", pa.int8()),
        _f("price", pa.float64()),
        _f("qty", pa.float64()),
        _f("is_snapshot", pa.bool_()),
        _f("batch_id", pa.int64()),
        # added
        _f("conn_id", pa.string(), nullable=True),
        _f("level_idx", pa.int32()),
    ]
)

BOOK_L3 = pa.schema(
    [
        _f("ts_exchange", pa.int64()),
        _f("ts_recv", pa.int64()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("order_id", pa.uint64()),
        _f("event", pa.string()),
        _f("side", pa.int8()),
        _f("price_ticks", pa.int64()),
        _f("qty", pa.int64()),
        _f("new_order_id", pa.uint64(), nullable=True),
        _f("match_id", pa.uint64(), nullable=True),
        # added (producer fields kept as is, e.g. ITCH)
        _f("seq", pa.int64(), nullable=True),
    ]
)

BBO = pa.schema(
    [
        _f("ts", pa.int64()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("bid_px", pa.float64(), nullable=True),
        _f("bid_qty", pa.float64(), nullable=True),
        _f("ask_px", pa.float64(), nullable=True),
        _f("ask_qty", pa.float64(), nullable=True),
        # added
        _f("ts_recv", pa.int64()),
        _f("seq", pa.int64()),
        _f("in_resync", pa.bool_()),
    ]
)

BARS = pa.schema(
    [
        _f("ts", pa.int64()),
        _f("interval", pa.string()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("open", pa.float64()),
        _f("high", pa.float64()),
        _f("low", pa.float64()),
        _f("close", pa.float64()),
        _f("volume", pa.float64()),
        _f("buy_volume", pa.float64()),
        _f("sell_volume", pa.float64()),
        _f("notional", pa.float64()),
        _f("vwap", pa.float64()),
        _f("n_trades", pa.int64()),
    ]
)

INSTRUMENTS = pa.schema(
    [
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("venue_symbol", pa.string()),
        _f("base", pa.string()),
        _f("quote", pa.string()),
        _f("tick_size", pa.float64()),
        _f("lot_size", pa.float64()),
        _f("contract_type", pa.string()),
        _f("maker_fee_bps", pa.float64()),
        _f("taker_fee_bps", pa.float64()),
        _f("valid_from", pa.date32()),
        _f("valid_to", pa.date32(), nullable=True),
        _f("source", pa.string()),
        # added
        _f("symbol_norm", pa.string(), nullable=True),
    ]
)

GAPS = pa.schema(
    [
        _f("gap_id", pa.string()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("channel", pa.string()),
        _f("kind", pa.string()),
        _f("start_recv", pa.int64()),
        _f("end_recv", pa.int64()),
        _f("start_exchange", pa.int64(), nullable=True),
        _f("end_exchange", pa.int64(), nullable=True),
        _f("seq_from", pa.int64(), nullable=True),
        _f("seq_to", pa.int64(), nullable=True),
        _f("recovery", pa.string()),
        _f("status", pa.string()),
        _f("recovered_rows", pa.int64()),
    ]
)

DQ_DAILY = pa.schema(
    [
        _f("date", pa.date32()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("dataset", pa.string()),
        _f("rows", pa.int64()),
        _f("dup_rows_removed", pa.int64()),
        _f("outliers", pa.int64()),
        _f("crossed_bbo", pa.int64()),
        _f("gap_count", pa.int64()),
        _f("gap_seconds_total", pa.float64()),
        _f("gap_seconds_unrecovered", pa.float64()),
        _f("coverage_pct", pa.float64()),
        _f("latency_p50_ms", pa.float64(), nullable=True),
        _f("latency_p99_ms", pa.float64(), nullable=True),
        _f("latency_max_ms", pa.float64(), nullable=True),
        _f("clock_offset_max_ms", pa.float64(), nullable=True),
        _f("status", pa.string()),
        _f("content_hash", pa.string()),
    ]
)

DQ_OUTLIERS = pa.schema(
    [
        _f("date", pa.date32()),
        _f("venue", pa.string()),
        _f("symbol", pa.string()),
        _f("trade_id", pa.string()),
        _f("ts_exchange", pa.int64()),
        _f("ts_recv", pa.int64()),
        _f("price", pa.float64()),
        _f("rule", pa.string()),
        _f("ref_price", pa.float64(), nullable=True),
        _f("deviation_bps", pa.float64(), nullable=True),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "trades": TRADES,
    "book_l2": BOOK_L2,
    "book_l3": BOOK_L3,
    "bbo": BBO,
    "bars": BARS,
    "instruments": INSTRUMENTS,
    "gaps": GAPS,
    "dq_daily": DQ_DAILY,
    "dq_outliers": DQ_OUTLIERS,
}

CANONICAL: dict[str, tuple[str, ...]] = {
    "trades": ("ts_exchange", "ts_recv", "venue", "symbol", "trade_id", "price", "qty", "side"),
    "book_l2": (
        "ts_exchange",
        "ts_recv",
        "venue",
        "symbol",
        "seq",
        "side",
        "price",
        "qty",
        "is_snapshot",
        "batch_id",
    ),
    "book_l3": (
        "ts_exchange",
        "ts_recv",
        "venue",
        "symbol",
        "order_id",
        "event",
        "side",
        "price_ticks",
        "qty",
        "new_order_id",
        "match_id",
    ),
    "bbo": ("ts", "venue", "symbol", "bid_px", "bid_qty", "ask_px", "ask_qty"),
}

# Dedupe keys (DQ-03).
DEDUPE_KEYS: dict[str, tuple[str, ...]] = {
    "trades": ("venue", "symbol", "trade_id"),
    "book_l2": ("venue", "symbol", "batch_id", "level_idx"),
    "bbo": ("venue", "symbol", "ts", "seq"),
}

TRADE_SOURCES = ("ws", "rest", "vendor", "import")  # dedupe priority, best first
VENUES = ("binance-usdm", "coinbase", "kraken", "XNAS")


def get(name: str) -> pa.Schema:
    """Return the Arrow schema of dataset ``name``; raise ``KeyError`` with the known names."""
    try:
        return SCHEMAS[name]
    except KeyError:
        known = ", ".join(sorted(SCHEMAS))
        raise KeyError(f"unknown dataset {name!r}; known: {known}") from None


def describe(name: str) -> str:
    """Human-readable listing of a dataset schema, marking canonical and added fields."""
    schema = get(name)
    canonical = set(CANONICAL.get(name, tuple(schema.names)))
    lines = [f"{name}  ({len(schema)} fields)"]
    for field in schema:
        tag = "canonical" if field.name in canonical else "added"
        null = "nullable" if field.nullable else "not null"
        lines.append(f"  {field.name:<24} {field.type!s:<10} {null:<9} {tag}")
    return "\n".join(lines)
