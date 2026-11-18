"""As-of join of trades to quotes (FR-14), the same semantics in Polars and ClickHouse.

- ``on="exchange"``: trade ``ts_exchange`` against quote ``ts`` (exchange time of the book
  update); ``on="recv"``: trade ``ts_recv`` against quote ``ts_recv`` (point in time: what was
  known when the trade arrived). A cross-venue join always uses ``recv``.
- The quote is the last one strictly before the trade (``allow_exact_matches=False``) or at
  or before it (``True``). Quotes flagged ``in_resync`` are never used.
- Several quotes with the same key: the last in archive order (largest ``ts_recv``, then
  ``seq``) wins, in both engines, so the result is deterministic.
- ``tolerance``: a quote older than this gives nulls (``None`` = no limit).
- Output: the trade columns, ``quote_ts`` (the quote's key), ``bid_px``, ``bid_qty``,
  ``ask_px``, ``ask_qty``, ``mid``, ``spread``, ``quote_age_ns`` and ``sign`` (+1 above the
  mid, -1 below, 0 at the mid or without a quote; Lee and Ready 1991, quote test).
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

On = Literal["exchange", "recv"]
QUOTE_COLS = ["bid_px", "bid_qty", "ask_px", "ask_qty"]
OUTPUT_EXTRA = ["quote_ts", *QUOTE_COLS, "mid", "spread", "quote_age_ns", "sign"]


def keys(on: On) -> tuple[str, str]:
    """(trade key column, quote key column)."""
    return ("ts_exchange", "ts") if on == "exchange" else ("ts_recv", "ts_recv")


def prepare_quotes(quotes: pl.DataFrame, on: On) -> pl.DataFrame:
    """Usable quotes, one per key (the last in archive order)."""
    _, qkey = keys(on)
    return (
        quotes.filter(~pl.col("in_resync"))
        .sort([qkey, "ts_recv", "seq"])
        .unique(subset=[qkey], keep="last", maintain_order=True)
        .select([pl.col(qkey).alias("quote_ts"), *QUOTE_COLS])
        .sort("quote_ts")
    )


def finish(joined: pl.DataFrame, tkey: str, tolerance_ns: int | None) -> pl.DataFrame:
    age = pl.col(tkey) - pl.col("quote_ts")
    stale = pl.col("quote_ts").is_null()
    if tolerance_ns is not None:
        stale = stale | (age > tolerance_ns)
    out = joined.with_columns(
        [pl.when(stale).then(None).otherwise(pl.col(c)).alias(c) for c in ["quote_ts", *QUOTE_COLS]]
    )
    mid = (pl.col("bid_px") + pl.col("ask_px")) / 2
    return out.with_columns(
        mid.alias("mid"),
        (pl.col("ask_px") - pl.col("bid_px")).alias("spread"),
        (pl.col(tkey) - pl.col("quote_ts")).alias("quote_age_ns"),
        pl.when(pl.col("price") > mid)
        .then(1)
        .when(pl.col("price") < mid)
        .then(-1)
        .otherwise(0)
        .fill_null(0)
        .cast(pl.Int8)
        .alias("sign"),
    )


def asof_polars(
    trades: pl.DataFrame,
    quotes: pl.DataFrame,
    *,
    on: On = "exchange",
    allow_exact_matches: bool = False,
    tolerance_ns: int | None = 5_000_000_000,
) -> pl.DataFrame:
    """As-of join in Polars. ``trades``/``quotes`` are one venue-symbol each."""
    tkey, _ = keys(on)
    left = trades.with_row_index("_row").sort([tkey, "_row"])
    right = prepare_quotes(quotes, on)
    joined = left.join_asof(
        right,
        left_on=tkey,
        right_on="quote_ts",
        strategy="backward",
        allow_exact_matches=allow_exact_matches,
        check_sortedness=False,
        coalesce=False,
    )
    return finish(joined.sort("_row").drop("_row"), tkey, tolerance_ns)


def asof_sql(
    on: On = "exchange",
    *,
    allow_exact_matches: bool = False,
    tolerance_ns: int | None = 5_000_000_000,
    trade_columns: list[str] | None = None,
    database: str = "tickdb",
) -> str:
    """ClickHouse ASOF LEFT JOIN with the same semantics; parameters ``venue``, ``symbol``,
    ``quote_venue``, ``quote_symbol``, ``start``, ``end``, ``lookback`` (all server-side)."""
    tkey, qkey = keys(on)
    cols = trade_columns or [
        "ts_exchange", "ts_recv", "venue", "symbol", "trade_id", "price", "qty", "side",
        "source", "ts_fetch",
    ]  # fmt: skip
    cmp = ">=" if allow_exact_matches else ">"
    stale = "q.quote_ts IS NULL"
    if tolerance_ns is not None:
        stale += f" OR t.{tkey} - q.quote_ts > {int(tolerance_ns)}"
    pick = ", ".join(f"if({stale}, NULL, q.{c}) AS {c}" for c in QUOTE_COLS)
    trade_sel = ", ".join(f"t.{c} AS {c}" for c in cols)
    return f"""
SELECT {trade_sel},
       if({stale}, NULL, q.quote_ts) AS quote_ts,
       {pick}
FROM (
    SELECT {", ".join(cols)},
           row_number() OVER (ORDER BY ts_recv, toUInt64OrZero(trade_id), trade_id) AS _row,
           toUInt8(1) AS _k
    FROM {database}.trades
    WHERE venue = {{venue:String}} AND symbol = {{symbol:String}}
      AND {tkey} >= {{start:Int64}} AND {tkey} < {{end:Int64}}
) AS t
ASOF LEFT JOIN (
    SELECT {qkey} AS quote_ts, toUInt8(1) AS _k,
           argMax(bid_px, (ts_recv, seq)) AS bid_px, argMax(bid_qty, (ts_recv, seq)) AS bid_qty,
           argMax(ask_px, (ts_recv, seq)) AS ask_px, argMax(ask_qty, (ts_recv, seq)) AS ask_qty
    FROM {database}.bbo
    WHERE venue = {{quote_venue:String}} AND symbol = {{quote_symbol:String}} AND NOT in_resync
      AND {qkey} >= {{start:Int64}} - {{lookback:Int64}} AND {qkey} < {{end:Int64}}
    GROUP BY quote_ts
) AS q ON t._k = q._k AND t.{tkey} {cmp} q.quote_ts
ORDER BY t._row
SETTINGS join_use_nulls = 1
"""


def asof_bruteforce(
    trades: pl.DataFrame,
    quotes: pl.DataFrame,
    *,
    on: On = "exchange",
    allow_exact_matches: bool = False,
    tolerance_ns: int | None = 5_000_000_000,
) -> pl.DataFrame:
    """O(n*m) reference used by the property tests."""
    tkey, _ = keys(on)
    q = prepare_quotes(quotes, on).to_dicts()
    rows = []
    for t in trades.to_dicts():
        best = None
        for quote in q:
            ok = (
                quote["quote_ts"] <= t[tkey] if allow_exact_matches else quote["quote_ts"] < t[tkey]
            )
            if ok and (best is None or quote["quote_ts"] >= best["quote_ts"]):
                best = quote
        rows.append({**t, **(best or dict.fromkeys(["quote_ts", *QUOTE_COLS]))})
    schema: dict[str, Any] = {
        **trades.schema,
        "quote_ts": pl.Int64,
        **dict.fromkeys(QUOTE_COLS, pl.Float64),
    }
    return finish(pl.DataFrame(rows, schema=schema), tkey, tolerance_ns)
