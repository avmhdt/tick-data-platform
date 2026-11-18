"""1 s and 1 m bars from trades (FR-13, spec 7b ``bars``).

Bars are bucketed on ``ts_exchange``: interval ``[ts, ts + delta)``, open/close by
``(ts_exchange, trade_id)``, volume split by aggressor side (unknown side counts in volume
only), ``vwap = notional / volume``, and no row for an empty interval. A bar's date is the
UTC date of its ``ts``. Trades are partitioned by receive time (DQ-02), so a trade made just
before midnight can sit in the next day's partition: bars of day D read the trades of D and,
when it exists, D+1. Bars of D are therefore final once D+1 has been compacted.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import polars as pl
import pyarrow as pa

from tickdb import schemas
from tickdb.timeutil import NS_PER_S

INTERVALS = {"1s": NS_PER_S, "1m": 60 * NS_PER_S}


def compute_bars(trades: pl.DataFrame, interval: str, start_ns: int, end_ns: int) -> pa.Table:
    """Bars of ``interval`` for trades with ``start_ns <= ts_exchange < end_ns``."""
    delta = INTERVALS[interval]
    if trades.is_empty():
        return schemas.BARS.empty_table()
    df = (
        trades.filter((pl.col("ts_exchange") >= start_ns) & (pl.col("ts_exchange") < end_ns))
        .with_columns(
            (pl.col("ts_exchange") - pl.col("ts_exchange") % delta).alias("ts"),
            pl.col("trade_id").cast(pl.Int64, strict=False).alias("_tid"),
        )
        .sort(["ts_exchange", "_tid", "trade_id"], maintain_order=True)
    )
    bars = (
        df.group_by(["venue", "symbol", "ts"], maintain_order=True)
        .agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("qty").sum().alias("volume"),
            pl.col("qty").filter(pl.col("side") == 1).sum().alias("buy_volume"),
            pl.col("qty").filter(pl.col("side") == -1).sum().alias("sell_volume"),
            (pl.col("price") * pl.col("qty")).sum().alias("notional"),
            pl.len().cast(pl.Int64).alias("n_trades"),
        )
        .with_columns(
            pl.lit(interval).alias("interval"),
            (pl.col("notional") / pl.col("volume")).alias("vwap"),
        )
        .sort("ts")
        .select(schemas.BARS.names)
    )
    return bars.to_arrow().cast(schemas.BARS)


def read_trades(dirs: Sequence[Path]) -> pl.DataFrame:
    frames = [
        pl.read_parquet(
            d / "*.parquet",
            columns=["venue", "symbol", "ts_exchange", "trade_id", "price", "qty", "side"],
        )
        for d in dirs
        if d.exists() and any(d.glob("*.parquet"))
    ]
    return pl.concat(frames) if frames else pl.DataFrame()


def resample_bars(bars: pl.DataFrame, every: str) -> pl.DataFrame:
    """Aggregate bars to a coarser interval (``every`` a multiple of the input interval)."""
    from tickdb.timeutil import parse_duration_ns

    delta = parse_duration_ns(every)
    keys = ["venue", "symbol", "bucket"]
    out = (
        bars.sort("ts")
        .with_columns((pl.col("ts") - pl.col("ts") % delta).alias("bucket"))
        .group_by(keys, maintain_order=True)
        .agg(
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
            pl.col("buy_volume").sum(),
            pl.col("sell_volume").sum(),
            pl.col("notional").sum(),
            pl.col("n_trades").sum(),
        )
        .rename({"bucket": "ts"})
        .with_columns(
            pl.lit(every).alias("interval"),
            (pl.col("notional") / pl.col("volume")).alias("vwap"),
        )
        .sort("ts")
    )
    return out.select(schemas.BARS.names)
