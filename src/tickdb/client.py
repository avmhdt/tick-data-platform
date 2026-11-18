"""The tickdb Python client (FR-15): ranges, quotes, book, bars, as-of joins and SQL, from
ClickHouse (read-only user) or straight from the Parquet archive, as Polars or Arrow.

Ranges are half-open ``[start, end)`` and accept ISO-8601 strings, dates, UTC datetimes or int
ns. Trades and ``book_l2`` are filtered on ``ts_exchange``, quotes and bars on ``ts`` (exchange
time), so a query can reach into the neighbouring receive-time partitions (DQ-02); both
backends scan the day before and after. Results come back in archive order with the canonical
schemas, so the two backends return equal frames.

Example::

    import tickdb
    db = tickdb.connect()                      # ClickHouse; tickdb.connect("parquet") reads files
    trades = db.trades("binance-usdm", "BTCUSDT", "2026-11-09", "2026-11-10")
    joined = db.asof_trades_quotes("binance-usdm", "BTCUSDT", "2026-11-09", "2026-11-10")
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import polars as pl
import pyarrow as pa

from tickdb import schemas
from tickdb.compact.bars import INTERVALS, compute_bars, resample_bars
from tickdb.config import Settings
from tickdb.query import asof as asof_q
from tickdb.timeutil import TimeLike, parse_duration_ns, to_ns, utc_date

Backend = Literal["clickhouse", "parquet"]
As = Literal["polars", "arrow"]
type Frame = pl.DataFrame | pa.Table
DAY_NS = 86_400_000_000_000
TIME_COL = {"trades": "ts_exchange", "book_l2": "ts_exchange", "bbo": "ts", "bars": "ts"}
ORDER = {
    "trades": ["ts_recv", "_tid", "trade_id"],
    "book_l2": ["ts_recv", "batch_id", "level_idx"],
    "bbo": ["ts_recv", "seq"],
    "bars": ["ts"],
}


def _df(value: Any) -> pl.DataFrame:
    """``pl.from_arrow`` of a table, typed as a DataFrame."""
    df = pl.from_arrow(value)
    assert isinstance(df, pl.DataFrame)
    return df


def _out(df: pl.DataFrame, as_: As, schema: pa.Schema | None = None) -> Frame:
    """Polars as is, or Arrow cast to the canonical schema (Polars drops not-null flags)."""
    if as_ != "arrow":
        return df
    table = df.to_arrow()
    if schema is not None and table.schema.names == schema.names:
        table = table.cast(schema)
    return table


def _symbols(symbol: str | Sequence[str]) -> list[str]:
    return [symbol] if isinstance(symbol, str) else list(symbol)


def _cast(df: pl.DataFrame, schema: pa.Schema) -> pl.DataFrame:
    """Canonical column order and dtypes (strings plain, ints and floats as in the schema)."""
    table = pa.Table.from_batches([], schema=schema) if df.is_empty() else df.to_arrow()
    if df.is_empty():
        return pl.from_arrow(table)  # type: ignore[return-value]
    columns = []
    for field in schema:
        col = table.column(field.name)
        if pa.types.is_dictionary(col.type):
            col = col.cast(pa.string())
        columns.append(col.cast(field.type))
    return pl.from_arrow(pa.Table.from_arrays(columns, schema=schema))  # type: ignore[return-value]


class Client:
    """Read access to the archive. Create with :func:`connect`."""

    def __init__(self, backend: Backend, settings: Settings, root: Path) -> None:
        self.backend = backend
        self.settings = settings
        self.root = root
        self._ch: Any = None

    # ------------------------------------------------------------------ plumbing

    def _clickhouse(self) -> Any:
        if self._ch is None:
            from tickdb.load.clickhouse import connect_settings

            self._ch = connect_settings(self.settings, readonly=True)
        return self._ch

    def _days(self, start: int, end: int) -> list[date]:
        first = utc_date(start) - timedelta(days=1)
        last = utc_date(max(start, end - 1)) + timedelta(days=1)
        return [first + timedelta(days=i) for i in range((last - first).days + 1)]

    def _files(
        self, dataset: str, venue: str, symbol: str, start: int, end: int, sub: str = ""
    ) -> list[str]:
        base = self.root / dataset / f"venue={venue}" / f"symbol={quote(symbol, safe='')}"
        out = []
        for day in self._days(start, end):
            path = base / f"date={day.isoformat()}"
            path = path / sub if sub else path
            out += [str(p) for p in sorted(path.glob("*.parquet"))]
        return out

    def _read(
        self,
        dataset: str,
        venue: str,
        symbols: list[str],
        start: int,
        end: int,
        columns: Sequence[str] | None = None,
        extra: str = "",
        sub: str = "",
        table: str | None = None,
        time_col: str | None = None,
    ) -> pl.DataFrame:
        schema = schemas.SCHEMAS[dataset]
        tcol = time_col or TIME_COL[dataset]
        if self.backend == "parquet":
            files = [f for s in symbols for f in self._files(dataset, venue, s, start, end, sub)]
            if not files:
                df = pl.from_arrow(schema.empty_table())
            else:
                df = (
                    pl.scan_parquet(files)
                    .filter((pl.col(tcol) >= start) & (pl.col(tcol) < end))
                    .collect()
                )
            df = df if isinstance(df, pl.DataFrame) else pl.DataFrame()
        else:
            names = ", ".join(schema.names)
            sql = (
                f"SELECT {names} FROM tickdb.{table or dataset} WHERE venue = {{venue:String}} "
                f"AND symbol IN {{symbols:Array(String)}} AND {tcol} >= {{start:Int64}} "
                f"AND {tcol} < {{end:Int64}} {extra}"
            )
            params = {"venue": venue, "symbols": symbols, "start": start, "end": end}
            df = _df(self._clickhouse().query_arrow(sql, parameters=params))
        df = _cast(df, schema)
        keys = ORDER[dataset]
        if dataset == "trades":
            df = df.with_columns(pl.col("trade_id").cast(pl.Int64, strict=False).alias("_tid"))
        df = df.sort(["symbol", *keys], maintain_order=True)
        if "_tid" in df.columns:
            df = df.drop("_tid")
        return df.select(list(columns)) if columns else df

    # ------------------------------------------------------------------ public API

    def trades(
        self,
        venue: str,
        symbol: str | Sequence[str],
        start: TimeLike,
        end: TimeLike,
        *,
        columns: Sequence[str] | None = None,
        sources: Sequence[str] = ("ws", "rest", "vendor", "import"),
        as_: As = "polars",
    ) -> Frame:
        """Trades with ``start <= ts_exchange < end``, in archive order (DQ-01)."""
        df = self._read("trades", venue, _symbols(symbol), to_ns(start), to_ns(end))
        df = df.filter(pl.col("source").is_in(list(sources)))
        return _out(df.select(list(columns)) if columns else df, as_, schemas.TRADES)

    def quotes(
        self,
        venue: str,
        symbol: str | Sequence[str],
        start: TimeLike,
        end: TimeLike,
        *,
        include_resync: bool = False,
        as_: As = "polars",
    ) -> Frame:
        """BBO rows with ``start <= ts < end``; rows in resync only if asked for (DQ-05)."""
        df = self._read("bbo", venue, _symbols(symbol), to_ns(start), to_ns(end))
        return _out(df if include_resync else df.filter(~pl.col("in_resync")), as_, schemas.BBO)

    def book_l2(
        self, venue: str, symbol: str, start: TimeLike, end: TimeLike, *, as_: As = "polars"
    ) -> Frame:
        """Incremental book rows; apply a batch at once, a snapshot batch replaces the book.
        Kraken books are at depth 1000: keep the best 1000 levels after each batch."""
        df = self._read("book_l2", venue, [symbol], to_ns(start), to_ns(end))
        return _out(df, as_, schemas.BOOK_L2)

    def bars(
        self,
        venue: str,
        symbol: str | Sequence[str],
        start: TimeLike,
        end: TimeLike,
        interval: str = "1m",
        *,
        ts: Literal["exchange", "recv"] = "exchange",
        fill: Literal["none", "forward"] = "none",
        as_: As = "polars",
    ) -> Frame:
        """OHLCV bars for any whole-second ``interval``, from stored 1 s / 1 m bars (exchange
        time) or, for ``ts="recv"``, from trades bucketed on receive time."""
        delta = parse_duration_ns(interval)
        if delta % 1_000_000_000:
            raise ValueError("interval must be a whole number of seconds")
        lo, hi = to_ns(start), to_ns(end)
        frames: list[pl.DataFrame] = []
        for sym in _symbols(symbol):
            if ts == "recv":
                trades = self._read("trades", venue, [sym], lo - DAY_NS, hi + DAY_NS)
                trades = trades.with_columns(pl.col("ts_recv").alias("ts_exchange"))
                base = _df(compute_bars(trades, "1s", lo, hi))
                base_iv = "1s"
            else:
                base_iv = "1m" if delta % INTERVALS["1m"] == 0 else "1s"
                sub, table = f"interval={base_iv}", f"bars_{base_iv}"
                base = self._read("bars", venue, [sym], lo, hi, sub=sub, table=table)
            out = base if interval == base_iv else resample_bars(base, interval)
            if fill == "forward":
                out = _forward_fill(out, venue, sym, interval, delta, lo, hi)
            frames.append(out)
        df = pl.concat(frames) if frames else pl.from_arrow(schemas.BARS.empty_table())
        return _out(_cast(df, schemas.BARS), as_, schemas.BARS)  # type: ignore[arg-type]

    def asof_trades_quotes(
        self,
        venue: str,
        symbol: str,
        start: TimeLike,
        end: TimeLike,
        *,
        on: Literal["exchange", "recv"] = "exchange",
        allow_exact_matches: bool = False,
        tolerance: str | None = "5s",
        quote_venue: str | None = None,
        quote_symbol: str | None = None,
        as_: As = "polars",
    ) -> Frame:
        """Each trade with the last usable quote before it (see :mod:`tickdb.query.asof`).
        The range applies to the join key (``ts_exchange``, or ``ts_recv`` with ``on="recv"``).
        ``quote_venue`` joins another venue's quotes (``quote_symbol`` defaults to the symbol
        with the same base asset) and forces ``on="recv"``."""
        lo, hi = to_ns(start), to_ns(end)
        tol = parse_duration_ns(tolerance) if tolerance else None
        qv = quote_venue or venue
        qs = quote_symbol or (symbol if qv == venue else _same_base(symbol, venue, qv))
        if qv != venue:
            on = "recv"
        lookback = tol if tol is not None else DAY_NS
        if self.backend == "clickhouse":
            sql = asof_q.asof_sql(on, allow_exact_matches=allow_exact_matches, tolerance_ns=tol)
            params = {"venue": venue, "symbol": symbol, "quote_venue": qv, "quote_symbol": qs,
                      "start": lo, "end": hi, "lookback": lookback}  # fmt: skip
            raw = _df(self._clickhouse().query_arrow(sql, parameters=params))
            trades = _cast(raw.select(schemas.TRADES.names), schemas.TRADES)
            extra = raw.select(["quote_ts", *asof_q.QUOTE_COLS]).with_columns(
                pl.col("quote_ts").cast(pl.Int64),
                *[pl.col(c).cast(pl.Float64) for c in asof_q.QUOTE_COLS],
            )
            joined = pl.concat([trades, extra], how="horizontal")
            tkey, _ = asof_q.keys(on)
            result = asof_q.finish(joined, tkey, tol)
        else:
            tkey, qkey = asof_q.keys(on)
            trades = self._read("trades", venue, [symbol], lo, hi, time_col=tkey)
            quotes = self._read("bbo", qv, [qs], lo - lookback, hi, time_col=qkey)
            result = asof_q.asof_polars(
                trades, quotes, on=on, allow_exact_matches=allow_exact_matches, tolerance_ns=tol
            )
        return _out(result, as_)

    def sql(
        self, query: str, params: Mapping[str, object] | None = None, *, as_: As = "polars"
    ) -> Frame:
        """ClickHouse SQL (read-only user, ``{name:Type}`` parameters); with the Parquet
        backend, DuckDB SQL over views ``trades``, ``book_l2``, ``bbo`` and ``bars``."""
        if self.backend == "clickhouse":
            table = self._clickhouse().query_arrow(query, parameters=dict(params or {}))
            return _out(pl.from_arrow(table), as_)  # type: ignore[arg-type]
        import duckdb

        con = duckdb.connect()
        for dataset in ("trades", "book_l2", "bbo", "bars"):
            pattern = self.root / dataset / "**" / "*.parquet"
            if any((self.root / dataset).glob("**/*.parquet")):
                con.execute(
                    f"CREATE VIEW {dataset} AS SELECT * FROM read_parquet('{pattern.as_posix()}')"
                )
        result = con.execute(query, dict(params or {})).to_arrow_table()
        return _out(pl.from_arrow(result), as_)  # type: ignore[arg-type]

    def gaps(
        self,
        start: TimeLike,
        end: TimeLike,
        *,
        venue: str | None = None,
        status: str | None = None,
    ) -> pl.DataFrame:
        """Gap rows whose start falls in ``[start, end)``."""
        lo, hi = to_ns(start), to_ns(end)
        frames = [
            pl.read_parquet(p / "*.parquet")
            for day in self._days(lo, hi)
            for p in (self.root / "meta" / "gaps").glob(f"venue=*/date={day.isoformat()}")
            if any(p.glob("*.parquet"))
        ]
        df = pl.concat(frames) if frames else pl.from_arrow(schemas.GAPS.empty_table())
        df = _cast(df, schemas.GAPS).filter(  # type: ignore[arg-type]
            (pl.col("start_recv") >= lo) & (pl.col("start_recv") < hi)
        )
        if venue:
            df = df.filter(pl.col("venue") == venue)
        if status:
            df = df.filter(pl.col("status") == status)
        return df.sort(["venue", "symbol", "channel", "start_recv"])

    def dq_daily(self, start: TimeLike, end: TimeLike) -> pl.DataFrame:
        """Daily data-quality rows (filled from M5 on)."""
        lo, hi = utc_date(to_ns(start)), utc_date(to_ns(end) - 1)
        files = sorted((self.root / "meta" / "dq_daily").glob("date=*/*.parquet"))
        df = (
            pl.concat([pl.read_parquet(f) for f in files])
            if files
            else pl.from_arrow(schemas.DQ_DAILY.empty_table())
        )
        df = _cast(df, schemas.DQ_DAILY)  # type: ignore[arg-type]
        return df.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))

    def instruments(self, venue: str | None = None, as_of: TimeLike | None = None) -> pl.DataFrame:
        """Effective-dated instrument metadata (DQ-12): rows valid on ``as_of`` if given."""
        df = load_instruments()
        if venue:
            df = df.filter(pl.col("venue") == venue)
        if as_of is not None:
            day = utc_date(to_ns(as_of))
            df = df.filter(
                (pl.col("valid_from") <= day)
                & (pl.col("valid_to").is_null() | (pl.col("valid_to") > day))
            )
        return df


INSTRUMENTS_CSV = Path(__file__).resolve().parents[2] / "config" / "instruments.csv"


def load_instruments(path: Path | None = None) -> pl.DataFrame:
    df = pl.read_csv(path or INSTRUMENTS_CSV, try_parse_dates=True, infer_schema_length=0)
    df = df.with_columns(
        pl.col("tick_size", "lot_size", "maker_fee_bps", "taker_fee_bps").cast(pl.Float64),
        pl.col("valid_from").str.to_date(),
        pl.col("valid_to").str.to_date(strict=False),
        pl.col("symbol_norm").replace("", None),
    )
    return _cast(df, schemas.INSTRUMENTS)


def _same_base(symbol: str, venue: str, other: str) -> str:
    inst = load_instruments()
    base = inst.filter((pl.col("venue") == venue) & (pl.col("symbol") == symbol))["base"]
    if base.is_empty():
        raise ValueError(f"unknown instrument {venue} {symbol}")
    match = inst.filter((pl.col("venue") == other) & (pl.col("base") == base[0]))["symbol"]
    if match.is_empty():
        raise ValueError(f"{other} has no instrument with base {base[0]}")
    return str(match[0])


def _forward_fill(
    bars: pl.DataFrame, venue: str, symbol: str, interval: str, delta: int, lo: int, hi: int
) -> pl.DataFrame:
    """One row per interval in [lo, hi): empty ones carry the last close, zero volume."""
    start = lo - lo % delta
    grid = pl.DataFrame({"ts": pl.int_range(start, hi, delta, eager=True, dtype=pl.Int64)})
    full = grid.join(bars, on="ts", how="left").sort("ts")
    close = pl.col("close").forward_fill()
    zero = ["volume", "buy_volume", "sell_volume", "notional"]
    return (
        full.with_columns(
            pl.lit(interval).alias("interval"),
            pl.lit(venue).alias("venue"),
            pl.lit(symbol).alias("symbol"),
            *[pl.col(c).fill_null(close) for c in ("open", "high", "low")],
            close.alias("close"),
            pl.col("vwap").fill_null(close),
            *[pl.col(c).fill_null(0.0) for c in zero],
            pl.col("n_trades").fill_null(0),
        )
        .filter(pl.col("close").is_not_null())
        .select(schemas.BARS.names)
    )


def connect(
    backend: Backend = "clickhouse",
    *,
    settings: Settings | None = None,
    root: str | None = None,
) -> Client:
    """A read-only client. ``backend="parquet"`` reads the archive files (no server needed)."""
    settings = settings or Settings()
    return Client(backend, settings, Path(root).expanduser() if root else settings.data_dir)
