"""Daily data-quality checks DQ-05 to DQ-10 over the Parquet archive (FR-17).

One row per (date, venue, symbol, dataset) for ``trades``, ``book_l2`` and ``bbo`` goes to
``dq_daily``; flagged trades go to ``dq_outliers`` (never deleted from the archive); feed
latency per hour goes to a side table for the DQ site. Everything is computed from the
archive and the gap table, so a day can be checked after its raw files are gone.

- Duplicates (DQ-03): rows removed by the normalizer (compaction counters) and keys still
  present in the archive, which must be 0.
- Crossed books (DQ-05): BBO rows with ``bid_px >= ask_px`` outside resync. Rows the venue
  itself published (every crossed book passed the Kraken checksum) are the documented
  exception of 2026-11-12: they make the day ``warn``, not ``fail``.
- Latency (DQ-07, DQ-08): ``ts_recv - ts_exchange`` of websocket rows only, one value per
  trade or per book batch. An hour whose largest clock offset exceeds the limit is marked
  unreliable.
- Outliers (DQ-09): price or quantity not positive, price off the tick grid (relative 1e-9),
  more than ``mad_k`` rolling MADs and ``min_bps`` from the rolling median over ``window_s``
  of exchange time, or more than ``outside_bbo_pct`` outside the BBO known at receive time
  (a live quote: not in resync and at most ``quote_max_age_s`` old, the book silence
  threshold of DQ-11, so trades backfilled inside a book gap are not compared with a stale
  quote).
- Coverage (DQ-10): minutes of the day with at least one websocket message, divided by 1440
  less the venue maintenance minutes listed in ``docs/maintenance.md``.
- Clock offset (DQ-08): this project's NTP probe where it ran, else the shared recorder's
  probes of venue server time (see ``clock_offsets``); the day's largest hourly value, in ms.

Status per row: ``fail`` on any remaining duplicate key, a crossed book outside resync, an
open (unclassified) gap, coverage below ``coverage_fail_pct`` or a missing content hash;
``warn`` on coverage below ``coverage_warn_pct``, outliers, unrecovered gap seconds, a
venue-confirmed crossed book or an unreliable latency hour; otherwise ``pass``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import orjson
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tickdb import MAX_THREADS, schemas
from tickdb.compact.compactor import partition_dir
from tickdb.dq.report import load_gaps
from tickdb.timeutil import day_bounds

NS_MIN = 60_000_000_000
NS_HOUR = 3_600_000_000_000
DATASETS = ("trades", "book_l2", "bbo")
UNRECOVERED = ("open", "partial", "unrecoverable")
DQ_LATENCY = pa.schema(
    [
        pa.field("date", pa.date32(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("hour", pa.int8(), nullable=False),
        pa.field("n", pa.int64(), nullable=False),
        pa.field("p50_ms", pa.float64()),
        pa.field("p99_ms", pa.float64()),
        pa.field("max_ms", pa.float64()),
        pa.field("clock_offset_max_ms", pa.float64()),
        pa.field("unreliable", pa.bool_(), nullable=False),
    ]
)


@dataclass(frozen=True)
class Limits:
    window_s: int = 60
    mad_k: float = 10.0
    min_bps: float = 50.0
    outside_bbo_pct: float = 1.0
    clock_offset_max_ms: float = 50.0
    coverage_warn_pct: float = 99.0
    coverage_fail_pct: float = 95.0
    tick_rel_tol: float = 1e-9
    quote_max_age_s: float = 15.0

    @classmethod
    def from_config(cls, dq: Any) -> Limits:
        o = dq.outlier
        return cls(
            window_s=o.window_s, mad_k=o.mad_k, min_bps=o.min_bps,
            outside_bbo_pct=o.outside_bbo_pct, clock_offset_max_ms=dq.clock_offset_max_ms,
            coverage_warn_pct=dq.coverage_warn_pct,
            coverage_fail_pct=getattr(dq, "coverage_fail_pct", 95.0),
        )  # fmt: skip


@dataclass
class DayResult:
    venue: str
    day: date
    daily: pa.Table
    outliers: pa.Table
    latency: pa.Table
    notes: dict[str, Any] = field(default_factory=dict)

    @property
    def statuses(self) -> dict[str, str]:
        return {f"{r['symbol']} {r['dataset']}": r["status"] for r in self.daily.to_pylist()}


def _files(path: Path) -> list[str]:
    return [p.as_posix() for p in sorted(path.glob("*.parquet"))]


def maintenance_minutes(venue: str, day: date, path: Path | None = None) -> set[int]:
    """Minute indexes of ``day`` inside venue maintenance windows from ``docs/maintenance.md``
    (table rows ``| venue | start UTC | end UTC | source |`` with ``YYYY-MM-DD HH:MM``)."""
    path = path or Path(__file__).resolve().parents[3] / "docs" / "maintenance.md"
    if not path.exists():
        return set()
    out: set[int] = set()
    start_day, _ = day_bounds(day)
    pattern = re.compile(r"^\|\s*([\w-]+)\s*\|\s*(\d{4}-\d\d-\d\d \d\d:\d\d)\s*\|\s*"
                         r"(\d{4}-\d\d-\d\d \d\d:\d\d)\s*\|")  # fmt: skip
    from datetime import UTC, datetime

    for line in path.read_text(encoding="utf-8").splitlines():
        m = pattern.match(line)
        if not m or m.group(1) != venue:
            continue
        lo, hi = (
            int(datetime.strptime(m.group(i), "%Y-%m-%d %H:%M").replace(tzinfo=UTC).timestamp())
            * 1_000_000_000
            for i in (2, 3)
        )
        for minute in range(1440):
            t = start_day + minute * NS_MIN
            if lo <= t < hi:
                out.add(minute)
    return out


def clock_offsets(
    data_root: Path, shared_root: Path, venue: str, day: date
) -> tuple[pl.DataFrame, str]:
    """Per-hour host clock offset |ms| and its source.

    Preferred: this project's NTP probe (FR-21, the exporter writes
    ``meta/clock/date=D.jsonl`` every 60 s); the hour's largest |offset|. Before it existed, the
    shared recorder's probes of each venue's server time (``<shared_root>/clock``) are used:
    one probe has an error of up to half its round trip (150-300 ms), so the hour's value is
    the median offset of its probes whose round trip is at most 1.5 times the hour's median.
    """
    start, _ = day_bounds(day)
    empty = pl.DataFrame(schema={"hour": pl.Int8, "offset_ms": pl.Float64})
    ntp = data_root / "meta" / "clock" / f"date={day.isoformat()}.jsonl"
    if ntp.exists() and ntp.stat().st_size:
        df = pl.read_ndjson(ntp, schema={"ts_ns": pl.Int64, "offset_ms": pl.Float64})
        hour = ((pl.col("ts_ns") - start) // NS_HOUR).cast(pl.Int8).alias("hour")
        out = df.group_by(hour).agg(pl.col("offset_ms").abs().max()).sort("hour")
        return out, "ntp"
    files = _files(shared_root / "clock" / f"venue={venue}" / f"date={day.isoformat()}")
    if not files:
        return empty, "none"
    df = pl.read_parquet(files, columns=["ts_recv", "offset_ns", "rtt_ns"]).with_columns(
        ((pl.col("ts_recv") - start) // NS_HOUR).cast(pl.Int8).alias("hour")
    )
    good = df.filter(pl.col("rtt_ns") <= 1.5 * pl.col("rtt_ns").median().over("hour"))
    out = (
        good.group_by("hour")
        .agg((pl.col("offset_ns").median().abs() / 1e6).alias("offset_ms"))
        .sort("hour")
    )
    return out, "venue_time_probe"


class _Stream:
    """Accumulates one dataset's latency values (ms, with their hour) and minutes."""

    def __init__(self) -> None:
        self.parts: list[pl.DataFrame] = []
        self.minutes: set[int] = set()
        self.rows = 0
        self.dups = 0

    def add(self, ts_recv: pl.Series, ts_exchange: pl.Series, start: int) -> None:
        rel = ts_recv - start
        self.minutes.update((rel // NS_MIN).unique().to_list())
        self.parts.append(
            pl.DataFrame(
                {
                    "lat": (ts_recv - ts_exchange).cast(pl.Float64) / 1e6,
                    "hour": (rel // NS_HOUR).cast(pl.Int8),
                }
            )
        )

    def latency(self) -> pl.DataFrame:
        if not self.parts:
            return pl.DataFrame(schema={"lat": pl.Float64, "hour": pl.Int8})
        return pl.concat(self.parts)


def _stats(lat: pl.Series) -> tuple[float | None, float | None, float | None]:
    """p50, p99 (linear interpolation) and max of latency values in ms."""
    if lat.is_empty():
        return None, None, None
    q = lat.quantile
    return q(0.5, "linear"), q(0.99, "linear"), lat.max()  # type: ignore[return-value]


def _trades(files: list[str]) -> pl.DataFrame:
    if not files:
        return pl.DataFrame(
            schema={
                "ts_exchange": pl.Int64,
                "ts_recv": pl.Int64,
                "trade_id": pl.String,
                "price": pl.Float64,
                "qty": pl.Float64,
                "source": pl.String,
            }
        )
    return pl.read_parquet(
        files, columns=["ts_exchange", "ts_recv", "trade_id", "price", "qty", "source"]
    )


def flag_outliers(
    trades: pl.DataFrame, bbo: pl.DataFrame, tick: float | None, lim: Limits
) -> pl.DataFrame:
    """Rows (trade_id, ts_exchange, ts_recv, price, rule, ref_price, deviation_bps) for every
    rule a trade breaks (DQ-09). ``trades`` holds all sources; ``bbo`` the day's quotes."""
    cols = ["trade_id", "ts_exchange", "ts_recv", "price", "rule", "ref_price", "deviation_bps"]
    empty = pl.DataFrame(
        schema={
            "trade_id": pl.String,
            "ts_exchange": pl.Int64,
            "ts_recv": pl.Int64,
            "price": pl.Float64,
            "rule": pl.String,
            "ref_price": pl.Float64,
            "deviation_bps": pl.Float64,
        }
    )
    if trades.is_empty():
        return empty
    t = trades.sort("ts_exchange", "trade_id")
    window = f"{lim.window_s * 1_000_000_000}i"
    t = t.with_columns(
        pl.col("price").rolling_median_by("ts_exchange", window_size=window).alias("_med")
    ).with_columns((pl.col("price") - pl.col("_med")).abs().alias("_dev"))
    t = t.with_columns(
        pl.col("_dev").rolling_median_by("ts_exchange", window_size=window).alias("_mad")
    )
    bps = pl.col("_dev") / pl.col("_med") * 1e4
    parts = [
        t.filter((pl.col("price") <= 0) | (pl.col("qty") <= 0)).with_columns(
            pl.lit("non_positive").alias("rule"),
            pl.lit(None, pl.Float64).alias("ref_price"),
            pl.lit(None, pl.Float64).alias("deviation_bps"),
        ),
        t.filter((pl.col("_dev") > lim.mad_k * pl.col("_mad")) & (bps > lim.min_bps)).with_columns(
            pl.lit("rolling_mad").alias("rule"),
            pl.col("_med").alias("ref_price"),
            bps.alias("deviation_bps"),
        ),
    ]
    if tick:
        ratio = pl.col("price") / tick
        off = (ratio - ratio.round()).abs() > lim.tick_rel_tol * ratio.abs()
        parts.append(
            t.filter(off & (pl.col("price") > 0)).with_columns(
                pl.lit("tick").alias("rule"),
                pl.lit(tick).alias("ref_price"),
                pl.lit(None, pl.Float64).alias("deviation_bps"),
            )
        )
    q = bbo.select(pl.col("ts_recv").alias("_q_recv"), "bid_px", "ask_px", "in_resync")
    if not q.is_empty():
        j = t.sort("ts_recv").join_asof(
            q.sort("_q_recv"), left_on="ts_recv", right_on="_q_recv", strategy="backward",
            tolerance=int(lim.quote_max_age_s * 1e9),
        )  # fmt: skip
        j = j.filter(
            ~pl.col("in_resync") & pl.col("bid_px").is_not_null() & pl.col("ask_px").is_not_null()
        )
        f = lim.outside_bbo_pct / 100
        below = pl.col("price") < pl.col("bid_px") * (1 - f)
        above = pl.col("price") > pl.col("ask_px") * (1 + f)
        ref = pl.when(below).then(pl.col("bid_px")).otherwise(pl.col("ask_px"))
        parts.append(
            j.filter(below | above).with_columns(
                pl.lit("outside_bbo").alias("rule"),
                ref.alias("ref_price"),
                ((pl.col("price") - ref).abs() / ref * 1e4).alias("deviation_bps"),
            )
        )
    out = pl.concat([p.select(cols) for p in parts])
    return out.sort("ts_recv", "trade_id", "rule") if out.height else empty


def _book_scan(files: list[str], start: int, s: _Stream) -> tuple[int, bool]:
    """One streaming pass over book_l2: rows, latency per update batch, minutes, and whether
    (batch_id, level_idx) is strictly increasing in file order (then no key repeats)."""
    rows = 0
    increasing = True
    last: tuple[int, int] | None = None
    for f in files:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(
            batch_size=2_000_000,
            columns=["ts_exchange", "ts_recv", "batch_id", "level_idx", "is_snapshot"],
        ):
            df = pl.from_arrow(batch)
            assert isinstance(df, pl.DataFrame)
            rows += df.height
            if not df.height:
                continue
            first = (int(df["batch_id"][0]), int(df["level_idx"][0]))
            if last is not None and first <= last:
                increasing = False
            d_b = df["batch_id"].diff().slice(1)
            d_l = df["level_idx"].cast(pl.Int64).diff().slice(1)
            if ((d_b < 0) | ((d_b == 0) & (d_l <= 0))).any():
                increasing = False
            last = (int(df["batch_id"][-1]), int(df["level_idx"][-1]))
            # one value per update batch; snapshot batches may come from REST (DQ-07)
            upd = df.filter((pl.col("level_idx") == 0) & ~pl.col("is_snapshot"))
            if upd.height:
                s.add(upd["ts_recv"], upd["ts_exchange"], start)
    return rows, increasing


def _book_dups_exact(files: list[str]) -> int:
    con = duckdb.connect(config={"threads": int(MAX_THREADS)})
    listing = ", ".join(f"'{f}'" for f in files)
    row = con.execute(
        f"SELECT count(*) - count(DISTINCT (batch_id, level_idx)) FROM read_parquet([{listing}])"
    ).fetchone()
    return int(row[0]) if row else 0


def _compaction(data_root: Path, venue: str, day: date) -> dict[str, Any]:
    path = data_root / "meta" / "compaction" / f"venue={venue}" / f"date={day.isoformat()}.json"
    return orjson.loads(path.read_bytes()) if path.exists() else {}


def check_day(
    data_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    *,
    shared_root: Path,
    limits: Limits | None = None,
    instruments: pl.DataFrame | None = None,
    maintenance: Path | None = None,
) -> DayResult:
    lim = limits or Limits()
    start, _end = day_bounds(day)
    comp = _compaction(data_root, venue, day)
    if not comp:
        raise FileNotFoundError(f"{venue} {day} is not compacted: no DQ rows written")
    counters = comp.get("counters", {})
    parts = comp.get("partitions", {})
    gaps = [g for g in load_gaps(data_root, day) if g["venue"] == venue]
    clock, clock_source = clock_offsets(data_root, shared_root, venue, day)
    clock_by_hour = dict(zip(clock["hour"].to_list(), clock["offset_ms"].to_list(), strict=True))
    clock_max = max(clock_by_hour.values()) if clock_by_hour else None
    unreliable_hours = {h for h, v in clock_by_hour.items() if v > lim.clock_offset_max_ms}
    maint = maintenance_minutes(venue, day, maintenance)
    denom = 1440 - len(maint)
    if instruments is None:
        from tickdb.client import load_instruments

        instruments = load_instruments()
    rows: list[dict[str, Any]] = []
    lat_rows: list[dict[str, Any]] = []
    out_frames: list[pl.DataFrame] = []
    notes: dict[str, Any] = {"maintenance_minutes": len(maint), "dup_trades_remaining": {},
                             "book_dup_check": {}}  # fmt: skip
    for symbol in symbols:
        tick_df = instruments.filter(
            (pl.col("venue") == venue) & (pl.col("symbol") == symbol)
            & (pl.col("valid_from") <= day)
            & (pl.col("valid_to").is_null() | (pl.col("valid_to") > day))
        )  # fmt: skip
        tick = float(tick_df["tick_size"][0]) if tick_df.height else None
        bbo_files = _files(partition_dir(data_root, "bbo", venue, symbol, day))
        bbo = (
            pl.read_parquet(
                bbo_files, columns=["ts", "ts_recv", "seq", "bid_px", "ask_px", "in_resync"]
            )
            if bbo_files
            else pl.DataFrame(
                schema={
                    "ts": pl.Int64,
                    "ts_recv": pl.Int64,
                    "seq": pl.Int64,
                    "bid_px": pl.Float64,
                    "ask_px": pl.Float64,
                    "in_resync": pl.Boolean,
                }
            )
        )
        streams: dict[str, _Stream] = {d: _Stream() for d in DATASETS}
        extra: dict[str, dict[str, Any]] = {d: {} for d in DATASETS}

        # trades
        trades = _trades(_files(partition_dir(data_root, "trades", venue, symbol, day)))
        st = streams["trades"]
        st.rows = trades.height
        st.dups = trades.height - trades["trade_id"].n_unique() if trades.height else 0
        ws = trades.filter(pl.col("source") == "ws")
        if ws.height:
            st.add(ws["ts_recv"], ws["ts_exchange"], start)
        flagged = flag_outliers(trades, bbo, tick, lim)
        extra["trades"]["outliers"] = flagged["trade_id"].n_unique() if flagged.height else 0
        if flagged.height:
            out_frames.append(flagged.with_columns(pl.lit(symbol).alias("symbol")))
        by_sym = counters.get(f"dup_trades_removed/{symbol}")
        total = counters.get("dup_trades_removed", 0)
        extra["trades"]["dup_removed"] = (
            int(by_sym) if by_sym is not None else (0 if not total else None)
        )
        notes["dup_trades_remaining"][symbol] = st.dups

        # book_l2
        book_files = _files(partition_dir(data_root, "book_l2", venue, symbol, day))
        sb = streams["book_l2"]
        sb.rows, increasing = _book_scan(book_files, start, sb)
        sb.dups = 0 if increasing or not book_files else _book_dups_exact(book_files)
        notes["book_dup_check"][symbol] = "ordered scan" if increasing else "exact count"
        extra["book_l2"]["dup_removed"] = 0

        # bbo
        sq = streams["bbo"]
        sq.rows = bbo.height
        sq.dups = (
            bbo.height - bbo.select(pl.struct("ts", "seq").n_unique()).item() if bbo.height else 0
        )
        if bbo.height:
            sq.minutes.update(((bbo["ts_recv"] - start) // NS_MIN).unique().to_list())
        crossed = bbo.filter(~pl.col("in_resync") & (pl.col("bid_px") >= pl.col("ask_px"))).height
        confirmed = counters.get(f"crossed_confirmed/{symbol}", 0)
        unverified = counters.get(f"crossed_unverified/{symbol}", 0)
        extra["bbo"]["crossed"] = crossed
        extra["bbo"]["venue_confirmed"] = bool(crossed and confirmed and not unverified)
        extra["bbo"]["dup_removed"] = 0

        for dataset in DATASETS:
            s = streams[dataset]
            channel = "trades" if dataset == "trades" else "book_l2"
            g = [x for x in gaps if x["symbol"] == symbol and x["channel"] == channel]
            secs = sum(x["duration_s"] for x in g)
            unrec = sum(x["duration_s"] for x in g if x["status"] in UNRECOVERED)
            if channel == "book_l2":
                unrec = secs  # L2 history inside a gap is never recoverable on free sources
            n_open = sum(x["status"] == "open" for x in g)
            minutes = {m for m in s.minutes if 0 <= m < 1440 and m not in maint}
            coverage = round(100.0 * len(minutes) / denom, 4) if denom else 100.0
            lat = s.latency()
            if dataset != "bbo":
                for h in range(24):
                    vals = lat.filter(pl.col("hour") == h)["lat"]
                    p50, p99, mx = _stats(vals)
                    lat_rows.append({
                        "date": day, "venue": venue, "symbol": symbol, "dataset": dataset,
                        "hour": h, "n": vals.len(), "p50_ms": p50, "p99_ms": p99, "max_ms": mx,
                        "clock_offset_max_ms": clock_by_hour.get(h),
                        "unreliable": h in unreliable_hours,
                    })  # fmt: skip
            d50, d99, dmax = _stats(lat["lat"]) if dataset != "bbo" else (None, None, None)
            key = f"{dataset}/{symbol}"
            content_hash = parts.get(key, {}).get("content_hash", "")
            n_out = extra[dataset].get("outliers", 0)
            n_cross = extra[dataset].get("crossed", 0)
            confirmed_cross = extra[dataset].get("venue_confirmed", False)
            unreliable = dataset != "bbo" and bool(unreliable_hours) and lat.height > 0
            fail = (
                s.dups > 0 or (n_cross > 0 and not confirmed_cross) or n_open > 0
                or coverage < lim.coverage_fail_pct or not content_hash
            )  # fmt: skip
            warn = (
                coverage < lim.coverage_warn_pct or n_out > 0 or unrec > 0
                or confirmed_cross or unreliable
            )  # fmt: skip
            rows.append({
                "date": day, "venue": venue, "symbol": symbol, "dataset": dataset,
                "rows": s.rows, "dup_rows_removed": extra[dataset].get("dup_removed"),
                "outliers": n_out, "crossed_bbo": n_cross, "gap_count": len(g),
                "gap_seconds_total": round(secs, 3), "gap_seconds_unrecovered": round(unrec, 3),
                "coverage_pct": coverage,
                "latency_p50_ms": d50, "latency_p99_ms": d99, "latency_max_ms": dmax,
                "clock_offset_max_ms": clock_max,
                "status": "fail" if fail else ("warn" if warn else "pass"),
                "content_hash": content_hash,
            })  # fmt: skip
    outliers = (
        pl.concat(out_frames).with_columns(pl.lit(day).alias("date"), pl.lit(venue).alias("venue"))
        if out_frames
        else None
    )
    out_tbl = (
        outliers.select(schemas.DQ_OUTLIERS.names).to_arrow().cast(schemas.DQ_OUTLIERS)
        if outliers is not None
        else schemas.DQ_OUTLIERS.empty_table()
    )
    notes["unreliable_hours"] = sorted(unreliable_hours)
    notes["clock_source"] = clock_source
    return DayResult(
        venue, day,
        pa.Table.from_pylist(rows, schema=schemas.DQ_DAILY),
        out_tbl,
        pa.Table.from_pylist(lat_rows, schema=DQ_LATENCY),
        notes,
    )  # fmt: skip


TABLE_DIRS = {"dq_daily": "daily", "dq_outliers": "outliers", "dq_latency": "latency"}


def write_day(data_root: Path, result: DayResult) -> dict[str, Path]:
    """``meta/dq_<name>/date=D/venue=V.parquet`` for each table, plus the notes as JSON."""
    out: dict[str, Path] = {}
    for name, table in (
        ("dq_daily", result.daily), ("dq_outliers", result.outliers),
        ("dq_latency", result.latency),
    ):  # fmt: skip
        d = data_root / "meta" / name / f"date={result.day.isoformat()}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"venue={result.venue}.parquet"
        tmp = path.with_suffix(".tmp")
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(path)
        out[name] = path
    notes = data_root / "meta" / "dq_daily" / f"date={result.day.isoformat()}"
    (notes / f"venue={result.venue}.json").write_bytes(
        orjson.dumps(result.notes, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    )
    return out


def read_table(data_root: Path, name: str, first: date, last: date) -> pa.Table:
    schema = DQ_LATENCY if name == "dq_latency" else schemas.SCHEMAS[name]
    tables = []
    for d in sorted((data_root / "meta" / name).glob("date=*")):
        day = date.fromisoformat(d.name.removeprefix("date="))
        if first <= day <= last:
            tables += [pq.read_table(f).cast(schema) for f in sorted(d.glob("*.parquet"))]
    return pa.concat_tables(tables) if tables else schema.empty_table()


def load_day(
    client: Any, data_root: Path, venue: str, day: date, database: str = "tickdb"
) -> dict[str, int]:
    """dq_daily, dq_outliers and dq_latency -> ClickHouse with REPLACE PARTITION (idempotent),
    and the day's gap rows into ``gaps`` (ReplacingMergeTree on the gap key: a re-load replaces
    a gap's status; read it with FINAL)."""
    import uuid

    part = f"('{day.isoformat()}', '{venue}')"
    loaded = {}
    for name in ("dq_daily", "dq_outliers", "dq_latency"):
        path = data_root / "meta" / name / f"date={day.isoformat()}" / f"venue={venue}.parquet"
        schema = DQ_LATENCY if name == "dq_latency" else schemas.SCHEMAS[name]
        table = pq.read_table(path).cast(schema)
        if not table.num_rows:  # nothing that day: an empty source can't REPLACE a partition
            client.command(f"ALTER TABLE {database}.{name} DROP PARTITION {part}")
        else:
            staging = f"{database}._stage_{name}_{uuid.uuid4().hex[:12]}"
            client.command(f"CREATE TABLE {staging} AS {database}.{name}")
            try:
                client.insert_arrow(staging, table)
                client.command(
                    f"ALTER TABLE {database}.{name} REPLACE PARTITION {part} FROM {staging}"
                )
            finally:
                client.command(f"DROP TABLE IF EXISTS {staging}")
        got = client.query(
            f"SELECT count() FROM {database}.{name} WHERE date = toDate('{day.isoformat()}') "
            f"AND venue = '{venue}'"
        ).result_rows[0][0]
        if int(got) != table.num_rows:
            raise RuntimeError(
                f"{name} {venue} {day}: {got} rows in ClickHouse, {table.num_rows} in Parquet"
            )
        loaded[name] = table.num_rows
    gaps = data_root / "meta" / "gaps" / f"venue={venue}" / f"date={day.isoformat()}"
    if gaps.exists():
        table = pq.read_table(gaps).cast(schemas.GAPS)
        if table.num_rows:
            client.insert_arrow(f"{database}.gaps", table)
        loaded["gaps"] = table.num_rows
    return loaded
