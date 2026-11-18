"""Query verification against the raw files (FR-23, AC-W-04).

An independent path recomputes typical queries for a venue-day: the raw frames are decoded
from the mirrored capture into JSON lines and DuckDB extracts the trades with its own JSON
functions (none of tickdb's normalizers is used). Websocket trades are compared with the
Parquet archive (DuckDB over the files) and ClickHouse (SQL):

- per symbol: trade count, sum of quantity, VWAP, first and last trade (id, price), and
  1-minute OHLCV bars on exchange time (open/close by exchange time, then trade id);
- between the backends: BBO at 20 seeded random receive times, and the as-of join's row count
  and mean spread.

Counts and ids must match exactly; float aggregates within relative 1e-12 (NFR-06).
"""

from __future__ import annotations

import csv
import json
import random
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import orjson

from tickdb.compact.compactor import partition_dir
from tickdb.normalize.rawlog import day_records
from tickdb.timeutil import day_bounds

REL_TOL = 1e-12
MARKERS = {
    "binance-usdm": b'"e":"aggTrade"',
    "coinbase": b'"type":"match"',
    "kraken": b'"channel":"trade"',
}
# DuckDB SQL that turns a JSON-lines file of {"ts_recv", "msg"} into one row per trade.
RAW_SQL = {
    "binance-usdm": """
        SELECT msg->>'$.data.s' AS symbol, CAST(msg->>'$.data.a' AS BIGINT) AS id,
               CAST(msg->>'$.data.p' AS DOUBLE) AS price, CAST(msg->>'$.data.q' AS DOUBLE) AS qty,
               CAST(msg->>'$.data.T' AS BIGINT) * 1000000 AS ts_exchange, ts_recv
        FROM src""",
    "coinbase": """
        SELECT msg->>'$.product_id' AS symbol, CAST(msg->>'$.trade_id' AS BIGINT) AS id,
               CAST(msg->>'$.price' AS DOUBLE) AS price, CAST(msg->>'$.size' AS DOUBLE) AS qty,
               epoch_us(CAST(msg->>'$.time' AS TIMESTAMPTZ)) * 1000 AS ts_exchange, ts_recv
        FROM src""",
    "kraken": """
        SELECT t->>'$.symbol' AS symbol, CAST(t->>'$.trade_id' AS BIGINT) AS id,
               CAST(t->>'$.price' AS DOUBLE) AS price, CAST(t->>'$.qty' AS DOUBLE) AS qty,
               epoch_us(CAST(t->>'$.timestamp' AS TIMESTAMPTZ)) * 1000 AS ts_exchange, ts_recv
        FROM (SELECT unnest(CAST(msg->'$.data' AS JSON[])) AS t, ts_recv FROM src)""",
}
AGG_SQL = """
    SELECT count(*) AS n_trades, sum(qty) AS sum_qty, sum(price * qty) / sum(qty) AS vwap,
           arg_min(id, (ts_exchange, id)) AS first_id, arg_min(price, (ts_exchange, id)) AS first_px,
           arg_max(id, (ts_exchange, id)) AS last_id, arg_max(price, (ts_exchange, id)) AS last_px
    FROM {table} WHERE symbol = ?"""
BARS_SQL = """
    SELECT ts_exchange - ts_exchange % 60000000000 AS ts,
           arg_min(price, (ts_exchange, id)) AS open, max(price) AS high, min(price) AS low,
           arg_max(price, (ts_exchange, id)) AS close, sum(qty) AS volume, count(*) AS n_trades
    FROM {table} WHERE symbol = ? GROUP BY 1 ORDER BY 1"""
CH_AGG = """
    SELECT count() AS n_trades, sum(qty) AS sum_qty, sum(price * qty) / sum(qty) AS vwap,
           argMin(toInt64(trade_id), (ts_exchange, toInt64(trade_id))) AS first_id,
           argMin(price, (ts_exchange, toInt64(trade_id))) AS first_px,
           argMax(toInt64(trade_id), (ts_exchange, toInt64(trade_id))) AS last_id,
           argMax(price, (ts_exchange, toInt64(trade_id))) AS last_px
    FROM tickdb.trades
    WHERE venue = {venue:String} AND symbol = {symbol:String} AND date = {day:Date} AND source = 'ws'"""
CH_BARS = """
    SELECT ts_exchange - ts_exchange % 60000000000 AS ts,
           argMin(price, (ts_exchange, toInt64(trade_id))) AS open, max(price) AS high,
           min(price) AS low, argMax(price, (ts_exchange, toInt64(trade_id))) AS close,
           sum(qty) AS volume, count() AS n_trades
    FROM tickdb.trades
    WHERE venue = {venue:String} AND symbol = {symbol:String} AND date = {day:Date} AND source = 'ws'
    GROUP BY ts ORDER BY ts"""


def close(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return bool(abs(a - b) <= REL_TOL * max(abs(a), abs(b), 1e-300))
    return bool(a == b)


def rows_equal(a: list[tuple[Any, ...]], b: list[tuple[Any, ...]]) -> bool:
    return len(a) == len(b) and all(
        len(x) == len(y) and all(close(u, v) for u, v in zip(x, y, strict=True))
        for x, y in zip(a, b, strict=True)
    )


@dataclass
class Check:
    name: str
    ok: bool
    detail: dict[str, Any] = field(default_factory=dict)


def raw_trades(raw_roots: list[Path], venue: str, day: date, out: Path) -> int:
    """Decode the day's websocket trade frames into JSON lines for DuckDB; return the count."""
    marker = MARKERS[venue]
    n = 0
    with out.open("wb") as fh:
        for rec in day_records(raw_roots, venue, day):
            if rec.ctrl is None and marker in rec.payload:
                fh.write(b'{"ts_recv":%d,"msg":%s}\n' % (rec.ts, rec.payload))
                n += 1
    return n


def verify_day(
    raw_roots: list[Path],
    data_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    *,
    clickhouse: Any | None = None,
    client_ch: Any | None = None,
    client_pq: Any | None = None,
    seed: int = 7,
) -> list[Check]:
    checks: list[Check] = []
    start, end = day_bounds(day)
    con = duckdb.connect()
    with tempfile.TemporaryDirectory() as tmp:
        frames = Path(tmp) / "frames.jsonl"
        raw_trades(raw_roots, venue, day, frames)
        con.execute(
            f"CREATE TABLE src AS SELECT * FROM read_json('{frames.as_posix()}', "
            "columns={'ts_recv': 'BIGINT', 'msg': 'JSON'}, format='newline_delimited')"
        )
        con.execute(
            "CREATE TABLE raw AS SELECT DISTINCT ON (symbol, id) * FROM ("
            + RAW_SQL[venue]
            + ") ORDER BY symbol, id, ts_recv"
        )
    for symbol in symbols:
        files = sorted(partition_dir(data_root, "trades", venue, symbol, day).glob("*.parquet"))
        pq_view = f"pq_{abs(hash(symbol))}"
        listing = ", ".join(f"'{f.as_posix()}'" for f in files) or "''"
        if files:
            con.execute(
                f"CREATE OR REPLACE VIEW {pq_view} AS SELECT symbol, CAST(trade_id AS BIGINT) AS id, "
                f"price, qty, ts_exchange, ts_recv FROM read_parquet([{listing}]) WHERE source = 'ws'"
            )
        raw_agg = con.execute(AGG_SQL.format(table="raw"), [symbol]).fetchall()
        raw_bars = con.execute(BARS_SQL.format(table="raw"), [symbol]).fetchall()
        engines: dict[str, tuple[list[Any], list[Any]]] = {}
        if files:
            engines["parquet"] = (
                con.execute(AGG_SQL.format(table=pq_view), [symbol]).fetchall(),
                con.execute(BARS_SQL.format(table=pq_view), [symbol]).fetchall(),
            )
        if clickhouse is not None:
            params = {"venue": venue, "symbol": symbol, "day": day}
            engines["clickhouse"] = (
                [tuple(r) for r in clickhouse.query(CH_AGG, parameters=params).result_rows],
                [tuple(r) for r in clickhouse.query(CH_BARS, parameters=params).result_rows],
            )
        for engine, (agg, bars) in engines.items():
            checks.append(Check(f"{symbol} trades aggregates raw={engine}",
                                rows_equal(raw_agg, agg), {"raw": raw_agg, engine: agg}))  # fmt: skip
            checks.append(Check(f"{symbol} 1m OHLCV raw={engine}", rows_equal(raw_bars, bars),
                                {"bars_raw": len(raw_bars), f"bars_{engine}": len(bars)}))  # fmt: skip
        if client_ch is not None and client_pq is not None:
            checks += _cross_backend(client_ch, client_pq, venue, symbol, start, end, seed)
    return checks


def _cross_backend(
    ch: Any, pq: Any, venue: str, symbol: str, start: int, end: int, seed: int
) -> list[Check]:
    out = []
    q_ch = ch.quotes(venue, symbol, start, end, include_resync=True)
    q_pq = pq.quotes(venue, symbol, start, end, include_resync=True)
    rng = random.Random(seed)
    picks = sorted(rng.randrange(start, end) for _ in range(20))

    def at(q: Any, t: int) -> Any:
        hit = q.filter(q["ts_recv"] <= t).tail(1)
        return None if hit.is_empty() else tuple(hit.row(0))

    same = [at(q_ch, t) == at(q_pq, t) for t in picks]
    out.append(Check(f"{symbol} BBO at 20 seeded times clickhouse=parquet", all(same),
                     {"matched": sum(same)}))  # fmt: skip
    a = ch.asof_trades_quotes(venue, symbol, start, end)
    b = pq.asof_trades_quotes(venue, symbol, start, end)
    mean_a = a["spread"].mean() if a.height else None
    mean_b = b["spread"].mean() if b.height else None
    out.append(Check(f"{symbol} as-of rows and mean spread clickhouse=parquet",
                     a.height == b.height and close(mean_a, mean_b),
                     {"rows": [a.height, b.height], "mean_spread": [mean_a, mean_b]}))  # fmt: skip
    return out


def log_experiment(
    command: str,
    params: dict[str, Any],
    metrics: dict[str, Any],
    ok: bool,
    path: Path = Path("results/experiment_log.csv"),
) -> None:
    """One row per bench, replay or verify run (spec 16): the honest trial count."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False
        ).stdout.strip()
    except OSError:
        sha = ""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new:
            writer.writerow(
                [
                    "timestamp_utc",
                    "git_sha",
                    "command",
                    "params_json",
                    "metrics_json",
                    "hardware_id",
                    "outcome",
                ]
            )
        writer.writerow([
            datetime.now(UTC).isoformat(timespec="seconds"), sha, command,
            json.dumps(params, default=str, sort_keys=True), json.dumps(metrics, default=str, sort_keys=True),
            "reference-pc-wsl", "ok" if ok else "mismatch",
        ])  # fmt: skip


def report(checks: list[Check]) -> dict[str, Any]:
    return {
        "ok": all(c.ok for c in checks),
        "checks": [
            {"name": c.name, "ok": c.ok, **({} if c.ok else {"detail": c.detail})} for c in checks
        ],
        "passed": sum(c.ok for c in checks),
        "total": len(checks),
    }


def dumps(obj: Any) -> str:
    return orjson.dumps(obj, default=str).decode()
