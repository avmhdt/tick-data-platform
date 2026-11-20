"""ITCH bulk-load (FR-25) from a synthetic itch-feed-handler output."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import orjson
import polars as pl
import pyarrow.parquet as pq
import pytest

from tickdb import schemas
from tickdb.load import itch
from tickdb.timeutil import day_bounds

DAY = date(2019, 1, 30)
START, END = day_bounds(DAY)
S = 1_000_000_000


def _producer(src: Path, symbol: str, report_volume: int | None) -> None:
    t0 = START + 15 * 3600 * S
    trades = pl.DataFrame(
        {
            "ts_exchange": [t0, t0 + S, t0 + 2 * S, END + 3600 * S],
            "venue": ["XNAS"] * 4,
            "symbol": [symbol] * 4,
            "trade_id": ["11", "12", "13", "14"],
            "price": [100.0, 100.5, 101.0, 99.0],
            "qty": [100.0, 50.0, 30.0, 20.0],
            "side": [1, -1, 0, 1],
            "price_ticks": [1000000, 1005000, 1010000, 990000],
            "source": ["E", "P", "C", "E"],
            "cross_type": [None, None, None, None],
            "printable": [True, True, False, True],
            "broken": [False] * 4,
            "seq": [5, 6, 7, 9],
        },
        schema_overrides={"side": pl.Int8, "cross_type": pl.String},
    ).with_columns(pl.col("ts_exchange").alias("ts_recv"))
    book = pl.DataFrame(
        {
            "ts_exchange": [t0, t0, t0, END + 10 * S],
            "venue": ["XNAS"] * 4,
            "symbol": [symbol] * 4,
            "order_id": [7, 7, 7, 8],
            "event": ["add", "exec", "exec", "add"],
            "side": [1, 1, 1, -1],
            "price_ticks": [1000000] * 4,
            "qty": [300, 100, 100, 50],
            "new_order_id": [None] * 4,
            "match_id": [None, 11, 12, None],
            "seq": [1, 2, 3, 4],
            "msg_type": ["A", "E", "E", "A"],
            "tracking_number": [0] * 4,
            "mpid": [None] * 4,
            "printable": [None, True, True, None],
        },
        schema_overrides={
            "order_id": pl.UInt64,
            "new_order_id": pl.UInt64,
            "match_id": pl.UInt64,
            "side": pl.Int8,
            "tracking_number": pl.UInt16,
            "mpid": pl.String,
            "printable": pl.Boolean,
        },
    ).with_columns(pl.col("ts_exchange").alias("ts_recv"))
    for name, df in (("trades", trades), ("book_l3", book)):
        d = src / name / "venue=XNAS" / f"symbol={symbol}" / f"date={DAY}"
        d.mkdir(parents=True)
        df.write_parquet(d / "part-0000.parquet")
    report = src / "reports" / f"build_{DAY}.json"
    report.parent.mkdir(exist_ok=True)
    body = orjson.loads(report.read_bytes()) if report.exists() else {"symbols": {}}
    if report_volume is not None:
        body["symbols"][symbol] = {"trade_volume": report_volume}
    report.write_bytes(orjson.dumps(body))


def test_import_maps_splits_and_cross_checks(tmp_path: Path) -> None:
    src, data = tmp_path / "src", tmp_path / "data"
    _producer(src, "AAPL", report_volume=170)  # 100 + 50 + 20 printable shares
    rep = itch.import_day(src, data, DAY)
    assert rep.ok, rep.as_dict()
    assert rep.symbols == ["AAPL"]
    assert rep.rows == {"trades": 3, "book_l3": 4}
    assert rep.non_printable_skipped == 1
    assert rep.dates == ["2019-01-30", "2019-01-31"]  # post-market after midnight UTC
    day1 = pq.read_table(itch.partition_dir(data, "trades", "XNAS", "AAPL", DAY))
    assert day1.schema == schemas.TRADES
    assert day1["trade_id"].to_pylist() == ["11", "12"]
    assert set(day1["source"].to_pylist()) == {"import"}
    book = pq.read_table(itch.partition_dir(data, "book_l3", "XNAS", "AAPL", DAY))
    assert book.schema == schemas.BOOK_L3
    assert book.num_rows == 3  # both execs of order 7 in the same ns are kept
    meta = orjson.loads((data / "meta" / "import" / "itch" / f"date={DAY}.json").read_bytes())
    assert meta["ok"] is True
    assert len(meta["partitions"]) == 4
    assert not (data / "interim" / "staging").exists() or not any(
        (data / "interim" / "staging").iterdir()
    )


def test_volume_mismatch_and_missing_report(tmp_path: Path) -> None:
    src, data = tmp_path / "src", tmp_path / "data"
    _producer(src, "AAPL", report_volume=999)
    _producer(src, "MSFT", report_volume=None)
    rep = itch.import_day(src, data, DAY)
    assert not rep.ok
    assert rep.volume_mismatches == {"AAPL": [170, 999]}
    assert rep.report_symbols_missing == ["MSFT"]


def test_client_reads_the_imported_day(tmp_path: Path) -> None:
    from tickdb.client import connect
    from tickdb.config import Settings

    src, data = tmp_path / "src", tmp_path / "data"
    _producer(src, "AAPL", report_volume=170)
    itch.import_day(src, data, DAY)
    db = connect("parquet", settings=Settings(_env_file=None), root=str(data))  # type: ignore[call-arg]
    trades = db.trades("XNAS", "AAPL", "2019-01-30", "2019-02-01")
    assert isinstance(trades, pl.DataFrame)
    assert trades["trade_id"].to_list() == ["11", "12", "14"]


@pytest.mark.parametrize("symbol", ["BRK.A"])
def test_symbol_names_round_trip(tmp_path: Path, symbol: str) -> None:
    src, data = tmp_path / "src", tmp_path / "data"
    _producer(src, symbol, report_volume=170)
    assert itch.symbols_in(src, DAY) == [symbol]
    assert itch.import_day(src, data, DAY).ok
