"""The client's Parquet backend on synthetic compacted days (FR-15, AC-W-08)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from tests.unit.test_normalize import DAY, binance_day, coinbase_day, kraken_day
from tickdb import schemas
from tickdb.client import Client, connect, load_instruments
from tickdb.compact.compactor import compact_day
from tickdb.config import Settings

START, END = "2026-11-09", "2026-11-10"


@pytest.fixture(scope="module")
def archive(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("archive")
    raw = binance_day(tmp / "raw")
    kraken_day(raw)
    coinbase_day(raw)
    data = tmp / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    compact_day(raw, data, "coinbase", DAY, ("BTC-USD",))
    return data


@pytest.fixture
def db(archive: Path) -> Client:
    return connect("parquet", settings=Settings(_env_file=None), root=str(archive))  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("method", "dataset", "args"),
    [
        ("trades", "trades", ("binance-usdm", "BTCUSDT", START, END)),
        ("quotes", "bbo", ("kraken", "BTC/USD", START, END)),
        ("book_l2", "book_l2", ("binance-usdm", "BTCUSDT", START, END)),
        ("bars", "bars", ("kraken", "BTC/USD", START, END)),
    ],
)
def test_canonical_dtypes_for_both_outputs(
    db: Client, method: str, dataset: str, args: tuple[str, ...]
) -> None:
    frame = getattr(db, method)(*args)
    table = getattr(db, method)(*args, as_="arrow")
    assert isinstance(frame, pl.DataFrame)
    assert isinstance(table, pa.Table)
    assert table.schema == schemas.SCHEMAS[dataset]
    assert frame.height == table.num_rows > 0
    assert pl.from_arrow(table).equals(frame)


def test_trades_range_order_and_sources(db: Client) -> None:
    trades = db.trades("binance-usdm", "BTCUSDT", START, END)
    assert isinstance(trades, pl.DataFrame)
    assert trades["trade_id"].to_list() == ["500", "501", "502"]
    assert trades["ts_recv"].is_sorted()
    later = db.trades("binance-usdm", "BTCUSDT", "2026-11-09T00:00:02.5", END)
    assert later["trade_id"].to_list() == ["502"]  # half-open on ts_exchange
    none = db.trades("binance-usdm", "BTCUSDT", START, END, sources=("rest",))
    assert none.height == 0
    cols = db.trades("binance-usdm", ["BTCUSDT"], START, END, columns=["trade_id", "price"])
    assert cols.columns == ["trade_id", "price"]


def test_quotes_resync_filter(db: Client) -> None:
    usable = db.quotes("kraken", "BTC/USD", START, END)
    every = db.quotes("kraken", "BTC/USD", START, END, include_resync=True)
    assert isinstance(usable, pl.DataFrame)
    assert isinstance(every, pl.DataFrame)
    assert every.height > usable.height
    assert not usable["in_resync"].any()


def test_bars_resample_and_forward_fill(db: Client) -> None:
    secs = db.bars("kraken", "BTC/USD", START, END, interval="1s")
    mins = db.bars("kraken", "BTC/USD", START, END, interval="1m")
    five = db.bars("kraken", "BTC/USD", START, END, interval="5m")
    assert isinstance(secs, pl.DataFrame)
    assert mins["n_trades"].sum() == secs["n_trades"].sum() == five["n_trades"].sum() == 1
    filled = db.bars("kraken", "BTC/USD", START, "2026-11-09T00:03", interval="1m", fill="forward")
    assert isinstance(filled, pl.DataFrame)
    assert filled.height == 3
    assert filled["volume"].to_list()[1:] == [0.0, 0.0]
    assert filled["close"].n_unique() == 1
    recv = db.bars("kraken", "BTC/USD", START, END, interval="1m", ts="recv")
    assert isinstance(recv, pl.DataFrame)
    assert recv["n_trades"].sum() == 1
    with pytest.raises(ValueError, match="whole number"):
        db.bars("kraken", "BTC/USD", START, END, interval="500ms")


def test_asof_via_client(db: Client) -> None:
    out = db.asof_trades_quotes("kraken", "BTC/USD", START, END, tolerance=None)
    assert isinstance(out, pl.DataFrame)
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["bid_px"] is not None
    assert row["mid"] == (row["bid_px"] + row["ask_px"]) / 2
    assert row["sign"] in (-1, 0, 1)
    recv = db.asof_trades_quotes("kraken", "BTC/USD", START, END, on="recv", tolerance="1s")
    assert isinstance(recv, pl.DataFrame)
    assert recv.height == 1
    cross = db.asof_trades_quotes(
        "coinbase", "BTC-USD", START, END, quote_venue="kraken", tolerance=None
    )
    assert isinstance(cross, pl.DataFrame)
    assert cross.height == 1  # coinbase trade joined to kraken quotes by receive time


def test_sql_passthrough_over_parquet(db: Client) -> None:
    out = db.sql("SELECT venue, count(*) AS n FROM trades GROUP BY venue ORDER BY venue")
    assert isinstance(out, pl.DataFrame)
    assert dict(zip(out["venue"], out["n"], strict=True)) == {
        "binance-usdm": 3,
        "coinbase": 1,
        "kraken": 1,
    }


def test_gaps_instruments_dq(db: Client) -> None:
    gaps = db.gaps(START, END, venue="coinbase")
    assert set(gaps["channel"]) == {"book_l2", "trades"}
    assert db.gaps(START, END, status="partial").height >= 1
    inst = db.instruments("kraken", as_of="2026-11-09")
    assert inst["symbol"].to_list() == ["BTC/USD", "ETH/USD", "SOL/USD"]
    assert db.instruments(as_of="2026-10-01").height == 0  # not valid before valid_from
    assert db.dq_daily(START, END).height == 0  # filled from M5
    assert load_instruments().schema == pl.from_arrow(schemas.INSTRUMENTS.empty_table()).schema


def test_empty_ranges_keep_the_schema(db: Client) -> None:
    out = db.trades("binance-usdm", "BTCUSDT", "2026-10-01", "2026-10-02", as_="arrow")
    assert isinstance(out, pa.Table)
    assert out.num_rows == 0
    assert out.schema == schemas.TRADES


def test_unknown_cross_venue_symbol(db: Client) -> None:
    with pytest.raises(ValueError, match="unknown instrument"):
        db.asof_trades_quotes("kraken", "DOGE/USD", START, END, quote_venue="coinbase")
