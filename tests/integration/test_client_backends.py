"""ClickHouse and Parquet backends return equal frames; ClickHouse's ASOF JOIN equals Polars'
join_asof row for row (AC-W-07, AC-W-08). Needs ClickHouse with the read-only user."""

from __future__ import annotations

import os

import polars as pl
import pytest

from tests.unit.test_normalize import DAY, binance_day, coinbase_day, kraken_day
from tickdb.client import connect
from tickdb.compact.compactor import compact_day
from tickdb.config import Settings
from tickdb.load import clickhouse as ch

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TICKDB_TEST_CLICKHOUSE") != "1"
        or not os.environ.get("CLICKHOUSE_READONLY_PASSWORD"),
        reason="writes fixture days into ClickHouse: throwaway server only (CI sets TICKDB_TEST_CLICKHOUSE=1)",
    ),
]
START, END = "2026-11-09", "2026-11-10"
VENUES = {"binance-usdm": "BTCUSDT", "kraken": "BTC/USD", "coinbase": "BTC-USD"}


@pytest.fixture(scope="module")
def backends(tmp_path_factory: pytest.TempPathFactory) -> tuple[object, object]:
    tmp = tmp_path_factory.mktemp("backends")
    raw = binance_day(tmp / "raw")
    kraken_day(raw)
    coinbase_day(raw)
    data = tmp / "data"
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    writer = ch.connect_settings(settings)
    ch.apply_schema(writer)
    for venue, symbol in VENUES.items():
        compact_day(raw, data, venue, DAY, (symbol,))
        assert ch.load_venue_day(writer, data, venue, DAY, (symbol,)).ok
    return connect("clickhouse", settings=settings), connect(
        "parquet", settings=settings, root=str(data)
    )


@pytest.mark.parametrize("venue", list(VENUES))
@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        ("trades", {}),
        ("quotes", {}),
        ("quotes", {"include_resync": True}),
        ("book_l2", {}),
        ("bars", {"interval": "1m"}),
        ("bars", {"interval": "1s"}),
        ("bars", {"interval": "5m", "fill": "forward"}),
    ],
)
def test_backends_agree(
    backends: tuple[object, object], venue: str, method: str, kwargs: dict[str, object]
) -> None:
    clickhouse, parquet = backends
    args = (venue, VENUES[venue], START, END)
    a = getattr(clickhouse, method)(*args, **kwargs)
    b = getattr(parquet, method)(*args, **kwargs)
    assert a.schema == b.schema
    assert a.equals(b), (a, b)


@pytest.mark.parametrize("venue", list(VENUES))
@pytest.mark.parametrize("on", ["exchange", "recv"])
@pytest.mark.parametrize("exact", [False, True])
@pytest.mark.parametrize("tolerance", [None, "5s", "1ms"])
def test_asof_sql_equals_polars(
    backends: tuple[object, object], venue: str, on: str, exact: bool, tolerance: str | None
) -> None:
    clickhouse, parquet = backends
    kw = {"on": on, "allow_exact_matches": exact, "tolerance": tolerance}
    a = clickhouse.asof_trades_quotes(venue, VENUES[venue], START, END, **kw)  # type: ignore[attr-defined]
    b = parquet.asof_trades_quotes(venue, VENUES[venue], START, END, **kw)  # type: ignore[attr-defined]
    assert a.columns == b.columns
    assert a.to_dicts() == b.to_dicts()


def test_readonly_user_cannot_write(backends: tuple[object, object]) -> None:
    clickhouse, _ = backends
    with pytest.raises(Exception, match=r"(?i)readonly|not enough privileges|ACCESS_DENIED"):
        clickhouse.sql("INSERT INTO tickdb.trades (venue) VALUES ('x')")  # type: ignore[attr-defined]


def test_sql_passthrough_with_parameters(backends: tuple[object, object]) -> None:
    clickhouse, _ = backends
    out = clickhouse.sql(  # type: ignore[attr-defined]
        "SELECT count() AS n FROM tickdb.trades WHERE venue = {v:String} AND date = {d:Date}",
        {"v": "binance-usdm", "d": DAY},
    )
    assert isinstance(out, pl.DataFrame)
    assert out["n"][0] == 3


def test_verify_against_both_backends(
    backends: tuple[object, object], tmp_path_factory: pytest.TempPathFactory
) -> None:
    from tickdb import verify

    clickhouse, parquet = backends
    root = parquet.root  # type: ignore[attr-defined]
    raw = root.parent / "raw"
    for venue, symbol in VENUES.items():
        checks = verify.verify_day(
            [raw], root, venue, DAY, (symbol,),
            clickhouse=clickhouse._clickhouse(), client_ch=clickhouse, client_pq=parquet,  # type: ignore[attr-defined]
        )  # fmt: skip
        result = verify.report(checks)
        assert result["ok"], result
        assert result["total"] == 6  # 2 vs Parquet, 2 vs ClickHouse, BBO samples, as-of
