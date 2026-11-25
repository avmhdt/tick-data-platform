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
    # The fixture day is fixed (2026-11-09); on a throwaway server the 14-day TTL would drop
    # its book rows on insert once the day is old enough.
    writer.command("ALTER TABLE tickdb.book_l2 REMOVE TTL")
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


def test_dq_tables_load_into_clickhouse(
    backends: tuple[object, object], tmp_path_factory: pytest.TempPathFactory
) -> None:
    from tickdb.dq import daily

    clickhouse, parquet = backends
    root = parquet.root  # type: ignore[attr-defined]
    writer = ch.connect_settings(Settings(_env_file=None))  # type: ignore[call-arg]
    shared = tmp_path_factory.mktemp("shared")
    for venue, symbol in VENUES.items():
        result = daily.check_day(root, venue, DAY, (symbol,), shared_root=shared)
        daily.write_day(root, result)
        for _ in range(2):  # idempotent, and an empty outlier day drops its partition
            loaded = daily.load_day(writer, root, venue, DAY)
        assert loaded["dq_daily"] == 3
        assert loaded["dq_latency"] == 48
    got = clickhouse.sql(  # type: ignore[attr-defined]
        "SELECT venue, count() AS n FROM tickdb.dq_daily FINAL WHERE date = {d:Date} "
        "GROUP BY venue ORDER BY venue",
        {"d": DAY},
    )
    assert got["n"].to_list() == [3, 3, 3]
    gaps = clickhouse.sql(  # type: ignore[attr-defined]
        "SELECT count() AS n FROM tickdb.gaps FINAL WHERE venue = 'coinbase'"
    )
    assert gaps["n"][0] >= 1


def test_dq_rows_of_two_days_survive_final(backends: tuple[object, object]) -> None:
    """FINAL can merge across partitions: the sort key must start with the date."""
    from datetime import timedelta

    import pyarrow as pa
    import pyarrow.parquet as pq

    from tickdb.dq import daily

    clickhouse, parquet = backends
    root = parquet.root  # type: ignore[attr-defined]
    writer = ch.connect_settings(Settings(_env_file=None))  # type: ignore[call-arg]
    nxt = DAY + timedelta(days=1)
    for name in ("dq_daily", "dq_outliers", "dq_latency"):
        src = root / "meta" / name / f"date={DAY.isoformat()}" / "venue=kraken.parquet"
        table = pq.read_table(src)
        rows = [r | {"date": nxt} for r in table.to_pylist()]
        dst = root / "meta" / name / f"date={nxt.isoformat()}" / "venue=kraken.parquet"
        dst.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), dst)
    daily.load_day(writer, root, "kraken", DAY)
    daily.load_day(writer, root, "kraken", nxt)
    for table, per_day in (("dq_daily", 3), ("dq_latency", 48)):
        got = clickhouse.sql(  # type: ignore[attr-defined]
            f"SELECT date, count() AS n FROM tickdb.{table} FINAL WHERE venue = 'kraken' "
            "GROUP BY date ORDER BY date"
        )
        assert got["n"].to_list() == [per_day, per_day], table
