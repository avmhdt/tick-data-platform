"""Parquet -> ClickHouse loads (FR-11, AC-W-03). Needs ClickHouse (CI service or make up)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.unit.test_normalize import DAY, coinbase_day, kraken_day
from tickdb.compact.compactor import compact_day
from tickdb.load import clickhouse as ch

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.environ.get("CLICKHOUSE_PASSWORD"), reason="no ClickHouse"),
]


@pytest.fixture
def client() -> object:
    import clickhouse_connect

    c = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "tickdb"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
    ch.apply_schema(c)
    return c


def _count(client: object, table: str, venue: str) -> int:
    q = f"SELECT count() FROM tickdb.{table} WHERE venue = '{venue}' AND date = toDate('{DAY}')"
    return int(client.command(q))  # type: ignore[attr-defined]


def test_load_is_idempotent_and_matches_parquet(client: object, tmp_path: Path) -> None:
    raw = kraken_day(tmp_path / "raw")
    coinbase_day(raw)
    data = tmp_path / "data"
    compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    compact_day(raw, data, "coinbase", DAY, ("BTC-USD",))
    first = ch.load_venue_day(client, data, "kraken", DAY, ("BTC/USD",))
    assert first.ok, first.tables
    counts = {t: _count(client, t, "kraken") for t in ch.TABLES}
    assert counts["book_l2"] == first.tables["book_l2"]["rows"] > 0
    ch.load_venue_day(client, data, "coinbase", DAY, ("BTC-USD",))
    again = ch.load_venue_day(client, data, "kraken", DAY, ("BTC/USD",))
    assert again.ok
    assert {t: _count(client, t, "kraken") for t in ch.TABLES} == counts  # no extra rows
    assert _count(client, "book_l2", "coinbase") > 0  # the other venue's partition survived
    leftovers = client.query(  # type: ignore[attr-defined]
        "SELECT count() FROM system.tables WHERE database = 'tickdb' AND name LIKE '_stage_%'"
    ).result_rows[0][0]
    assert leftovers == 0


def test_schema_statements_are_idempotent(client: object) -> None:
    assert ch.apply_schema(client) == ch.apply_schema(client) >= 10
    ttl = client.command(  # type: ignore[attr-defined]
        "SELECT engine_full FROM system.tables WHERE database='tickdb' AND name='book_l2'"
    )
    assert "toIntervalDay(14)" in str(ttl)
