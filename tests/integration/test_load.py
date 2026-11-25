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
    pytest.mark.skipif(
        os.environ.get("TICKDB_TEST_CLICKHOUSE") != "1",
        reason="writes fixture days into ClickHouse: throwaway server only (CI sets TICKDB_TEST_CLICKHOUSE=1)",
    ),
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
    if "TTL" in str(c.command("SHOW CREATE TABLE tickdb.book_l2")):
        c.command("ALTER TABLE tickdb.book_l2 REMOVE TTL")  # fixed fixture day, throwaway server
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
    # the live table's TTL is removed for the fixed fixture day; the schema itself keeps it
    book = next(st for st in ch.schema_statements() if "TABLE IF NOT EXISTS tickdb.book_l2" in st)
    assert "TTL date + INTERVAL 14 DAY" in book


def test_book_l2_past_ttl_is_skipped(client: object, tmp_path: Path) -> None:
    raw = kraken_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    rep = ch.load_venue_day(client, data, "kraken", DAY, ("BTC/USD",), book_l2_ttl_days=1)
    assert rep.ok, rep.tables
    assert rep.tables["book_l2"]["skipped"] == "past the ClickHouse TTL"
    assert rep.tables["trades"]["rows"] > 0
