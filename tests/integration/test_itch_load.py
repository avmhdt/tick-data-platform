"""ITCH day into ClickHouse: book_l3 and trades load with matching fingerprints, and
several events of one order in the same nanosecond survive FINAL. Throwaway server only."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.unit.test_itch_import import DAY, _producer
from tickdb.config import Settings
from tickdb.load import clickhouse as ch
from tickdb.load import itch

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TICKDB_TEST_CLICKHOUSE") != "1",
        reason="writes into ClickHouse: throwaway server only (CI sets TICKDB_TEST_CLICKHOUSE=1)",
    ),
]


def test_itch_day_loads(tmp_path: Path) -> None:
    from datetime import date

    src, data = tmp_path / "src", tmp_path / "data"
    _producer(src, "ZZZZ", report_volume=170)
    rep = itch.import_day(src, data, DAY)
    client = ch.connect_settings(Settings(_env_file=None))  # type: ignore[call-arg]
    ch.apply_schema(client)
    for d in rep.dates:
        r = ch.load_venue_day(
            client, data, "XNAS", date.fromisoformat(d), ("ZZZZ",), tables=("trades", "book_l3")
        )
        assert r.ok, r.tables
    client.command("OPTIMIZE TABLE tickdb.book_l3 FINAL")
    n = client.query(
        "SELECT count() FROM tickdb.book_l3 FINAL WHERE venue = 'XNAS' AND symbol = 'ZZZZ'"
    ).result_rows[0][0]
    assert n == 4
