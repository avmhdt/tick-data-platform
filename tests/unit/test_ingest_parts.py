from __future__ import annotations

import random
import re
from datetime import date
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st
from typer.testing import CliRunner

from tests.conftest import CONFIG
from tickdb import cli, diskproj
from tickdb.ingest.backoff import Backoff
from tickdb.ingest.base import uuid7
from tickdb.ingest.book import ASK, BID, L2Book, kraken_checksum
from tickdb.ingest.ratelimit import TokenBucket

# Worked example from Kraken's "Spot WebSocket Book v2" checksum guide.
DOC_ASKS = [
    ("45285.2", "0.00100000"), ("45286.4", "1.54571953"), ("45286.6", "1.54571109"),
    ("45289.6", "1.54560911"), ("45290.2", "0.15890660"), ("45291.8", "1.54553491"),
    ("45294.7", "0.04454749"), ("45296.1", "0.35380000"), ("45297.5", "0.09945542"),
    ("45299.5", "0.18772827"),
]  # fmt: skip
DOC_BIDS = [
    ("45283.5", "0.10000000"), ("45283.4", "1.54582015"), ("45282.1", "0.10000000"),
    ("45281.0", "0.10000000"), ("45280.3", "1.54592586"), ("45279.0", "0.07990000"),
    ("45277.6", "0.03310103"), ("45277.5", "0.30000000"), ("45277.3", "1.54602737"),
    ("45276.6", "0.15445238"),
]  # fmt: skip


def test_kraken_checksum_doc_example() -> None:
    assert kraken_checksum(DOC_BIDS, DOC_ASKS) == 3310070434
    book = L2Book()
    book.load(reversed(DOC_BIDS), reversed(DOC_ASKS))
    assert kraken_checksum(book.top(BID, 10), book.top(ASK, 10)) == 3310070434
    book.apply(BID, "45283.5", "0.20000000")
    assert kraken_checksum(book.top(BID, 10), book.top(ASK, 10)) != 3310070434


def test_book_apply_remove_truncate_best() -> None:
    book = L2Book()
    for p in ("1", "2", "3", "4"):
        book.apply(BID, p, "1")
        book.apply(ASK, str(int(p) + 4), "1")
    assert book.best() == (4.0, 5.0)
    book.apply(BID, "4", "0")
    book.apply(BID, "9.5", "0")  # unknown level removal is a no-op
    assert book.best() == (3.0, 5.0)
    assert [p for p, _ in book.top(BID, 2)] == ["3", "2"]
    assert [p for p, _ in book.top(ASK, 2)] == ["5", "6"]
    book.truncate(2)
    assert sorted(book.bids) == [2.0, 3.0]
    assert sorted(book.asks) == [5.0, 6.0]
    assert not book.crossed()
    book.apply(BID, "5.5", "1")
    assert book.crossed()
    book.clear()
    assert book.best() == (None, None)
    assert not book.crossed()


@given(st.lists(st.tuples(st.booleans(), st.integers(1, 60), st.integers(0, 3)), max_size=200))
def test_book_matches_a_dict_model(ops: list[tuple[bool, int, int]]) -> None:
    book = L2Book()
    model: dict[int, dict[float, str]] = {BID: {}, ASK: {}}
    for is_bid, price, qty in ops:
        side = BID if is_bid else ASK
        book.apply(side, f"{price}.0", str(qty))
        if qty:
            model[side][float(price)] = str(qty)
        else:
            model[side].pop(float(price), None)
    assert [float(p) for p, _ in book.top(BID, 5)] == sorted(model[BID], reverse=True)[:5]
    assert [float(p) for p, _ in book.top(ASK, 5)] == sorted(model[ASK])[:5]


@given(st.integers(0, 40), st.integers(0, 2**32 - 1))
def test_backoff_stays_in_full_jitter_envelope(attempts: int, seed: int) -> None:
    backoff = Backoff(1.0, 2.0, 60.0, rng=random.Random(seed))
    for n in range(attempts):
        delay = backoff.next_delay()
        assert 0.0 <= delay <= min(60.0, 2.0**n)
    backoff.reset()
    assert backoff.attempt == 0
    assert backoff.ceiling(10) == 60.0


def test_backoff_without_jitter() -> None:
    backoff = Backoff(1.0, 2.0, 5.0, jitter="none")
    assert [backoff.next_delay() for _ in range(5)] == [1.0, 2.0, 4.0, 5.0, 5.0]


async def test_token_bucket_waits_for_tokens() -> None:
    now = [0.0]
    bucket = TokenBucket(rate=10.0, capacity=20.0, clock=lambda: now[0])
    await bucket.acquire(20)  # full burst at once
    assert bucket.wait_time(5) == pytest.approx(0.5)
    now[0] = 1.0
    assert bucket.wait_time(5) == 0.0
    with pytest.raises(ValueError, match="capacity"):
        await bucket.acquire(25)
    with pytest.raises(ValueError, match="positive"):
        TokenBucket(0, 1)


async def test_token_bucket_sleeps_when_empty() -> None:
    bucket = TokenBucket(rate=200.0, capacity=1.0)
    await bucket.acquire(1)
    await bucket.acquire(1)  # waits about 5 ms for a refill
    assert bucket.tokens < 1.0


def test_uuid7_is_version_7_and_time_ordered() -> None:
    a, b = uuid7(), uuid7()
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    assert re.match(pattern, a)
    assert a[:13] <= b[:13]


def test_disk_projection(tmp_path: Path) -> None:
    for day, size in (("2026-11-08", 1000), ("2026-11-09", 3000), ("2026-11-10", 50)):
        d = tmp_path / "raw/external/lob-alpha/venue=x" / f"date={day}" / "deep"
        d.mkdir(parents=True)
        (d / "f").write_bytes(b"x" * size)
    raw = diskproj.Store("raw", Path("raw/external/lob-alpha"), retention_days=7)
    keep = diskproj.Store("keep", Path("raw/external/lob-alpha"), retention_days=None)
    rate = diskproj.measure(tmp_path, raw, today=date(2026, 11, 10))
    assert rate.days_measured == 2  # today's partial day is excluded
    assert rate.bytes_per_day == 2000
    assert rate.bytes_now == 4050
    assert rate.projected(30) == 14_000  # levels off at the 7-day retention
    assert diskproj.measure(tmp_path, keep, date(2026, 11, 10)).projected(30) == 60_000
    text = diskproj.render([rate], [30, 90], free_bytes=10**12, floor=150 * 10**9)
    assert "after 30 d" in text
    assert "ok" in text
    low = diskproj.render([rate], [30], free_bytes=10, floor=150 * 10**9)
    assert "BELOW FLOOR" in low
    empty = diskproj.measure(tmp_path, diskproj.Store("none", Path("nope"), None), date.today())
    assert empty.bytes_per_day == 0


def test_ingest_run_refused_while_capture_is_shared() -> None:
    result = CliRunner().invoke(
        cli.app, ["ingest", "run", "--venue", "kraken", "--config", str(CONFIG)]
    )
    assert result.exit_code == cli.EXIT_CONFIG
    assert "shared" in result.output


def test_ingest_run_unknown_venue() -> None:
    result = CliRunner().invoke(
        cli.app, ["ingest", "run", "--venue", "okx", "--config", str(CONFIG)]
    )
    assert result.exit_code == cli.EXIT_CONFIG


def test_disk_project_command(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli.app,
        [
            "disk",
            "project",
            "--dest",
            str(tmp_path),
            "--guard-path",
            str(tmp_path),
            "--config",
            str(CONFIG),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "raw mirror (shared)" in result.output
    assert "free now" in result.output


def test_schema_sql_parses_into_statements() -> None:
    from tickdb.load import clickhouse as ch

    statements = ch.schema_statements()
    assert statements[0].startswith("CREATE DATABASE")
    tables = [s.split()[5] for s in statements if s.startswith("CREATE TABLE")]
    assert {
        "tickdb.trades",
        "tickdb.book_l2",
        "tickdb.bbo",
        "tickdb.bars_1s",
        "tickdb.gaps",
    } <= set(tables)
    assert ch.schema_statements("-- note\nSELECT 1; ;\n") == ["SELECT 1"]
