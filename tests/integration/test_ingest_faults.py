"""Ingestors against mock venues with injected faults (AC-W-06)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import orjson
import pytest

from tests.integration.ingest_harness import all_synced, events, records, run_with, wait_for
from tests.integration.mock_venues import MOCKS, MockBinance, MockCoinbase, MockKraken, MockVenue
from tickdb.ingest.base import VenueIngestor
from tickdb.ingest.spool import decodable

VENUES = ["binance-usdm", "coinbase", "kraken"]


@pytest.fixture
async def mock(request: pytest.FixtureRequest) -> MockVenue:
    venue = request.param
    m = await MOCKS[venue](rate_per_s=300).start()
    yield m  # type: ignore[misc]
    await m.stop()


def _trade_ids(recs: list[dict[str, object]], venue: str) -> dict[str, list[int]]:
    ids: dict[str, list[int]] = {}
    for r in recs:
        if r["kind"] != "ws" or r["channel"] != "trades":
            continue
        msg = orjson.loads(str(r["payload"]))
        if venue == "binance-usdm":
            ids.setdefault(msg["data"]["s"], []).append(int(msg["data"]["a"]))
        elif venue == "coinbase":
            ids.setdefault(msg["product_id"], []).append(int(msg["trade_id"]))
        else:
            for row in msg["data"]:
                ids.setdefault(row["symbol"], []).append(int(row["trade_id"]))
    return ids


@pytest.mark.parametrize("mock", VENUES, indirect=True)
async def test_clean_session_syncs_and_spools_everything(mock: MockVenue, tmp_path: Path) -> None:
    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(1.0)

    ing = await run_with(mock.kind, mock, tmp_path, scenario)
    recs = records(tmp_path)
    done = events(recs, "resync_done")
    assert {e["symbol"] for e in done} == set(mock.symbols)
    assert not events(recs, "seq_gap")
    assert not events(recs, "checksum_fail")
    assert not events(recs, "trade_id_gap")
    ids = _trade_ids(recs, mock.kind)
    for symbol, got in ids.items():
        assert got == list(range(got[0], got[-1] + 1)), symbol  # contiguous, no duplicates
    files = list((tmp_path / "raw" / "spool").rglob("*"))
    assert not [f for f in files if f.name.endswith(".partial")]
    assert all(decodable(f)[0] for f in files if f.is_file())
    assert all(r["ts_recv"] > 0 and r["v"] == 1 for r in recs)
    ws = [r for r in recs if r["kind"] == "ws"]
    assert [r["rx_seq"] for r in ws if r["conn_id"] == ws[0]["conn_id"]] == sorted(
        r["rx_seq"] for r in ws if r["conn_id"] == ws[0]["conn_id"]
    )
    assert max(ing.resync_times) <= 10.0  # AC-W-05 median bound, checked per resync here


@pytest.mark.parametrize("mock", VENUES, indirect=True)
async def test_drop_reconnects_within_backoff_and_resyncs(mock: MockVenue, tmp_path: Path) -> None:
    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        await m.drop_all()
        await asyncio.sleep(0.3)
        await wait_for(
            lambda: all_synced(ing) and m.connections_opened >= 2 * len(ing.connections())
        )
        await asyncio.sleep(0.5)

    await run_with(mock.kind, mock, tmp_path, scenario)
    recs = records(tmp_path)
    gaps = events(recs, "gap_open")
    assert {(g["symbol"], g["kind"]) for g in gaps} >= {(s, "disconnect") for s in mock.symbols}
    waits = events(recs, "reconnect_wait")
    assert waits
    assert all(0.0 <= w["delay_s"] <= 0.2 for w in waits)  # attempt 0: [0, base]
    connects = events(recs, "connect")
    assert len(connects) >= 2 * len({c["conn"] for c in connects})
    done = events(recs, "resync_done")
    for symbol in mock.symbols:
        assert sum(1 for e in done if e["symbol"] == symbol) >= 2  # after connect and reconnect


@pytest.mark.parametrize("mock", ["binance-usdm"], indirect=True)
async def test_binance_sequence_gap_triggers_rest_resync(mock: MockVenue, tmp_path: Path) -> None:
    assert isinstance(mock, MockBinance)

    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        mock.skip_seq("ETHUSDT")
        await wait_for(
            lambda: bool(
                ing.metrics.seq_gaps.labels("binance-usdm", "ETHUSDT", "book_l2")._value.get()
            )
        )
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(0.3)

    await run_with("binance-usdm", mock, tmp_path, scenario)
    recs = records(tmp_path)
    gaps = events(recs, "seq_gap")
    assert [g["symbol"] for g in gaps] == ["ETHUSDT"]
    starts = [e for e in events(recs, "resync_start") if e["reason"] == "seq_gap"]
    assert [s["symbol"] for s in starts] == ["ETHUSDT"]
    snaps = [r for r in recs if r["kind"] == "rest_snapshot" and r["symbol"] == "ETHUSDT"]
    assert len(snaps) >= 2  # one at connect, one for the gap
    last_done = [e for e in events(recs, "resync_done") if e["symbol"] == "ETHUSDT"][-1]
    assert last_done["_ts"] > gaps[0]["_ts"]


@pytest.mark.parametrize("mock", ["kraken"], indirect=True)
async def test_kraken_checksum_mismatch_resubscribes(mock: MockVenue, tmp_path: Path) -> None:
    assert isinstance(mock, MockKraken)

    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(0.3)
        mock.bad_checksum_next = "SOL/USD"
        await wait_for(lambda: mock.bad_checksum_next is None)
        await asyncio.sleep(0.2)
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(0.3)

    await run_with("kraken", mock, tmp_path, scenario)
    recs = records(tmp_path)
    fails = events(recs, "checksum_fail")
    assert [f["symbol"] for f in fails] == ["SOL/USD"]  # 100 % match everywhere else
    assert fails[0]["computed"] != fails[0]["expected"]
    done = [e for e in events(recs, "resync_done") if e["symbol"] == "SOL/USD"]
    assert len(done) == 2
    assert done[-1]["_ts"] > fails[0]["_ts"]


@pytest.mark.parametrize("mock", ["coinbase"], indirect=True)
async def test_coinbase_crossed_book_and_trade_gap(mock: MockVenue, tmp_path: Path) -> None:
    assert isinstance(mock, MockCoinbase)

    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        await wait_for(lambda: "ETH-USD" in ing.active["ws"].state["last_trade"])
        mock.cross_next = "BTC-USD"
        mock.skip_trade_id("ETH-USD")
        await wait_for(lambda: mock.cross_next is None)
        await asyncio.sleep(0.3)
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(0.5)

    await run_with("coinbase", mock, tmp_path, scenario)
    recs = records(tmp_path)
    assert [e["symbol"] for e in events(recs, "crossed")] == ["BTC-USD"]
    gaps = events(recs, "trade_id_gap")
    assert [(g["symbol"], g["seq_to"] - g["seq_from"]) for g in gaps] == [("ETH-USD", 0)]
    done = [e for e in events(recs, "resync_done") if e["symbol"] == "BTC-USD"]
    assert len(done) == 2


@pytest.mark.parametrize("mock", ["kraken", "binance-usdm"], indirect=True)
async def test_silence_closes_and_reconnects(mock: MockVenue, tmp_path: Path) -> None:
    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        m.silence(2.0)
        await wait_for(lambda: m.connections_opened > len(ing.connections()), limit_s=15)
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(0.2)

    await run_with(mock.kind, mock, tmp_path, scenario, silence=(1.0, 1.0))
    recs = records(tmp_path)
    assert events(recs, "silence")
    reasons = {e["reason"] for e in events(recs, "disconnect")}
    assert "silence" in reasons
    assert any(g["kind"] == "disconnect" for g in events(recs, "gap_open"))


@pytest.mark.parametrize("mock", VENUES, indirect=True)
async def test_rotation_is_make_before_break(mock: MockVenue, tmp_path: Path) -> None:
    async def scenario(ing: VenueIngestor, m: MockVenue) -> None:
        await wait_for(lambda: all_synced(ing))
        await asyncio.sleep(4.5)

    await run_with(mock.kind, mock, tmp_path, scenario, lifetime="2s")
    recs = records(tmp_path)
    rotations = events(recs, "rotate")
    assert len(rotations) >= len(MOCKS) - 2  # at least one rotation per connection
    assert not events(recs, "gap_open")  # planned rotation never opens a gap
    assert {e["reason"] for e in events(recs, "disconnect")} <= {"rotate", "stop"}
    for symbol, got in _trade_ids(recs, mock.kind).items():
        unique = sorted(set(got))
        assert unique == list(range(unique[0], unique[-1] + 1)), symbol  # nothing lost
    # each rotation's new connection was synced before the old one closed
    for rot in rotations:
        new_done = [e for e in events(recs, "resync_done") if e["_conn"] == rot["new"]]
        if new_done:
            assert max(e["_ts"] for e in new_done) <= rot["_ts"]
