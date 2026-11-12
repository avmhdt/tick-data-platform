"""REST backfill against mocked venue APIs (FR-05, DQ-06, AC-W-02)."""

from __future__ import annotations

from pathlib import Path

import httpx

from tests.fixtures.rawlog import RawDay
from tests.unit.test_normalize import DAY, _agg, gaps, table
from tickdb.compact.compactor import compact_day
from tickdb.ingest import backfill as bf
from tickdb.ingest.ratelimit import TokenBucket


def binance_gap_day(root: Path) -> Path:
    raw = RawDay(root, "binance-usdm", DAY)
    raw.control(0.0, {"k": "state", "state": {"active": {"market": "market.g1"}}})
    t0 = raw.t0 // 1_000_000
    raw.frame(1.0, "market.g1", _agg("BTCUSDT", 500, t0 + 1000, maker=True))
    raw.frame(2.0, "market.g1", _agg("BTCUSDT", 501, t0 + 2000, maker=False))
    raw.control(3.0, {"k": "conn_close", "conn": "market.g1", "reason": "disconnect"})
    raw.control(9.0, {"k": "conn_open", "conn": "market.g2", "logical": "market"})
    raw.frame(10.0, "market.g2", _agg("BTCUSDT", 505, t0 + 10_000, maker=True))
    raw.frame(11.0, "market.g2", _agg("BTCUSDT", 506, t0 + 11_000, maker=True))
    raw.frame(12.0, "market.g2", _agg("BTCUSDT", 508, t0 + 12_000, maker=True))  # ws id hole
    return raw.write()


def _client(handler: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def binance_api(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params["fromId"])
    t0 = 1_794_182_400_000
    rows = [
        {"a": i, "p": "100.0", "q": "0.5", "f": i, "l": i, "T": t0 + 3000 + i, "m": False}
        for i in range(start, min(start + 2, 508))  # two per page: exercises paging
    ]
    return httpx.Response(200, json=rows)


def _filler(venue: str, handler: object) -> bf.Backfiller:
    return bf.Backfiller(venue, "https://rest.example", _client(handler), TokenBucket(1000, 1000))


async def test_backfill_recovers_gap_and_merges_in_order(tmp_path: Path) -> None:
    raw = binance_gap_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    trade_gaps = [g for g in gaps(data, "binance-usdm") if g["channel"] == "trades"]
    before = {g["kind"]: g for g in trade_gaps}
    assert (before["disconnect"]["seq_from"], before["disconnect"]["seq_to"]) == (502, 504)
    assert before["disconnect"]["status"] == "open"
    assert before["trade_id_gap"]["seq_from"] == 507
    needs = bf.plan(data, "binance-usdm", DAY)
    assert sorted((n.lo, n.hi) for n in needs) == [(502, 504), (507, 507)]
    filler = _filler("binance-usdm", binance_api)
    got = sorted([await filler.fetch(n) for n in needs])
    assert got == [1, 3]
    assert filler.write(data, DAY) is not None
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    after = {g["kind"]: g for g in gaps(data, "binance-usdm") if g["channel"] == "trades"}
    assert after["disconnect"]["status"] == "recovered"
    assert after["disconnect"]["recovery"] == "rest_backfill"
    assert after["disconnect"]["recovered_rows"] == 3
    assert after["trade_id_gap"]["status"] == "recovered"
    trades = table(data, "trades", "binance-usdm", "BTCUSDT").to_pylist()
    assert sorted(int(t["trade_id"]) for t in trades) == list(range(500, 509))
    rest = [t for t in trades if t["source"] == "rest"]
    assert len(rest) == 4
    assert all(t["ts_recv"] == t["ts_exchange"] and t["ts_fetch"] for t in rest)
    ts = [t["ts_recv"] for t in trades]
    assert ts == sorted(ts)  # REST rows merged into archive order
    assert bf.plan(data, "binance-usdm", DAY) == []  # nothing left to fetch


async def test_nothing_returned_marks_unrecoverable(tmp_path: Path) -> None:
    raw = binance_gap_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    filler = _filler("binance-usdm", lambda r: httpx.Response(200, json=[]))
    for need in bf.plan(data, "binance-usdm", DAY):
        assert await filler.fetch(need) == 0
    filler.write(data, DAY)
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    statuses = {g["status"] for g in gaps(data, "binance-usdm") if g["channel"] == "trades"}
    assert statuses == {"unrecoverable"}


async def test_coinbase_pages_backwards() -> None:
    def api(request: httpx.Request) -> httpx.Response:
        after = int(request.url.params["after"])
        rows = [
            {
                "trade_id": i,
                "price": "1",
                "size": "1",
                "time": "2026-11-09T00:00:01Z",
                "side": "buy",
            }
            for i in range(after - 1, max(after - 3, 99), -1)
        ]
        return httpx.Response(200, json=rows)

    filler = _filler("coinbase", api)
    assert await filler.fetch(bf.Need("BTC-USD", 101, 106, 0)) == 6
    assert filler.requests == 3


async def test_kraken_pages_by_since(tmp_path: Path) -> None:
    def api(request: httpx.Request) -> httpx.Response:
        since = int(request.url.params["since"])
        first = 40 + since
        rows = [
            ["100.5", "0.1", f"1794182401.{i:04d}", "s", "m", "", i] for i in (first, first + 1)
        ]
        body = {"error": [], "result": {"XXBTZUSD": rows, "last": str(since + 2)}}
        return httpx.Response(200, json=body)

    filler = _filler("kraken", api)
    assert await filler.fetch(bf.Need("BTC/USD", 40, 43, 0)) == 4
    assert filler.write(tmp_path, DAY) is not None
    assert next((tmp_path / "raw" / "backfill").rglob("*.zst")).stat().st_size > 0
    assert bf.Backfiller("kraken", "x").write(tmp_path, DAY) is None


def test_plan_without_gaps(tmp_path: Path) -> None:
    assert bf.plan(tmp_path, "kraken", DAY) == []


async def test_binance_vision_fallback_after_rest_refuses(tmp_path: Path) -> None:
    import zipfile

    from tickdb.ingest import binance_vision as bv

    raw = binance_gap_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    target = bv.local_path(data, "BTCUSDT", DAY)
    target.parent.mkdir(parents=True)
    t0 = 1_794_182_400_000
    lines = [
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
    ]
    lines += [f"{i},100.0,0.5,{i},{i},{t0 + 3000 + i},true" for i in range(500, 509)]
    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("BTCUSDT-aggTrades.csv", "\n".join(lines) + "\n")
    target.with_suffix(".zip.ok").write_text("verified")
    assert [r["a"] for r in bv.rows(target, 502, 504)] == [502, 503, 504]

    refuse = httpx.Response(400, json={"code": -4166, "msg": "Search window is restricted"})
    filler = _filler("binance-usdm", lambda r: refuse)
    for need in bf.plan(data, "binance-usdm", DAY):
        assert await filler.fetch(need) == 0
        assert filler.vision(need, data, [DAY]) == need.hi - need.lo + 1
    assert any("restricted" in e for e in filler.errors)
    filler.write(data, DAY)
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    trade_gaps = [g for g in gaps(data, "binance-usdm") if g["channel"] == "trades"]
    assert {(g["status"], g["recovery"]) for g in trade_gaps} == {("recovered", "binance_vision")}
    vendor = [
        t
        for t in table(data, "trades", "binance-usdm", "BTCUSDT").to_pylist()
        if t["source"] == "vendor"
    ]
    assert len(vendor) == 4
    assert all(t["side"] == -1 for t in vendor)  # buyer is maker: seller aggressed
