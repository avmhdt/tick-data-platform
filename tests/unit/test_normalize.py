"""Normalizers and compaction on synthetic raw days in the shared capture's format."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.fixtures.rawlog import RawDay, kraken_book_frame
from tickdb.compact.compactor import compact_day, load_book_end, partition_dir, replay_verify
from tickdb.compact.hashing import StreamHasher, content_hash
from tickdb.ingest.book import BID, L2Book

DAY = date(2026, 11, 9)
NEXT = date(2026, 11, 10)


def table(root: Path, dataset: str, venue: str, symbol: str, day: date = DAY) -> pa.Table:
    return pq.read_table(partition_dir(root, dataset, venue, symbol, day))


def gaps(root: Path, venue: str, day: date = DAY) -> list[dict[str, object]]:
    path = root / "meta" / "gaps" / f"venue={venue}" / f"date={day.isoformat()}"
    return pq.read_table(path).to_pylist()


# ------------------------------------------------------------------ binance-usdm


def _depth(sym: str, first: int, final: int, prev: int, ms: int, bid: str = "100.0") -> dict:
    data = {"e": "depthUpdate", "E": ms, "T": ms, "s": sym, "U": first, "u": final, "pu": prev,
            "b": [[bid, "1.0"]], "a": [["101.0", "2.0"]]}  # fmt: skip
    return {"stream": f"{sym.lower()}@depth@100ms", "data": data}


def _agg(sym: str, trade_id: int, ms: int, maker: bool) -> dict:
    data = {"e": "aggTrade", "E": ms, "a": trade_id, "s": sym, "p": "100.5", "q": "0.1",
            "T": ms, "m": maker}  # fmt: skip
    return {"stream": f"{sym.lower()}@aggTrade", "data": data}


def binance_day(root: Path) -> Path:
    raw = RawDay(root, "binance-usdm", DAY)
    state = {
        "books": {"BTCUSDT": {"state": "LIVE", "last_seq": 100}},
        "active": {"public": "public.g1", "market": "market.g1"},
    }
    raw.control(0.0, {"k": "state", "state": state})
    t0 = raw.t0 // 1_000_000
    raw.frame(1.0, "public.g1", _depth("BTCUSDT", 101, 105, 100, t0 + 1000))
    raw.frame(2.0, "public.g1", _depth("BTCUSDT", 106, 110, 105, t0 + 2000))
    # day's first checkpoint (lastUpdateId 107) bridges the book: diff 101..105 is older,
    # diff 106..110 straddles it
    snapshot = {"lastUpdateId": 107, "T": t0 + 2500, "bids": [["99.0", "3.0"]], "asks": [["101.0", "4.0"]]}  # fmt: skip
    raw.control(2.5, {"k": "checkpoint", "symbol": "BTCUSDT", "checkpoint": "rest_l2"}, snapshot)
    raw.frame(3.0, "public.g1", _depth("BTCUSDT", 111, 112, 110, t0 + 3000))
    raw.frame(4.0, "public.g1", _depth("BTCUSDT", 120, 121, 119, t0 + 4000))  # pu break
    raw.frame(5.0, "public.g1", _depth("BTCUSDT", 122, 123, 121, t0 + 5000))
    snap2 = {"lastUpdateId": 122, "T": t0 + 5500, "bids": [["98.0", "1.0"]], "asks": [["102.0", "1.0"]]}  # fmt: skip
    raw.control(5.5, {"k": "snapshot", "symbol": "BTCUSDT"}, snap2)
    raw.frame(6.0, "public.g1", _depth("BTCUSDT", 124, 125, 123, t0 + 6000))
    # sequence-aligned checkpoints once the book is known: untouched level 98.0 must match
    good = {
        "lastUpdateId": 125,
        "T": t0 + 6500,
        "bids": [["100.0", "1.0"], ["98.0", "1.0"]],
        "asks": [["101.0", "2.0"]],
    }
    bad = {**good, "bids": [["98.0", "7.0"]]}
    raw.control(6.5, {"k": "checkpoint", "symbol": "BTCUSDT", "checkpoint": "rest_l2"}, good)
    raw.control(6.6, {"k": "checkpoint", "symbol": "BTCUSDT", "checkpoint": "rest_l2"}, bad)
    # trades on two market connections during a make-before-break: duplicates removed
    raw.frame(1.0, "market.g1", _agg("BTCUSDT", 500, t0 + 1000, maker=True))
    raw.control(1.5, {"k": "conn_open", "conn": "market.g2", "logical": "market", "standby": True})
    raw.frame(2.0, "market.g1", _agg("BTCUSDT", 501, t0 + 2000, maker=False))
    raw.frame(2.1, "market.g2", _agg("BTCUSDT", 501, t0 + 2000, maker=False))
    raw.control(2.2, {"k": "promote", "logical": "market", "conn": "market.g2"})
    raw.control(2.3, {"k": "conn_close", "conn": "market.g1", "reason": "rotate"})
    raw.frame(3.0, "market.g2", _agg("BTCUSDT", 502, t0 + 3000, maker=True))
    raw.frame(3.0, "ticker.g1", {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker"}})
    return raw.write()


def test_binance_bridge_gap_and_dedupe(tmp_path: Path) -> None:
    raw = binance_day(tmp_path / "raw")
    data = tmp_path / "data"
    result = compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    book = table(data, "book_l2", "binance-usdm", "BTCUSDT").to_pylist()
    batches = {}
    for row in book:
        batches.setdefault(row["batch_id"], row)
    order = [(b["seq"], b["is_snapshot"], b["conn_id"]) for b in batches.values()]
    assert order == [
        (105, False, "public.g1"),  # older than the checkpoint: own receive time
        (107, True, "checkpoint"),
        (110, False, "public.g1"),  # straddles it: stamped when the book became known
        (112, False, "public.g1"),
        (121, False, "public.g1"),  # after the break, emitted as it waited
        (122, True, "snapshot"),
        (123, False, "public.g1"),
        (125, False, "public.g1"),
    ]
    ts = [r["ts_recv"] for r in book]
    assert ts == sorted(ts)  # archive order is receive-time order
    snap_ts = next(b["ts_recv"] for b in batches.values() if b["seq"] == 107)
    assert next(b["ts_recv"] for b in batches.values() if b["seq"] == 110) == snap_ts
    seq_gaps = [g for g in gaps(data, "binance-usdm") if g["kind"] == "seq_gap"]
    assert len(seq_gaps) == 1
    assert seq_gaps[0]["status"] == "partial"  # book restored, L2 history lost
    assert seq_gaps[0]["recovery"] == "snapshot"
    trades = table(data, "trades", "binance-usdm", "BTCUSDT").to_pylist()
    assert [t["trade_id"] for t in trades] == ["500", "501", "502"]
    assert [t["side"] for t in trades] == [-1, 1, -1]
    assert result.counters["dup_trades_removed"] == 1
    assert result.counters["ticker_frames_skipped"] == 1
    assert result.counters["ckpt_match"] == 1
    assert result.counters["ckpt_mismatch"] == 1
    end = load_book_end(data, "binance-usdm", DAY)["BTCUSDT"]
    assert end["known"] is True
    assert end["last_u"] == 125


def test_binance_carry_across_midnight(tmp_path: Path) -> None:
    raw = binance_day(tmp_path / "raw")
    nxt = RawDay(raw, "binance-usdm", NEXT)
    state = {
        "books": {"BTCUSDT": {"state": "LIVE", "last_seq": 125}},
        "active": {"public": "public.g1"},
    }
    nxt.control(0.0, {"k": "state", "state": state})
    nxt.frame(1.0, "public.g1", _depth("BTCUSDT", 126, 127, 125, nxt.t0 // 1_000_000 + 1000))
    nxt.write()
    data = tmp_path / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    compact_day(raw, data, "binance-usdm", NEXT, ("BTCUSDT",))
    rows = table(data, "book_l2", "binance-usdm", "BTCUSDT", NEXT).to_pylist()
    assert rows[0]["conn_id"] == "carry"
    assert rows[0]["is_snapshot"]
    assert rows[-1]["seq"] == 127  # the first diff of the day is archived immediately


# ------------------------------------------------------------------ coinbase


def coinbase_day(root: Path) -> Path:
    raw = RawDay(root, "coinbase", DAY)
    state = {"books": {"BTC-USD": {"state": "LIVE"}}, "active": {"ws": "ws.g1"}}
    raw.control(0.0, {"k": "state", "state": state})
    upd = lambda t, side, p, q: {"type": "l2update", "product_id": "BTC-USD",  # noqa: E731
                                 "changes": [[side, p, q]], "time": "2026-11-09T00:00:01.5Z"}  # fmt: skip
    raw.frame(1.0, "ws.g1", upd(1, "buy", "100.00", "1.0"))  # book levels unknown, still archived
    raw.frame(1.5, "ws.g1", {"type": "match", "trade_id": 7, "product_id": "BTC-USD",
                             "side": "sell", "size": "0.5", "price": "100.10",
                             "time": "2026-11-09T00:00:01.4Z"})  # fmt: skip
    raw.control(2.0, {"k": "conn_close", "conn": "ws.g1", "reason": "stale"})
    raw.control(3.0, {"k": "conn_open", "conn": "ws.g2", "logical": "ws"})
    raw.frame(3.1, "ws.g2", {"type": "snapshot", "product_id": "BTC-USD",
                             "bids": [["99.00", "2.0"]], "asks": [["101.00", "3.0"]]})  # fmt: skip
    raw.frame(3.2, "ws.g2", upd(2, "sell", "101.00", "0"))
    body = {"bids": [["99.00", "2.0", 1]], "asks": [], "sequence": 1}
    raw.control(5.0, {"k": "checkpoint", "symbol": "BTC-USD", "checkpoint": "rest_l2"}, body)
    raw.frame(6.0, "ws.g2", {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 9})
    return raw.write()


def test_coinbase_unknown_book_is_archived_and_gap_recovered(tmp_path: Path) -> None:
    raw = coinbase_day(tmp_path / "raw")
    data = tmp_path / "data"
    result = compact_day(raw, data, "coinbase", DAY, ("BTC-USD",))
    book = table(data, "book_l2", "coinbase", "BTC-USD").to_pylist()
    assert [(r["is_snapshot"], r["conn_id"], r["qty"]) for r in book] == [
        (False, "ws.g1", 1.0),
        (True, "ws.g2", 2.0),
        (True, "ws.g2", 3.0),
        (False, "ws.g2", 0.0),
    ]
    assert result.counters["updates_book_unknown"] == 1
    kinds = {(g["channel"], g["kind"], g["status"]) for g in gaps(data, "coinbase")}
    assert ("book_l2", "disconnect", "partial") in kinds
    assert ("trades", "disconnect", "open") in kinds  # closes at end of day; backfill decides
    trades = table(data, "trades", "coinbase", "BTC-USD").to_pylist()
    assert trades[0]["side"] == 1  # maker sold: the buyer aggressed
    assert trades[0]["ts_exchange"] == 1_794_182_401_400_000_000
    end = load_book_end(data, "coinbase", DAY)["BTC-USD"]
    assert end == {"known": True, "conn": "ws.g2", "bids": [["99.00", "2.0"]], "asks": []}


def test_coinbase_carry_needs_the_same_connection(tmp_path: Path) -> None:
    raw = coinbase_day(tmp_path / "raw")
    for conn, origin in (("ws.g2", "carry"), ("ws.g9", None)):
        nxt = RawDay(raw, "coinbase", NEXT)
        state = {"books": {"BTC-USD": {"state": "LIVE"}}, "active": {"ws": conn}}
        nxt.control(0.0, {"k": "state", "state": state})
        nxt.write()
        data = tmp_path / f"data-{conn}"
        compact_day(raw, data, "coinbase", DAY, ("BTC-USD",))
        compact_day(raw, data, "coinbase", NEXT, ("BTC-USD",))
        path = partition_dir(data, "book_l2", "coinbase", "BTC-USD", NEXT)
        rows = pq.read_table(path).to_pylist() if path.exists() else []
        assert (rows[0]["conn_id"] if rows else None) == origin


# ------------------------------------------------------------------ kraken


def kraken_day(root: Path) -> Path:
    raw = RawDay(root, "kraken", DAY)
    book = L2Book()
    bids = [(f"{100 - i}.0", "1.00000000") for i in range(12)]
    asks = [(f"{101 + i}.0", "2.00000000") for i in range(12)]
    book.load(bids, asks)
    state = {"books": {"BTC/USD": {"state": "LIVE", "bids": bids, "asks": asks}},
             "active": {"ws": "ws.g1"}}  # fmt: skip
    raw.control(0.0, {"k": "state", "state": state})
    book.apply(BID, "100.0", "0.50000000")
    raw.frame(
        1.0,
        "ws.g1",
        kraken_book_frame(
            "update", "BTC/USD", [("100.0", "0.50000000")], [], book, "2026-11-09T00:00:00.9Z"
        ),
    )
    good = kraken_book_frame(
        "update", "BTC/USD", [], [("101.0", "3.00000000")], book, "2026-11-09T00:00:01.9Z"
    )
    bad = good.replace('"checksum":', '"checksum":1')  # corrupt the checksum text
    raw.frame(2.0, "ws.g1", bad)
    raw.frame(
        3.0,
        "ws.g1",
        kraken_book_frame(
            "snapshot",
            "BTC/USD",
            bids[:3],
            asks[:3],
            _book(bids[:3], asks[:3]),
            "2026-11-09T00:00:02.9Z",
        ),
    )
    raw.frame(4.0, "ws.g1", {"channel": "trade", "type": "update", "data": [
        {"symbol": "BTC/USD", "side": "sell", "price": 100.5, "qty": 0.25, "ord_type": "market",
         "trade_id": 42, "timestamp": "2026-11-09T00:00:03.9Z"}]})  # fmt: skip
    return raw.write()


def _book(bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> L2Book:
    book = L2Book()
    book.load(bids, asks)
    return book


def test_kraken_state_start_checksum_and_resubscribe(tmp_path: Path) -> None:
    raw = kraken_day(tmp_path / "raw")
    data = tmp_path / "data"
    result = compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    assert result.counters["day_start_state"] == 1
    assert result.counters["checksum_ok"] == 2  # the update and the resubscribe snapshot
    assert result.counters["checksum_fail"] == 1
    book = table(data, "book_l2", "kraken", "BTC/USD").to_pylist()
    assert book[0]["conn_id"] == "state"
    checksum_gaps = [g for g in gaps(data, "kraken") if g["kind"] == "checksum"]
    assert [g["status"] for g in checksum_gaps] == ["partial"]
    trades = table(data, "trades", "kraken", "BTC/USD").to_pylist()
    assert trades[0]["side"] == -1
    assert trades[0]["price"] == 100.5
    # partition directories percent-encode the '/'
    assert (data / "book_l2" / "venue=kraken" / "symbol=BTC%2FUSD").is_dir()
    bbo = table(data, "bbo", "kraken", "BTC/USD").to_pylist()
    assert bbo[0]["in_resync"] is False  # the day starts from the state book
    assert (bbo[0]["bid_px"], bbo[0]["ask_px"]) == (100.0, 101.0)
    flags = [r["in_resync"] for r in bbo]
    assert True in flags  # the checksum gap flags rows until the resubscribe snapshot
    assert flags[-1] is False
    assert result.counters["bbo_crossed/BTC/USD"] == 0
    bars = pq.read_table(
        partition_dir(data, "bars", "kraken", "BTC/USD", DAY) / "interval=1m"
    ).to_pylist()
    assert [(b["n_trades"], b["sell_volume"]) for b in bars] == [(1, 0.25)]


# ------------------------------------------------------------------ idempotency, hashes


@pytest.mark.parametrize(
    ("venue", "make", "symbol"),
    [("binance-usdm", binance_day, "BTCUSDT"), ("coinbase", coinbase_day, "BTC-USD"),
     ("kraken", kraken_day, "BTC/USD")],
)  # fmt: skip
def test_replay_and_double_compaction_are_identical(
    tmp_path: Path, venue: str, make: object, symbol: str
) -> None:
    raw = make(tmp_path / "raw")  # type: ignore[operator]
    data = tmp_path / "data"
    first = compact_day(raw, data, venue, DAY, (symbol,))
    second = compact_day(raw, data, venue, DAY, (symbol,))
    assert {k: v["content_hash"] for k, v in first.partitions.items()} == {
        k: v["content_hash"] for k, v in second.partitions.items()
    }
    rows = table(data, "book_l2", venue, symbol).num_rows
    assert rows == first.partitions[f"book_l2/{symbol}"]["rows"]  # reload adds no rows
    report = replay_verify(raw, data, venue, DAY, (symbol,))
    assert report["ok"], report
    manifest = orjson.loads(
        (partition_dir(data, "trades", venue, symbol, DAY) / "_manifest.json").read_bytes()
    )
    assert manifest["rows"] == table(data, "trades", venue, symbol).num_rows
    assert manifest["spool_files"]
    assert not list((data / "interim").rglob("*.parquet"))  # staging and scratch cleaned


def test_content_hash_ignores_batching_but_not_values() -> None:
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    t = pa.table(
        {"a": list(range(200_000)), "b": [str(i % 7) for i in range(200_000)]}, schema=schema
    )
    whole = content_hash(t)
    h = StreamHasher(schema)
    for batch in t.to_batches(max_chunksize=12_345):
        h.update(batch)
    assert h.hexdigest() == whole
    assert h.rows == 200_000
    changed = t.set_column(0, "a", pa.array([1, *range(1, 200_000)], pa.int64()))
    assert content_hash(changed) != whole
    shuffled = t.take(pa.array(list(reversed(range(200_000)))))
    assert content_hash(shuffled, ["a"]) == whole  # sorting first restores the order


def test_gap_report_files(tmp_path: Path) -> None:
    from tickdb.dq import report

    raw = coinbase_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "coinbase", DAY, ("BTC-USD",))
    out = tmp_path / "reports"
    summary = report.write_day(data, DAY, out)
    assert summary["gaps"] == 2
    md = (out / f"{DAY}.md").read_text()
    assert "| coinbase | BTC-USD | book_l2 | disconnect |" in md
    assert "price" not in (out / f"{DAY}.csv").read_text().splitlines()[0]  # aggregates only
    path = report.write_summary(out, DAY, NEXT)
    text = path.read_text()
    assert f"| {DAY} | 2 |" in text
    assert f"| {NEXT} | no report |" in text


def test_book_checks_summary(tmp_path: Path) -> None:
    from tickdb.dq import report

    raw = kraken_day(tmp_path / "raw")
    binance_day(raw)
    data = tmp_path / "data"
    compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    checks = report.book_checks(data, DAY, DAY)
    assert checks["kraken"]["checksum_ok"] == 2
    assert checks["kraken"]["checksum_fail"] == 1
    assert checks["kraken"]["crossed_bbo"] == 0
    assert checks["binance-usdm"]["ckpt_match"] == 1
    assert checks["binance-usdm"]["ckpt_match_pct"] == 50.0
    assert checks["binance-usdm"]["seq_gaps"] == 1
    assert checks["binance-usdm"]["seq_gaps_resynced"] == 1
    assert checks["binance-usdm"]["resync_median_s"] is not None


def test_coinbase_checkpoints_aligned_by_sequence(tmp_path: Path) -> None:
    from tickdb.dq import checkpoints as ck

    raw = RawDay(tmp_path / "raw", "coinbase", DAY)
    raw.control(
        0.0,
        {
            "k": "state",
            "state": {"books": {"BTC-USD": {"state": "LIVE"}}, "active": {"ws": "ws.g1"}},
        },
    )
    raw.frame(
        0.5,
        "ws.g1",
        {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["99.00", "2.0"]],
            "asks": [["101.00", "3.0"]],
        },
    )
    raw.frame(1.0, "ws.g1", {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 10})
    raw.frame(
        1.5,
        "ws.g1",
        {
            "type": "l2update",
            "product_id": "BTC-USD",
            "changes": [["buy", "99.00", "5.0"]],
            "time": "2026-11-09T00:00:01.5Z",
        },
    )
    raw.frame(2.0, "ws.g1", {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 20})
    raw.frame(5.0, "ws.g1", {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 30})
    stale = {
        "sequence": 9,
        "bids": [["99.00", "2.0", 1]],
        "asks": [["101.00", "3.0", 1]],
    }  # the book of t=1.0
    wrong = {"sequence": 25, "bids": [["99.00", "7.0", 1]], "asks": [["101.00", "3.0", 1]]}
    raw.control(2.4, {"k": "checkpoint", "symbol": "BTC-USD"}, stale)  # received 1.4 s later
    raw.control(5.1, {"k": "checkpoint", "symbol": "BTC-USD"}, wrong)
    raw.write()
    result = ck.check_day([tmp_path / "raw"], {}, DAY, ("BTC-USD",))["BTC-USD"]
    assert result["compared"] == 2
    assert result["match_1.0s"] == 1  # the stale one matches at its aligned time
    assert result["match_pct_1.0s"] == 50.0
