from __future__ import annotations

import asyncio
from pathlib import Path

import zstandard

from tickdb.ingest.spool import (
    SpoolWriter,
    decodable,
    hour_key,
    make_record,
    read_spool,
    recover_partials,
    spool_path,
)

T0 = 1_794_268_800_000_000_000  # 2026-11-10 00:00:00 UTC
HOUR = 3_600_000_000_000


def rec(ts: int, i: int, channel: str = "trades", conn: str = "c1") -> dict[str, object]:
    return make_record(
        venue="kraken",
        conn_id=conn,
        rx_seq=i,
        ts_recv=ts,
        kind="ws",
        channel=channel,
        symbol="BTC/USD",
        payload=f'{{"a":{i},"text":"line\\nbreak"}}',
    )


def test_layout_follows_receive_hour() -> None:
    assert hour_key(T0 + HOUR + 5) == ("2026-11-10", "01")
    path = spool_path(Path("/d"), "kraken", "book_l2", "c9", T0 + 2 * HOUR)
    assert path.as_posix() == (
        "/d/raw/spool/venue=kraken/date=2026-11-10/hour=02/book_l2-c9.jsonl.zst"
    )


def test_record_fields_in_spec_order() -> None:
    assert list(rec(T0, 1)) == [
        "v",
        "venue",
        "conn_id",
        "rx_seq",
        "ts_recv",
        "kind",
        "channel",
        "symbol",
        "payload",
    ]


async def test_round_trip_rotation_and_rename(tmp_path: Path) -> None:
    clock = [T0 + 10]
    writer = SpoolWriter(tmp_path, "kraken", flush_interval_s=0.05, clock_ns=lambda: clock[0])
    task = asyncio.create_task(writer.run())
    for i in range(50):
        await writer.put(rec(T0 + i, i))
    await asyncio.sleep(0.15)
    # hour 00 is still open: its file is a .partial and already decodes (flushed frames)
    partial = next(tmp_path.rglob("*.partial"))
    assert len(list(read_spool(partial))) == 50
    for i in range(50, 60):
        await writer.put(rec(T0 + HOUR + i, i))  # hour 01
    clock[0] = T0 + HOUR + 10 * 1_000_000_000  # hour 00 closed > 5 s ago
    await asyncio.sleep(0.15)
    await writer.stop()
    await task
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.zst"))
    assert files == [
        "raw/spool/venue=kraken/date=2026-11-10/hour=00/trades-c1.jsonl.zst",
        "raw/spool/venue=kraken/date=2026-11-10/hour=01/trades-c1.jsonl.zst",
    ]
    back = [r for f in sorted(tmp_path.rglob("*.zst")) for r in read_spool(f)]
    assert [r["rx_seq"] for r in back] == list(range(60))
    assert back[0]["payload"] == '{"a":0,"text":"line\\nbreak"}'  # verbatim text
    assert writer.records_written == 60
    assert not list(tmp_path.rglob("*.partial"))


async def test_late_record_never_reopens_a_closed_file(tmp_path: Path) -> None:
    clock = [T0 + 10]
    writer = SpoolWriter(tmp_path, "kraken", flush_interval_s=0.05, clock_ns=lambda: clock[0])
    task = asyncio.create_task(writer.run())
    await writer.put(rec(T0 + 1, 1))
    clock[0] = T0 + HOUR + 10 * 1_000_000_000
    await asyncio.sleep(0.15)  # hour 00 closed
    await writer.put(rec(T0 + 2, 2))  # a late record for hour 00
    await writer.stop()
    await task
    names = sorted(p.name for p in (tmp_path.rglob("hour=00/*")))
    assert names == ["trades-c1-1.jsonl.zst", "trades-c1.jsonl.zst"]


async def test_separate_files_per_channel_and_connection(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path, "kraken", flush_interval_s=0.05)
    task = asyncio.create_task(writer.run())
    now = T0
    await writer.put(rec(now, 1, "trades", "a"))
    await writer.put(rec(now, 2, "book_l2", "a"))
    await writer.put(rec(now, 3, "book_l2", "b"))
    await writer.stop()
    await task
    assert sorted(p.name for p in tmp_path.rglob("*.zst")) == [
        "book_l2-a.jsonl.zst",
        "book_l2-b.jsonl.zst",
        "trades-a.jsonl.zst",
    ]


def test_truncated_frame_reads_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl.zst"
    c = zstandard.ZstdCompressor()
    good = c.compress(b'{"i":1}\n{"i":2}\n')
    cut = c.compress(b'{"i":3}\n' * 100)[:-7]
    path.write_bytes(good + cut)
    assert [r["i"] for r in read_spool(path)] == [1, 2]
    ok, n = decodable(path)
    assert not ok
    assert n >= 2
    path.write_bytes(good + c.compress(b'{"i":3}\n{"i":'))  # partial last line
    assert [r["i"] for r in read_spool(path)] == [1, 2, 3]
    assert decodable(path) == (True, 3)


def test_recover_partials(tmp_path: Path) -> None:
    d = tmp_path / "raw/spool/venue=kraken/date=2026-11-10/hour=00"
    d.mkdir(parents=True)
    (d / "trades-a.jsonl.zst.partial").write_bytes(b"x")
    (d / "trades-b.jsonl.zst").write_bytes(b"done")
    (d / "trades-b.jsonl.zst.partial").write_bytes(b"y")
    got = sorted(p.name for p in recover_partials(tmp_path, "kraken"))
    assert got == ["trades-a.jsonl.zst", "trades-b.recovered.jsonl.zst"]
    assert (d / "trades-b.jsonl.zst").read_bytes() == b"done"  # never overwritten
    assert recover_partials(tmp_path) == []
