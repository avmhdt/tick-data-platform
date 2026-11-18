"""Crash-safe raw spool (FR-04, spec 7b/7d).

One JSON object per line, verbatim payload text, compressed as one zstd frame per flush (at
most every ``flush_interval_s``), so ``kill -9`` loses at most the last unflushed second and
leaves a decodable file. Layout::

    raw/spool/venue=<v>/date=YYYY-MM-DD/hour=HH/<channel>-<conn_id>.jsonl.zst

The hour is the UTC hour of ``ts_recv`` (DQ-02). Open files end in ``.partial`` and are fsynced
and renamed when their hour closes or the writer stops. ``recover_partials`` renames leftovers of
a killed process on start; ``read_spool`` stops cleanly at a truncated last frame.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import orjson
import zstandard

PARTIAL = ".partial"
SUFFIX = ".jsonl.zst"
SPOOL_DIR = Path("raw") / "spool"
NS_PER_HOUR = 3_600_000_000_000


def hour_key(ts_recv: int) -> tuple[str, str]:
    moment = datetime.fromtimestamp(ts_recv // 1_000_000_000, tz=UTC)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%H")


def spool_path(root: Path, venue: str, channel: str, conn_id: str, ts_recv: int) -> Path:
    day, hour = hour_key(ts_recv)
    return (
        root
        / SPOOL_DIR
        / f"venue={venue}"
        / f"date={day}"
        / f"hour={hour}"
        / f"{channel}-{conn_id}{SUFFIX}"
    )


def make_record(
    *,
    venue: str,
    conn_id: str,
    rx_seq: int,
    ts_recv: int,
    kind: str,
    channel: str,
    symbol: str | None,
    payload: str,
) -> dict[str, Any]:
    """A spool record in the spec's field order (``v`` = format version 1)."""
    return {
        "v": 1,
        "venue": venue,
        "conn_id": conn_id,
        "rx_seq": rx_seq,
        "ts_recv": ts_recv,
        "kind": kind,
        "channel": channel,
        "symbol": symbol,
        "payload": payload,
    }


@dataclass
class _Open:
    path: Path
    final: Path
    handle: IO[bytes]
    hour_end_ns: int
    buffer: list[bytes] = field(default_factory=list)
    bytes_written: int = 0


class SpoolWriter:
    """Async spool writer with a bounded queue (backpressure: ``put`` waits when full)."""

    def __init__(
        self,
        root: Path,
        venue: str,
        *,
        flush_interval_s: float = 1.0,
        level: int = 3,
        queue_max: int = 100_000,
        on_depth: Callable[[int], None] | None = None,
        on_bytes: Callable[[int], None] | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.root = root
        self.venue = venue
        self.flush_interval_s = flush_interval_s
        self.queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=queue_max)
        self.on_depth = on_depth
        self.on_bytes = on_bytes
        self.clock_ns = clock_ns
        self._compressor = zstandard.ZstdCompressor(level=level)
        self._files: dict[tuple[str, str, str], _Open] = {}
        self.records_written = 0
        self.files_closed: list[Path] = []
        self._closed = asyncio.Event()

    async def put(self, record: dict[str, Any]) -> None:
        await self.queue.put(record)
        if self.on_depth:
            self.on_depth(self.queue.qsize())

    def put_nowait(self, record: dict[str, Any]) -> None:
        self.queue.put_nowait(record)

    async def run(self) -> None:
        """Drain the queue until ``stop()``; flush at least every ``flush_interval_s``."""
        loop = asyncio.get_running_loop()
        next_flush = loop.time() + self.flush_interval_s
        stopping = False
        while not stopping:
            timeout = max(0.0, next_flush - loop.time())
            batch: list[dict[str, Any] | None] = []
            with contextlib.suppress(TimeoutError):
                batch.append(await asyncio.wait_for(self.queue.get(), timeout=timeout))
            while not self.queue.empty():
                batch.append(self.queue.get_nowait())
            for item in batch:
                if item is None:
                    stopping = True
                    break
                self._buffer(item)
            if stopping or loop.time() >= next_flush:
                await asyncio.to_thread(self._flush_all, self.clock_ns())
                next_flush = loop.time() + self.flush_interval_s
                if self.on_depth:
                    self.on_depth(self.queue.qsize())
        await asyncio.to_thread(self._close_all)
        self._closed.set()

    async def stop(self) -> None:
        await self.queue.put(None)
        await self._closed.wait()

    # ------------------------------------------------------------------ internals

    def _buffer(self, record: dict[str, Any]) -> None:
        ts = int(record["ts_recv"])
        day, hour = hour_key(ts)
        key = (str(record["channel"]), str(record["conn_id"]), f"{day}T{hour}")
        handle = self._files.get(key)
        if handle is None:
            final = spool_path(self.root, self.venue, key[0], key[1], ts)
            final.parent.mkdir(parents=True, exist_ok=True)
            n = 1
            while final.exists() or final.with_name(final.name + PARTIAL).exists():
                # a late record for an hour already closed: never touch the closed file
                final = final.with_name(f"{key[0]}-{key[1]}-{n}{SUFFIX}")
                n += 1
            partial = final.with_name(final.name + PARTIAL)
            hour_start = ts - ts % NS_PER_HOUR
            handle = _Open(partial, final, partial.open("ab"), hour_start + NS_PER_HOUR)
            self._files[key] = handle
        handle.buffer.append(orjson.dumps(record) + b"\n")
        self.records_written += 1

    def _flush_one(self, handle: _Open) -> None:
        if not handle.buffer:
            return
        frame = self._compressor.compress(b"".join(handle.buffer))
        handle.buffer.clear()
        handle.handle.write(frame)
        handle.handle.flush()  # into the OS page cache: survives kill -9 of this process
        handle.bytes_written += len(frame)
        if self.on_bytes:
            self.on_bytes(len(frame))

    def _close_one(self, key: tuple[str, str, str]) -> None:
        handle = self._files.pop(key)
        self._flush_one(handle)
        handle.handle.flush()
        os.fsync(handle.handle.fileno())
        handle.handle.close()
        os.replace(handle.path, handle.final)
        self.files_closed.append(handle.final)

    def _flush_all(self, now_ns: int) -> None:
        for key, handle in list(self._files.items()):
            self._flush_one(handle)
            if now_ns >= handle.hour_end_ns + 5 * 1_000_000_000:  # hour closed, 5 s late window
                self._close_one(key)

    def _close_all(self) -> None:
        for key in list(self._files):
            self._close_one(key)


def recover_partials(root: Path, venue: str | None = None) -> list[Path]:
    """Rename ``*.partial`` files left by a killed writer; their decodable prefix is kept."""
    base = root / SPOOL_DIR
    pattern = f"venue={venue}/**/*{PARTIAL}" if venue else f"**/*{PARTIAL}"
    recovered = []
    for partial in sorted(base.glob(pattern)):
        final = partial.with_name(partial.name.removesuffix(PARTIAL))
        if final.exists():  # never overwrite: keep both under a distinct name
            final = final.with_name(final.name.replace(SUFFIX, f".recovered{SUFFIX}"))
        os.replace(partial, final)
        recovered.append(final)
    return recovered


def _decode(path: Path, chunk_size: int = 1 << 20) -> Iterator[bytes | None]:
    """Decompressed bytes of every frame; yields ``None`` last if the final frame is cut."""
    dctx = zstandard.ZstdDecompressor()
    dobj = dctx.decompressobj()
    fed = False
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            while chunk:
                out = dobj.decompress(chunk)
                fed = True
                if out:
                    yield out
                if dobj.eof:
                    chunk = dobj.unused_data
                    dobj = dctx.decompressobj()
                    fed = False
                else:
                    chunk = b""
    if fed:
        yield None


def read_spool(path: Path) -> Iterator[dict[str, Any]]:
    """Decode a spool file; a cut last frame keeps its complete lines, a partial last line
    is dropped."""
    pending = b""
    for out in _decode(path):
        if out is None:
            break
        pending += out
        *lines, pending = pending.split(b"\n")
        for line in lines:
            if line:
                yield orjson.loads(line)


def decodable(path: Path) -> tuple[bool, int]:
    """(every frame complete, number of complete records)."""
    count = 0
    pending = b""
    for out in _decode(path):
        if out is None:
            return False, count
        pending += out
        count += pending.count(b"\n")
        pending = pending[pending.rfind(b"\n") + 1 :]
    return True, count
