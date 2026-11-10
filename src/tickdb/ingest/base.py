"""Ingestor framework (FR-01, FR-03, FR-04, FR-18, FR-21).

One :class:`VenueIngestor` per venue process. Each connection spec runs its own loop:

- connect and subscribe; on any unplanned close, write ``disconnect`` gap events for every
  stream on that connection and reconnect after :class:`Backoff` (full jitter);
- stamp ``ts_recv = time.time_ns()`` for every frame before it is parsed, then spool the
  verbatim text with ``conn_id`` and ``rx_seq``;
- validate the frame (sequence, checksum, trade ids) through the venue adapter; a book problem
  writes the gap event and starts a resync in the background, so frames keep flowing;
- a silence watchdog closes a connection whose stream has been quiet past its threshold;
- planned rotation is make-before-break: the successor connects, subscribes and syncs before
  the old connection closes, and a ``rotate`` event marks the switch (no gap is written).

Under the one shared crypto capture these ingestors are never pointed at live venues; the CLI
refuses to (docs/decisions.md). They are exercised against mock venues in the test suite.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import random
import secrets
import shutil
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import httpx
import orjson
import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tickdb import __version__
from tickdb.config import TickdbConfig, VenueConfig
from tickdb.ingest.backoff import Backoff
from tickdb.ingest.metrics import Metrics
from tickdb.ingest.ratelimit import TokenBucket
from tickdb.ingest.spool import SpoolWriter, make_record
from tickdb.timeutil import parse_duration_ns

USER_AGENT = f"tickdb/{__version__} (+https://github.com/avmhdt/tick-data-platform)"
log = structlog.get_logger()


def uuid7() -> str:
    """UUIDv7 text (time-ordered); Python 3.12 has no ``uuid.uuid7``."""
    ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(secrets.token_bytes(10), "big")
    value = (ms & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= (rand >> 64 & 0xFFF) << 64
    value |= 0b10 << 62
    value |= rand & ((1 << 62) - 1)
    h = f"{value:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


@dataclass(frozen=True)
class ConnSpec:
    """One websocket connection a venue needs."""

    name: str
    url: str
    symbols: tuple[str, ...]
    channels: frozenset[str]


@dataclass(frozen=True)
class Issue:
    """A validation failure found in a frame."""

    kind: str  # seq_gap | checksum_fail | trade_id_gap | crossed | protocol
    symbol: str
    channel: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class FrameCheck:
    """What the adapter learned from one frame."""

    channel: str = "control"  # trades | book_l2 | heartbeats | control
    symbol: str | None = None
    ts_exchange: int | None = None
    issues: list[Issue] = field(default_factory=list)
    unknown: bool = False


@dataclass
class Conn:
    """One open websocket and its per-connection venue state."""

    spec: ConnSpec
    conn_id: str
    ws: ClientConnection
    opened: float
    rx_seq: int = 0
    data_frames: int = 0
    state: dict[str, Any] = field(default_factory=dict)
    last_frame: dict[tuple[str, str], float] = field(default_factory=dict)
    close_reason: str = ""
    synced: set[str] = field(default_factory=set)
    resyncing: dict[str, int] = field(default_factory=dict)  # symbol -> resync start ts
    planned_close: bool = False
    frame_event: asyncio.Event = field(default_factory=asyncio.Event)

    async def wait_frame(self, limit_s: float = 30.0) -> None:
        """Wait for the next frame on this connection (raises TimeoutError)."""
        self.frame_event.clear()
        await asyncio.wait_for(self.frame_event.wait(), limit_s)

    def book_symbols(self) -> tuple[str, ...]:
        return self.spec.symbols if "book_l2" in self.spec.channels else ()

    @property
    def ready(self) -> bool:
        if self.book_symbols():
            return set(self.book_symbols()) <= self.synced
        return self.data_frames > 0


class VenueIngestor(abc.ABC):
    """Base class. Subclasses implement the four venue hooks."""

    venue: ClassVar[str]
    rest_rate: ClassVar[tuple[float, float]] = (1.0, 5.0)  # tokens/s, burst (50 % of limits)

    def __init__(
        self,
        cfg: VenueConfig,
        tcfg: TickdbConfig,
        spool: SpoolWriter,
        metrics: Metrics,
        *,
        http: httpx.AsyncClient | None = None,
        rng: random.Random | None = None,
        clock_probe: Callable[[], float | None] | None = None,
        data_dir: Path | None = None,
    ) -> None:
        self.cfg = cfg
        self.tcfg = tcfg
        self.spool = spool
        self.metrics = metrics
        self.http = http or httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT}, timeout=tcfg.rest.timeout_s
        )
        self.rng = rng or random.Random()
        self.clock_probe = clock_probe
        self.data_dir = data_dir
        self.bucket = TokenBucket(*self.rest_rate)
        self.active: dict[str, Conn] = {}
        self.stop_event = asyncio.Event()
        self.loop_lags: deque[float] = deque(maxlen=100_000)
        self.resync_times: list[float] = []
        lifetime = cfg.max_conn_lifetime
        self.lifetime_s = parse_duration_ns(lifetime) / 1e9 if lifetime else None
        self._resync_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------ venue hooks

    @abc.abstractmethod
    def connections(self) -> list[ConnSpec]:
        """Connection specs for the configured symbols and channels."""

    @abc.abstractmethod
    async def subscribe(self, ws: ClientConnection, conn: Conn) -> None:
        """Send subscription messages (if any) on a new connection."""

    @abc.abstractmethod
    def on_frame(self, frame: str, ts_recv: int, conn: Conn) -> FrameCheck:
        """Classify and validate one frame (sequence / checksum / trade ids)."""

    @abc.abstractmethod
    async def resync(self, conn: Conn, symbol: str, reason: str) -> None:
        """Restore a correct book: REST snapshot or resubscribe. Calls ``resync_done``."""

    async def checkpoint(self, symbol: str) -> None:  # noqa: B027 - optional hook
        """Spool a REST depth checkpoint (DQ-04); venues without one do nothing."""

    # ------------------------------------------------------------------ spool helpers

    async def event(self, conn_id: str, etype: str, **fields: Any) -> None:
        if "type" in fields:
            raise ValueError("event fields may not override 'type'")
        payload = orjson.dumps({"type": etype, **fields}).decode()
        await self.spool.put(
            make_record(
                venue=self.venue,
                conn_id=conn_id,
                rx_seq=0,
                ts_recv=time.time_ns(),
                kind="event",
                channel="control",
                symbol=fields.get("symbol"),
                payload=payload,
            )
        )

    async def rest_get(
        self,
        endpoint: str,
        url: str,
        params: dict[str, str | int],
        *,
        weight: float = 1.0,
        kind: str,
        symbol: str,
        conn_id: str = "rest",
    ) -> tuple[str, int] | None:
        """Rate-limited GET whose verbatim body is spooled as ``kind``; None on failure."""
        await self.bucket.acquire(weight)
        try:
            response = await self.http.get(url, params=params)
        except httpx.HTTPError as exc:
            self.metrics.rest_requests.labels(self.venue, endpoint, "error").inc()
            await self.event(
                conn_id, "rest_error", symbol=symbol, endpoint=endpoint, error=str(exc)
            )
            return None
        ts_recv = time.time_ns()
        self.metrics.rest_requests.labels(self.venue, endpoint, str(response.status_code)).inc()
        if response.status_code != 200:
            await self.event(
                conn_id, "rest_error", symbol=symbol, endpoint=endpoint, status=response.status_code
            )
            return None
        text = response.text
        await self.spool.put(
            make_record(
                venue=self.venue,
                conn_id=conn_id,
                rx_seq=0,
                ts_recv=ts_recv,
                kind=kind,
                channel="book_l2",
                symbol=symbol,
                payload=text,
            )
        )
        return text, ts_recv

    # ------------------------------------------------------------------ resync bookkeeping

    async def resync_done(self, conn: Conn, symbol: str) -> None:
        started = conn.resyncing.pop(symbol, None)
        conn.synced.add(symbol)
        took = (time.time_ns() - started) / 1e9 if started else 0.0
        self.resync_times.append(took)
        await self.event(conn.conn_id, "resync_done", symbol=symbol, seconds=round(took, 3))

    async def start_resync(self, conn: Conn, symbol: str, reason: str) -> None:
        if symbol in conn.resyncing:
            return
        conn.synced.discard(symbol)
        conn.resyncing[symbol] = time.time_ns()
        self.metrics.resyncs.labels(self.venue, symbol, reason).inc()
        await self.event(conn.conn_id, "resync_start", symbol=symbol, reason=reason)
        task = asyncio.create_task(self._resync_guard(conn, symbol, reason))
        self._resync_tasks.add(task)
        task.add_done_callback(self._resync_tasks.discard)

    async def _resync_guard(self, conn: Conn, symbol: str, reason: str) -> None:
        try:
            await self.resync(conn, symbol, reason)
        except Exception as exc:  # a failed resync closes the connection: reconnect resyncs
            log.warning("resync_failed", venue=self.venue, symbol=symbol, error=str(exc))
            conn.close_reason = "resync"
            with contextlib.suppress(Exception):
                await conn.ws.close()

    # ------------------------------------------------------------------ connection lifecycle

    async def _open(self, spec: ConnSpec) -> Conn:
        conn_id = uuid7()
        ws = await connect(
            spec.url, user_agent_header=USER_AGENT, open_timeout=10, max_size=None, ping_interval=20
        )
        conn = Conn(spec=spec, conn_id=conn_id, ws=ws, opened=time.monotonic())
        now = time.monotonic()
        for symbol in spec.symbols:
            for channel in spec.channels:
                conn.last_frame[(symbol, channel)] = now
        await self.event(conn_id, "connect", conn=spec.name, url=spec.url)
        self.metrics.ws_connected.labels(self.venue).inc()
        await self.subscribe(ws, conn)
        await self.event(conn_id, "subscribe", conn=spec.name, symbols=list(spec.symbols))
        for symbol in conn.book_symbols():
            await self.start_resync(conn, symbol, "connect")
        return conn

    async def _read(self, conn: Conn, backoff: Backoff) -> str:
        venue = self.venue
        stable_reset = self.tcfg.reconnect.stable_reset_s
        try:
            async for raw in conn.ws:
                ts_recv = time.time_ns()  # before any parsing (NFR-02)
                text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                conn.rx_seq += 1
                try:
                    check = self.on_frame(text, ts_recv, conn)
                except Exception as exc:  # never drop a frame silently: spool it as unknown
                    log.warning("frame_error", venue=venue, error=str(exc))
                    check = FrameCheck(unknown=True)
                await self.spool.put(
                    make_record(
                        venue=venue,
                        conn_id=conn.conn_id,
                        rx_seq=conn.rx_seq,
                        ts_recv=ts_recv,
                        kind="ws",
                        channel=check.channel,
                        symbol=check.symbol,
                        payload=text,
                    )
                )
                self.metrics.ingest_delay.labels(venue).observe((time.time_ns() - ts_recv) / 1e9)
                self.metrics.ws_frames.labels(venue, check.channel).inc()
                if check.channel in ("trades", "book_l2", "heartbeats"):
                    conn.data_frames += 1
                    now = time.monotonic()
                    if check.symbol is not None:
                        conn.last_frame[(check.symbol, check.channel)] = now
                    if check.ts_exchange is not None:
                        self.metrics.feed_latency.labels(venue, check.channel).observe(
                            max(0.0, (ts_recv - check.ts_exchange) / 1e9)
                        )
                for issue in check.issues:
                    await self._handle_issue(conn, issue)
                conn.frame_event.set()
                if backoff.attempt and time.monotonic() - conn.opened > stable_reset:
                    backoff.reset()
        except ConnectionClosed:
            pass
        return conn.close_reason or "closed"

    async def _handle_issue(self, conn: Conn, issue: Issue) -> None:
        await self.event(
            conn.conn_id, issue.kind, symbol=issue.symbol, channel=issue.channel, **issue.detail
        )
        if issue.kind == "seq_gap":
            self.metrics.seq_gaps.labels(self.venue, issue.symbol, issue.channel).inc()
        if issue.kind == "checksum_fail":
            self.metrics.checksum_failures.labels(self.venue, issue.symbol).inc()
        if issue.channel == "book_l2":
            await self.start_resync(conn, issue.symbol, issue.kind)

    async def _closed(self, conn: Conn, reason: str) -> None:
        self.metrics.ws_connected.labels(self.venue).dec()
        await self.event(conn.conn_id, "disconnect", conn=conn.spec.name, reason=reason)
        if conn.planned_close:
            return
        for symbol in conn.spec.symbols:
            for channel in sorted(conn.spec.channels):
                await self.event(
                    conn.conn_id, "gap_open", symbol=symbol, channel=channel, kind="disconnect"
                )

    async def _session(self, spec: ConnSpec, backoff: Backoff) -> str:
        """Run one logical connection, including planned rotations, until it fails."""
        try:
            conn = await self._open(spec)
        except (OSError, TimeoutError, InvalidStatus, ConnectionClosed) as exc:
            await self.event("none", "connect_failed", conn=spec.name, error=str(exc))
            return "connect_failed"
        self.active[spec.name] = conn
        reader = asyncio.create_task(self._read(conn, backoff))
        while True:
            timeout = None
            if self.lifetime_s is not None:
                timeout = max(0.0, conn.opened + self.lifetime_s - time.monotonic())
            stopper = asyncio.create_task(self.stop_event.wait())
            done, _ = await asyncio.wait(
                {reader, stopper}, timeout=timeout, return_when="FIRST_COMPLETED"
            )
            stopper.cancel()
            if reader in done:
                reason = reader.result()
                await self._closed(conn, reason)
                return reason
            if self.stop_event.is_set():
                conn.planned_close = True
                await conn.ws.close()
                await reader
                await self._closed(conn, "stop")
                return "stop"
            # planned rotation, make-before-break
            successor = await self._rotate(conn, backoff)
            if successor is None:
                conn.opened = time.monotonic() - self.lifetime_s + 60.0  # type: ignore[operator]
                continue
            new_conn, new_reader = successor
            conn.planned_close = True
            await self.event(new_conn.conn_id, "rotate", old=conn.conn_id, new=new_conn.conn_id)
            await conn.ws.close()
            await reader
            await self._closed(conn, "rotate")
            conn, reader = new_conn, new_reader
            self.active[spec.name] = conn

    async def _rotate(self, old: Conn, backoff: Backoff) -> tuple[Conn, asyncio.Task[str]] | None:
        try:
            new = await self._open(old.spec)
        except (OSError, TimeoutError, InvalidStatus, ConnectionClosed) as exc:
            await self.event(old.conn_id, "rotate_failed", error=str(exc))
            return None
        reader = asyncio.create_task(self._read(new, backoff))
        deadline = time.monotonic() + 60.0
        while not new.ready and not reader.done() and (left := deadline - time.monotonic()) > 0:
            with contextlib.suppress(TimeoutError):
                await new.wait_frame(min(left, 1.0))
        if new.ready and not reader.done():
            return new, reader
        new.planned_close = True
        await new.ws.close()
        await reader
        await self._closed(new, "rotate_failed")
        return None

    async def _conn_loop(self, spec: ConnSpec) -> None:
        rc = self.tcfg.reconnect
        backoff = Backoff(rc.base_s, rc.factor, rc.cap_s, rc.jitter, rng=self.rng)
        while not self.stop_event.is_set():
            reason = await self._session(spec, backoff)
            self.active.pop(spec.name, None)
            if self.stop_event.is_set():
                break
            self.metrics.ws_reconnects.labels(self.venue, reason).inc()
            delay = backoff.next_delay()
            await self.event("none", "reconnect_wait", conn=spec.name, reason=reason, delay_s=delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)

    # ------------------------------------------------------------------ background tasks

    async def _watchdog(self) -> None:
        limits = self.tcfg.silence_timeout_s
        while not self.stop_event.is_set():
            await asyncio.sleep(0.25)
            now = time.monotonic()
            for conn in list(self.active.values()):
                for (symbol, channel), last in conn.last_frame.items():
                    age = now - last
                    self.metrics.last_frame_age.labels(self.venue, symbol, channel).set(age)
                    limit = getattr(limits, channel, None)
                    if limit is not None and age > limit and not conn.close_reason:
                        conn.close_reason = "silence"
                        await self.event(
                            conn.conn_id, "silence", symbol=symbol, channel=channel, age_s=age
                        )
                        with contextlib.suppress(Exception):
                            await conn.ws.close()

    async def _loop_lag(self, interval: float = 0.05) -> None:
        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            start = loop.time()
            await asyncio.sleep(interval)
            lag = max(0.0, loop.time() - start - interval)
            self.loop_lags.append(lag)
            self.metrics.event_loop_lag.labels(self.venue).observe(lag)

    async def _checkpoints(self) -> None:
        every = self.cfg.book.rest_checkpoint_s
        if not every:
            return
        symbols = list(self.cfg.symbols)
        step = every / max(1, len(symbols))
        while not self.stop_event.is_set():
            for symbol in symbols:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stop_event.wait(), timeout=step)
                if self.stop_event.is_set():
                    return
                await self.checkpoint(symbol)

    async def _housekeeping(self, every_s: float = 60.0) -> None:
        while not self.stop_event.is_set():
            if self.clock_probe is not None:
                offset_ms = await asyncio.to_thread(self.clock_probe)
                if offset_ms is not None:
                    self.metrics.clock_offset.set(offset_ms / 1000.0)
                    await self.event("none", "clock", offset_ms=round(offset_ms, 3))
            if self.data_dir is not None and self.data_dir.exists():
                self.metrics.disk_free.labels(str(self.data_dir)).set(
                    shutil.disk_usage(self.data_dir).free
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), timeout=every_s)

    # ------------------------------------------------------------------ entry points

    async def run(self, until: Awaitable[Any] | None = None) -> None:
        """Run until ``stop()`` (or ``until`` completes), then flush and close the spool."""
        writer = asyncio.create_task(self.spool.run())
        tasks = [asyncio.create_task(self._conn_loop(spec)) for spec in self.connections()]
        tasks += [
            asyncio.create_task(self._watchdog()),
            asyncio.create_task(self._loop_lag()),
            asyncio.create_task(self._checkpoints()),
            asyncio.create_task(self._housekeeping()),
        ]
        if until is not None:
            stopper = asyncio.ensure_future(until)
            stopper.add_done_callback(lambda _f: self.stop_event.set())
        try:
            await self.stop_event.wait()
        finally:
            self.stop_event.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in list(self._resync_tasks):
                task.cancel()
            await asyncio.gather(*self._resync_tasks, return_exceptions=True)
            await self.spool.stop()
            await writer
            await self.http.aclose()

    def stop(self) -> None:
        self.stop_event.set()
