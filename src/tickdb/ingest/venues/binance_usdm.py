"""Binance USD-M futures adapter: ``/market`` aggTrade, ``/public`` depth@100ms, REST depth.

Docs (checked 2026-11-10):
- https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book

Sync rule (DQ-04): buffer diffs, fetch a REST snapshot, drop events with ``u < lastUpdateId``;
the first applied event has ``U <= lastUpdateId <= u``; every later event has ``pu`` equal to
the previous ``u``. A break opens a ``seq_gap`` and a resync. Aggregate trade ids ``a`` are
contiguous per symbol, so a hole is a ``trade_id_gap`` (backfilled at M3).

Weights: depth limit 1000 costs 20 of 2400/min; the bucket allows 20/s (50 %).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, ClassVar

import orjson
from websockets.asyncio.client import ClientConnection

from tickdb.ingest.base import Conn, ConnSpec, FrameCheck, Issue, VenueIngestor

MS = 1_000_000
DEPTH_WEIGHT = {5: 2, 10: 2, 20: 2, 50: 2, 100: 5, 500: 10, 1000: 20}


@dataclass
class _Depth:
    status: str = "buffering"  # buffering | live
    buffer: list[tuple[int, int, int]] = field(default_factory=list)  # (U, u, pu)
    last_u: int | None = None


class BinanceUsdmIngestor(VenueIngestor):
    venue: ClassVar[str] = "binance-usdm"
    rest_rate: ClassVar[tuple[float, float]] = (20.0, 100.0)
    snapshot_wait_s: ClassVar[float] = 10.0
    snapshot_tries: ClassVar[int] = 5

    def connections(self) -> list[ConnSpec]:
        cfg = self.cfg
        streams = {"trades": cfg.trades.stream or "aggTrade", "book_l2": cfg.book.stream or "depth"}
        specs = []
        for channel, name in (("trades", "market"), ("book_l2", "public")):
            if channel not in cfg.channels:
                continue
            names = "/".join(f"{s.lower()}@{streams[channel]}" for s in cfg.symbols)
            route = cfg.ws_routes.get(channel, "/stream")
            url = f"{cfg.ws_url.rstrip('/')}{route}?streams={names}"
            specs.append(ConnSpec(name, url, tuple(cfg.symbols), frozenset({channel})))
        return specs

    async def subscribe(self, ws: ClientConnection, conn: Conn) -> None:
        conn.state["depth"] = {s: _Depth() for s in conn.book_symbols()}
        conn.state["last_trade"] = {}

    def on_frame(self, frame: str, ts_recv: int, conn: Conn) -> FrameCheck:
        message = orjson.loads(frame)
        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return FrameCheck()
        kind = data.get("e")
        symbol = str(data.get("s"))
        if kind == "aggTrade":
            check = FrameCheck("trades", symbol, int(data["T"]) * MS)
            trade_id = int(data["a"])
            last: dict[str, int] = conn.state["last_trade"]
            prev = last.get(symbol)
            if prev is not None and trade_id > prev + 1:
                detail: dict[str, Any] = {"seq_from": prev + 1, "seq_to": trade_id - 1}
                check.issues.append(Issue("trade_id_gap", symbol, "trades", detail))
            last[symbol] = max(trade_id, prev or trade_id)
            return check
        if kind == "depthUpdate":
            check = FrameCheck("book_l2", symbol, int(data["E"]) * MS)
            book: _Depth = conn.state["depth"][symbol]
            first, final, prev_final = int(data["U"]), int(data["u"]), int(data["pu"])
            if book.status == "buffering":
                book.buffer.append((first, final, prev_final))
            elif prev_final != book.last_u:
                gap: dict[str, Any] = {"seq_from": book.last_u, "seq_to": prev_final}
                check.issues.append(Issue("seq_gap", symbol, "book_l2", gap))
                book.status, book.buffer, book.last_u = "buffering", [], None
            else:
                book.last_u = final
            return check
        return FrameCheck(unknown=True)

    async def _snapshot(self, symbol: str, conn_id: str, kind: str) -> dict[str, Any] | None:
        limit = self.cfg.book.snapshot_limit or 1000
        got = await self.rest_get(
            "depth",
            f"{self.cfg.rest_url.rstrip('/')}/fapi/v1/depth",
            {"symbol": symbol, "limit": limit},
            weight=DEPTH_WEIGHT.get(limit, 20),
            kind=kind,
            symbol=symbol,
            conn_id=conn_id,
        )
        if got is None:
            return None
        body: dict[str, Any] = orjson.loads(got[0])
        return body

    async def resync(self, conn: Conn, symbol: str, reason: str) -> None:
        book: _Depth = conn.state["depth"][symbol]
        loop = asyncio.get_running_loop()
        for _ in range(self.snapshot_tries):
            snapshot = await self._snapshot(symbol, conn.conn_id, "rest_snapshot")
            if snapshot is None:
                await asyncio.sleep(1.0)
                continue
            last_update_id = int(snapshot["lastUpdateId"])
            deadline = loop.time() + self.snapshot_wait_s
            while True:
                book.buffer = [ev for ev in book.buffer if ev[1] >= last_update_id]
                if book.buffer or loop.time() > deadline:
                    break
                await asyncio.sleep(0.02)
            if not book.buffer:
                continue
            first = book.buffer[0]
            if first[0] > last_update_id:
                continue  # snapshot older than the buffered stream: fetch again
            chain_ok = all(b[2] == a[1] for a, b in zip(book.buffer, book.buffer[1:], strict=False))
            if not chain_ok:
                book.buffer = []
                continue
            book.last_u = book.buffer[-1][1]
            book.buffer = []
            book.status = "live"
            await self.resync_done(conn, symbol)
            return
        raise RuntimeError(f"{symbol}: no valid snapshot after {self.snapshot_tries} tries")

    async def checkpoint(self, symbol: str) -> None:
        await self._snapshot(symbol, "rest", "rest_checkpoint")
