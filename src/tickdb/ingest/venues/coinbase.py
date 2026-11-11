"""Coinbase Exchange feed adapter: ``level2_batch``, ``matches``, ``heartbeat``; REST book.

Docs (checked 2026-11-10):
- https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
- https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits
- https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book

The shared capture uses this feed (no auth). ``level2_batch`` has no sequence number, so the
spec's ``sequence_num`` rule (an Advanced Trade field) cannot apply: integrity rests on the
snapshot-before-update rule, a crossed-book check after each update, the silence watchdog
and REST checkpoints every 600 s (DQ-04). ``match.side`` is the maker side; ``trade_id`` is
contiguous per product, so a hole is a ``trade_id_gap``. Public REST: 10 req/s, bucket 5/s.
"""

from __future__ import annotations

from typing import Any, ClassVar

import orjson
from websockets.asyncio.client import ClientConnection

from tickdb.ingest.base import Conn, ConnSpec, FrameCheck, Issue, VenueIngestor
from tickdb.ingest.book import ASK, BID, L2Book
from tickdb.timeutil import iso_to_ns


class CoinbaseIngestor(VenueIngestor):
    venue: ClassVar[str] = "coinbase"
    rest_rate: ClassVar[tuple[float, float]] = (5.0, 5.0)

    def _channel_names(self) -> list[str]:
        names = {
            "trades": self.cfg.trades.stream or "matches",
            "book_l2": self.cfg.book.stream or "level2_batch",
            "heartbeats": "heartbeat",
        }
        return [names[c] for c in self.cfg.channels]

    def connections(self) -> list[ConnSpec]:
        data = frozenset(c for c in self.cfg.channels if c in ("trades", "book_l2"))
        return [ConnSpec("ws", self.cfg.ws_url, tuple(self.cfg.symbols), data)]

    async def subscribe(self, ws: ClientConnection, conn: Conn) -> None:
        conn.state["books"] = {s: L2Book() for s in conn.book_symbols()}
        conn.state["last_trade"] = {}
        message = {
            "type": "subscribe",
            "product_ids": list(conn.spec.symbols),
            "channels": self._channel_names(),
        }
        await ws.send(orjson.dumps(message).decode())

    def on_frame(self, frame: str, ts_recv: int, conn: Conn) -> FrameCheck:
        msg: dict[str, Any] = orjson.loads(frame)
        kind = msg.get("type")
        symbol = str(msg.get("product_id", ""))
        if kind in ("match", "last_match"):
            check = FrameCheck("trades", symbol, iso_to_ns(msg["time"]) if "time" in msg else None)
            trade_id = int(msg["trade_id"])
            last: dict[str, int] = conn.state["last_trade"]
            prev = last.get(symbol)
            if kind == "match" and prev is not None and trade_id > prev + 1:
                detail: dict[str, Any] = {"seq_from": prev + 1, "seq_to": trade_id - 1}
                check.issues.append(Issue("trade_id_gap", symbol, "trades", detail))
            last[symbol] = max(trade_id, prev or trade_id)
            return check
        if kind == "snapshot":
            book: L2Book = conn.state["books"][symbol]
            book.load(
                ((p, q) for p, q, *_ in msg.get("bids", [])),
                ((p, q) for p, q, *_ in msg.get("asks", [])),
            )
            conn.state.setdefault("snapshot_seen", set()).add(symbol)
            return FrameCheck("book_l2", symbol, None)
        if kind == "l2update":
            check = FrameCheck("book_l2", symbol, iso_to_ns(msg["time"]) if "time" in msg else None)
            if symbol not in conn.state.get("snapshot_seen", set()):
                if symbol not in conn.resyncing:  # an update with no snapshot behind it
                    check.issues.append(
                        Issue("protocol", symbol, "book_l2", {"why": "no snapshot"})
                    )
                return check
            book = conn.state["books"][symbol]
            for side, price, size in msg.get("changes", []):
                book.apply(BID if side == "buy" else ASK, price, size)
            if book.crossed():
                conn.state["snapshot_seen"].discard(symbol)
                check.issues.append(Issue("crossed", symbol, "book_l2", {}))
            return check
        if kind == "heartbeat":
            return FrameCheck(
                "heartbeats", symbol, iso_to_ns(msg["time"]) if "time" in msg else None
            )
        if kind == "subscriptions":
            return FrameCheck()
        return FrameCheck(unknown=True)

    async def resync(self, conn: Conn, symbol: str, reason: str) -> None:
        if reason != "connect":
            conn.state.get("snapshot_seen", set()).discard(symbol)
            channel = self.cfg.book.stream or "level2_batch"
            for action in ("unsubscribe", "subscribe"):
                message = {"type": action, "product_ids": [symbol], "channels": [channel]}
                await conn.ws.send(orjson.dumps(message).decode())
        # Done once the snapshot for this product arrives on the websocket.
        while symbol not in conn.state.get("snapshot_seen", set()):
            await conn.wait_frame(30.0)
        await self.resync_done(conn, symbol)

    async def checkpoint(self, symbol: str) -> None:
        await self.rest_get(
            "book",
            f"{self.cfg.rest_url.rstrip('/')}/products/{symbol}/book",
            {"level": 2},
            kind="rest_checkpoint",
            symbol=symbol,
        )
