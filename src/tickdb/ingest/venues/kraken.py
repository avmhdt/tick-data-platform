"""Kraken WS v2 adapter: ``book`` (CRC32 checksum) and ``trade``.

Docs (checked 2026-11-10):
- https://docs.kraken.com/api/docs/websocket-v2/book
- https://docs.kraken.com/api/docs/guides/spot-ws-book-v2
- https://docs.kraken.com/api/docs/websocket-v2/trade

The book arrives as a snapshot after subscribe, then updates. After applying all of an
update's levels and truncating to the subscribed depth, the CRC32 of the top 10 levels must
equal the update's ``checksum``; a mismatch is a ``checksum_fail`` and a resubscribe. Numbers
are read as their wire text (``parse_float=str``) because the checksum is over that text.
Kraken books carry no sequence number; the spool's ``rx_seq`` stands in (spec 7b).
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

import orjson
from websockets.asyncio.client import ClientConnection

from tickdb.ingest.base import Conn, ConnSpec, FrameCheck, Issue, VenueIngestor
from tickdb.ingest.book import ASK, BID, L2Book, kraken_checksum
from tickdb.timeutil import iso_to_ns


def _levels(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(str(r["price"]), str(r["qty"])) for r in rows]


class KrakenIngestor(VenueIngestor):
    venue: ClassVar[str] = "kraken"

    @property
    def depth(self) -> int:
        return self.cfg.book.depth or 10

    def connections(self) -> list[ConnSpec]:
        data = frozenset(c for c in self.cfg.channels if c in ("trades", "book_l2"))
        return [ConnSpec("ws", self.cfg.ws_url, tuple(self.cfg.symbols), data)]

    def _book_sub(self, method: str, symbols: list[str]) -> str:
        params: dict[str, Any] = {"channel": "book", "symbol": symbols}
        if method == "subscribe":
            params["depth"] = self.depth
        return orjson.dumps({"method": method, "params": params}).decode()

    async def subscribe(self, ws: ClientConnection, conn: Conn) -> None:
        conn.state["books"] = {s: L2Book() for s in conn.book_symbols()}
        conn.state["live"] = set()
        conn.state["last_trade"] = {}
        if conn.book_symbols():
            await ws.send(self._book_sub("subscribe", list(conn.book_symbols())))
        if "trades" in conn.spec.channels:
            params = {"channel": "trade", "symbol": list(conn.spec.symbols), "snapshot": False}
            await ws.send(orjson.dumps({"method": "subscribe", "params": params}).decode())

    def on_frame(self, frame: str, ts_recv: int, conn: Conn) -> FrameCheck:
        if '"channel":"book"' in frame:
            return self._book(json.loads(frame, parse_float=str), conn)
        msg: dict[str, Any] = orjson.loads(frame)
        channel = msg.get("channel")
        if channel == "trade":
            return self._trades(msg, conn)
        if channel == "heartbeat":
            return FrameCheck("heartbeats")
        if channel == "status" or "method" in msg:
            return FrameCheck()
        return FrameCheck(unknown=True)

    def _trades(self, msg: dict[str, Any], conn: Conn) -> FrameCheck:
        rows = msg.get("data", [])
        symbol = rows[0]["symbol"] if rows else None
        ts = iso_to_ns(rows[0]["timestamp"]) if rows and "timestamp" in rows[0] else None
        check = FrameCheck("trades", symbol, ts)
        last: dict[str, int] = conn.state["last_trade"]
        for row in rows:
            sym, trade_id = row["symbol"], int(row["trade_id"])
            prev = last.get(sym)
            if prev is not None and trade_id > prev + 1:
                detail: dict[str, Any] = {"seq_from": prev + 1, "seq_to": trade_id - 1}
                check.issues.append(Issue("trade_id_gap", sym, "trades", detail))
            last[sym] = max(trade_id, prev or trade_id)
        return check

    def _book(self, msg: dict[str, Any], conn: Conn) -> FrameCheck:
        kind = msg.get("type")
        rows = msg.get("data", [])
        if not rows:
            return FrameCheck("book_l2")
        check = FrameCheck("book_l2", rows[0]["symbol"])
        live: set[str] = conn.state["live"]
        for row in rows:
            symbol = row["symbol"]
            book: L2Book = conn.state["books"][symbol]
            if "timestamp" in row:
                check.ts_exchange = iso_to_ns(row["timestamp"])
            if kind == "snapshot":
                book.load(_levels(row.get("bids", [])), _levels(row.get("asks", [])))
            elif symbol in live:
                for price, qty in _levels(row.get("bids", [])):
                    book.apply(BID, price, qty)
                for price, qty in _levels(row.get("asks", [])):
                    book.apply(ASK, price, qty)
            else:
                continue  # updates of a book that is resyncing are ignored until its snapshot
            book.truncate(self.depth)
            expected = int(row["checksum"])
            got = kraken_checksum(book.top(BID, 10), book.top(ASK, 10))
            if got != expected:
                live.discard(symbol)
                detail = {"expected": expected, "computed": got, "frame": kind}
                check.issues.append(Issue("checksum_fail", symbol, "book_l2", detail))
            elif kind == "snapshot":
                live.add(symbol)
        return check

    async def resync(self, conn: Conn, symbol: str, reason: str) -> None:
        live: set[str] = conn.state["live"]
        if reason != "connect":
            live.discard(symbol)
            await conn.ws.send(self._book_sub("unsubscribe", [symbol]))
            await conn.ws.send(self._book_sub("subscribe", [symbol]))
        while symbol not in live:
            await conn.wait_frame(30.0)
        await self.resync_done(conn, symbol)
