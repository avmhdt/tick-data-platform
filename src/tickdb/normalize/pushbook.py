"""Shared logic for venues whose websocket pushes the book snapshot (coinbase, kraken).

``live`` means the recorder had the stream in sync (its hourly ``state`` record says LIVE, or
a websocket snapshot arrived on the active connection): every update from the active
connection is archived. ``known`` means this replay also knows the book's levels, from a
websocket snapshot, from the ``state`` record's levels (Kraken keeps them) or from the
previous day's end-of-day book when the same connection stayed active across midnight. Only
a known book can be checksummed, and derived BBO is flagged ``in_resync`` until it is known.
A known book that the day started with is written as one snapshot batch (``conn_id`` =
``state`` or ``carry``) so each day's archive replays on its own.
"""

from __future__ import annotations

from typing import Any

from tickdb.ingest.book import ASK, BID, L2Book
from tickdb.normalize.engine import Engine, VenueNormalizer


class PushBookNormalizer(VenueNormalizer):
    logical = "ws"

    def __init__(self, engine: Engine) -> None:
        super().__init__(engine)
        self.live: set[str] = set()
        self.known: set[str] = set()
        self.books = {s: L2Book() for s in engine.symbols}
        self.rx: dict[str, int] = {}  # per-connection receive counter (book seq)
        self.seeded = False

    def next_seq(self, conn: str) -> int:
        self.rx[conn] = self.rx.get(conn, 0) + 1
        return self.rx[conn]

    def seed(self, state: dict[str, Any], ts: int) -> None:
        super().seed(state, ts)
        if self.seeded:
            return
        self.seeded = True
        active = self.active.get(self.logical)
        for symbol, book in state.get("books", {}).items():
            if symbol not in self.books or book.get("state") != "LIVE":
                continue
            self.live.add(symbol)
            bids = [(str(p), str(q)) for p, q, *_ in book.get("bids", [])]
            asks = [(str(p), str(q)) for p, q, *_ in book.get("asks", [])]
            origin = "state"
            if not bids and not asks:
                carry = self.engine.carry.get(symbol, {})
                if not carry.get("known") or carry.get("conn") != active:
                    continue  # levels unknown until the next snapshot
                bids = [(p, q) for p, q in carry.get("bids", [])]
                asks = [(p, q) for p, q in carry.get("asks", [])]
                origin = "carry"
            self.books[symbol].load(bids, asks)
            levels = [(BID, p, q) for p, q in bids] + [(ASK, p, q) for p, q in asks]
            self.engine.book(symbol, ts, ts, 0, True, origin, levels)
            self.known.add(symbol)
            self.engine.counters[f"day_start_{origin}"] += 1

    def lose(self, symbol: str, kind: str, ts: int) -> None:
        if symbol in self.live:
            self.engine.open_gap(symbol, "book_l2", kind, ts)
        self.live.discard(symbol)
        self.known.discard(symbol)

    def distrust(self, symbol: str, kind: str, ts: int) -> None:
        """The stream keeps flowing but the book is wrong (checksum): archive, don't trust."""
        self.engine.open_gap(symbol, "book_l2", kind, ts)
        self.known.discard(symbol)

    def on_connected(self, logical: str, ts: int) -> None:
        for symbol in self.engine.symbols:
            self.lose(symbol, "disconnect", ts)

    def on_disconnected(self, logical: str, reason: str, ts: int) -> None:
        for symbol in self.engine.symbols:
            self.lose(symbol, "disconnect", ts)
            self.engine.open_gap(symbol, "trades", "disconnect", ts)

    def on_stale(self, symbol: str, ts: int) -> None:
        if symbol in self.books:
            self.lose(symbol, "silence", ts)

    def snapshot(
        self, symbol: str, ts_exchange: int, ts_recv: int, conn: str, bids: Any, asks: Any
    ) -> None:
        self.books[symbol].load(bids, asks)
        self.engine.recovered_by(symbol, "book_l2", "snapshot")
        levels = [(BID, p, q) for p, q in bids] + [(ASK, p, q) for p, q in asks]
        self.engine.book(symbol, ts_exchange, ts_recv, self.next_seq(conn), True, conn, levels)
        self.live.add(symbol)
        self.known.add(symbol)

    def update(
        self,
        symbol: str,
        ts_exchange: int,
        ts_recv: int,
        conn: str,
        levels: list[tuple[int, str, str]],
    ) -> bool:
        """Archive one update batch; returns False if the stream is not in sync."""
        if symbol not in self.live:
            self.engine.counters["updates_while_resyncing"] += 1
            return False
        book = self.books[symbol]
        for side, price, qty in levels:
            book.apply(side, price, qty)
        self.engine.book(symbol, ts_exchange, ts_recv, self.next_seq(conn), False, conn, levels)
        if symbol not in self.known:
            self.engine.counters["updates_book_unknown"] += 1
        return True

    def book_end(self) -> dict[str, dict[str, Any]]:
        active = self.active.get(self.logical)
        return {
            symbol: {
                "known": symbol in self.known and symbol in self.live,
                "conn": active,
                "bids": [list(lv) for lv in book.top(BID, len(book.bids))],
                "asks": [list(lv) for lv in book.top(ASK, len(book.asks))],
            }
            for symbol, book in self.books.items()
        }
