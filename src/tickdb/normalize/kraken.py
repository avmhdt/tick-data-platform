"""kraken normalizer (WS v2): ``trade`` -> trades, ``book`` -> book_l2, CRC32 verified.

Docs: https://docs.kraken.com/api/docs/websocket-v2/book and .../guides/spot-ws-book-v2

Every update is applied, the book truncated to the subscribed depth (1000 in the shared
capture, D-06) and the CRC32 of the top 10 compared with the message's ``checksum``
(AC-W-05: 100 % outside resync windows). A mismatch opens a ``checksum`` gap; the recorder
resubscribes and the next snapshot ends it. Numbers are read as wire text.
"""

from __future__ import annotations

import json
from typing import Any

import orjson

from tickdb.ingest.book import ASK, BID, kraken_checksum
from tickdb.normalize.engine import Engine
from tickdb.normalize.pushbook import PushBookNormalizer
from tickdb.normalize.rawlog import RawRecord
from tickdb.timeutil import iso_to_ns


def _levels(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(str(r["price"]), str(r["qty"])) for r in rows]


class KrakenNormalizer(PushBookNormalizer):
    venue = "kraken"

    def __init__(self, engine: Engine, depth: int = 1000) -> None:
        super().__init__(engine)
        self.depth = depth

    def on_frame(self, rec: RawRecord) -> None:
        if b'"channel":"book"' in rec.payload:
            self._book(json.loads(rec.payload, parse_float=str), rec)
            return
        msg: dict[str, Any] = orjson.loads(rec.payload)
        channel = msg.get("channel")
        if channel == "trade":
            for row in msg.get("data", []):
                self.engine.trade(
                    row["symbol"],
                    iso_to_ns(row["timestamp"]),
                    rec.ts,
                    str(row["trade_id"]),
                    repr(row["price"]),
                    repr(row["qty"]),
                    1 if row["side"] == "buy" else -1,  # side is the taker side
                )
        elif channel in ("heartbeat", "status") or "method" in msg:
            self.engine.counters["control_frames"] += 1
        else:
            self.engine.counters["unknown_frames"] += 1

    def _book(self, msg: dict[str, Any], rec: RawRecord) -> None:
        if not self.is_active(rec):
            self.engine.counters["standby_book_frames"] += 1
            return
        kind = msg.get("type")
        for row in msg.get("data", []):
            symbol = row["symbol"]
            if symbol not in self.books:
                self.engine.counters["unknown_symbol"] += 1
                continue
            ts_exchange = iso_to_ns(row["timestamp"]) if "timestamp" in row else rec.ts
            bids, asks = _levels(row.get("bids", [])), _levels(row.get("asks", []))
            book = self.books[symbol]
            last_good = self.engine.last_good.get((symbol, "book_l2"))
            if kind == "snapshot":
                self.snapshot(symbol, ts_exchange, rec.ts, rec.conn, bids, asks)
            else:
                levels = [(BID, p, q) for p, q in bids] + [(ASK, p, q) for p, q in asks]
                if not self.update(symbol, ts_exchange, rec.ts, rec.conn, levels):
                    continue
            book.truncate(self.depth)
            if "checksum" not in row:
                continue
            if symbol not in self.known:
                self.engine.counters["checksum_skipped_book_unknown"] += 1
                continue
            if kraken_checksum(book.top(BID, 10), book.top(ASK, 10)) == int(row["checksum"]):
                self.engine.counters["checksum_ok"] += 1
            else:
                self.engine.counters["checksum_fail"] += 1
                self.distrust(symbol, "checksum", rec.ts, last_good)
