"""coinbase normalizer (Exchange feed): ``matches`` -> trades, ``level2_batch`` -> book_l2.

Docs: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

``match.side`` is the maker side, so ``sell`` means the buyer aggressed (+1). ``last_match``
is the trade before the subscription and is not a new trade. ``level2_batch`` carries no
sequence; book ``seq`` is the per-connection receive counter.
"""

from __future__ import annotations

from typing import Any

import orjson

from tickdb.ingest.book import ASK, BID
from tickdb.normalize.pushbook import PushBookNormalizer
from tickdb.normalize.rawlog import RawRecord
from tickdb.timeutil import iso_to_ns


class CoinbaseNormalizer(PushBookNormalizer):
    venue = "coinbase"

    def on_backfill(self, symbol: str, rec: RawRecord) -> None:
        for t in orjson.loads(rec.payload):
            ts = iso_to_ns(t["time"])
            side = 1 if t["side"] == "sell" else -1  # maker side, as on the websocket
            self.engine.trade(
                symbol, ts, ts, str(t["trade_id"]), t["price"], t["size"], side, "rest", rec.ts
            )

    def on_frame(self, rec: RawRecord) -> None:
        msg: dict[str, Any] = orjson.loads(rec.payload)
        kind = msg.get("type")
        symbol = str(msg.get("product_id", ""))
        if kind == "match":
            self.engine.trade(
                symbol,
                iso_to_ns(msg["time"]),
                rec.ts,
                str(msg["trade_id"]),
                msg["price"],
                msg["size"],
                1 if msg["side"] == "sell" else -1,
            )
            return
        if kind in ("last_match", "heartbeat", "subscriptions"):
            self.engine.counters[f"{kind}_frames"] += 1
            return
        if kind not in ("snapshot", "l2update"):
            self.engine.counters["unknown_frames"] += 1
            return
        if not self.is_active(rec) or symbol not in self.books:
            self.engine.counters["standby_book_frames"] += 1
            return
        if kind == "snapshot":
            bids = [(p, q) for p, q, *_ in msg.get("bids", [])]
            asks = [(p, q) for p, q, *_ in msg.get("asks", [])]
            self.snapshot(symbol, rec.ts, rec.ts, rec.conn, bids, asks)
            return
        levels = [
            (BID if side == "buy" else ASK, price, size)
            for side, price, size in msg.get("changes", [])
        ]
        ts_exchange = iso_to_ns(msg["time"]) if "time" in msg else rec.ts
        self.update(symbol, ts_exchange, rec.ts, rec.conn, levels)
