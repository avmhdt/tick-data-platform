"""binance-usdm normalizer: aggTrade -> trades, depth@100ms + REST snapshots -> book_l2.

Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly

Sync (DQ-04): once live, every diff's ``pu`` equals the previous ``u``; a break opens a
``seq_gap`` and the stream waits for a REST snapshot (control ``snapshot``). The day starts
live from the recorder's hourly ``state`` record (sequence only, no levels), so the book's
levels are unknown until a snapshot or the day's first REST checkpoint is bridged.

Bridging a snapshot with lastUpdateId L needs the diff that straddles it (``U <= L <= u``),
which may arrive before or after the snapshot. When both are in hand (time ``t_known``):
buffered diffs with ``u < L`` are emitted with their own receive times; then the snapshot
batch (``seq = L``); then the diffs from the straddling one on, all stamped ``t_known``,
because that is when the book they build became known. The archive stays in receive-time
order and replays to the correct book (docs/decisions.md, M3).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

import orjson

from tickdb.ingest.book import ASK, BID, L2Book
from tickdb.normalize.checkpoints import compare
from tickdb.normalize.engine import Engine, VenueNormalizer
from tickdb.normalize.rawlog import RawRecord

MS = 1_000_000
BUFFER_MAX = 200_000


@dataclass
class _Diff:
    ts_recv: int
    ts_exchange: int
    first: int
    final: int
    prev: int
    conn: str
    levels: list[tuple[int, str, str]]


@dataclass
class _Pending:
    ts_recv: int
    body: dict[str, Any]
    recovery: str

    @property
    def last_id(self) -> int:
        return int(self.body["lastUpdateId"])


@dataclass
class _Sync:
    synced: bool = False  # pu chain intact
    known: bool = False  # book levels known (a snapshot has been bridged)
    last_u: int | None = None
    buffer: list[_Diff] = field(default_factory=list)
    pending: _Pending | None = None


def _levels(bids: list[list[str]], asks: list[list[str]]) -> list[tuple[int, str, str]]:
    return [(BID, p, q) for p, q, *_ in bids] + [(ASK, p, q) for p, q, *_ in asks]


class BinanceUsdmNormalizer(VenueNormalizer):
    venue = "binance-usdm"

    def __init__(self, engine: Engine) -> None:
        super().__init__(engine)
        self.sync = {s: _Sync() for s in engine.symbols}
        self.books = {s: L2Book() for s in engine.symbols}
        # recent applied diffs (final u, touched levels) and checkpoints waiting for the stream
        self.recent: dict[str, deque[tuple[int, set[tuple[int, float]]]]] = {
            s: deque(maxlen=2000) for s in engine.symbols
        }
        self.waiting: dict[str, list[dict[str, Any]]] = {s: [] for s in engine.symbols}

    # ------------------------------------------------------------------ control

    def seed(self, state: dict[str, Any], ts: int) -> None:
        super().seed(state, ts)
        for symbol, book in state.get("books", {}).items():
            sync = self.sync.get(symbol)
            if sync is None or sync.synced or sync.buffer:
                continue
            if book.get("state") == "LIVE" and book.get("last_seq") is not None:
                sync.synced, sync.last_u = True, int(book["last_seq"])
                carry = self.engine.carry.get(symbol, {})
                if carry.get("known") and carry.get("last_u") == sync.last_u:
                    # same diff chain across midnight: the previous day's book is exact
                    bids = [(p, q) for p, q in carry.get("bids", [])]
                    asks = [(p, q) for p, q in carry.get("asks", [])]
                    self.books[symbol].load(bids, asks)
                    levels = [(BID, p, q) for p, q in bids] + [(ASK, p, q) for p, q in asks]
                    self.engine.book(symbol, ts, ts, sync.last_u, True, "carry", levels)
                    sync.known = True
                    self.engine.counters["day_start_carry"] += 1

    def on_connected(self, logical: str, ts: int) -> None:
        if logical == "public":
            for symbol in self.sync:
                self._lose_sync(symbol, "disconnect", ts)

    def on_disconnected(self, logical: str, reason: str, ts: int) -> None:
        if logical == "public":
            for symbol in self.sync:
                self._lose_sync(symbol, "disconnect", ts)
        elif logical == "market":
            for symbol in self.engine.symbols:
                self.engine.open_gap(symbol, "trades", "disconnect", ts)

    def on_stale(self, symbol: str, ts: int) -> None:
        if symbol in self.sync:
            self._lose_sync(symbol, "silence", ts)

    def on_control(self, rec: RawRecord) -> None:
        ctrl = rec.ctrl or {}
        symbol = str(ctrl.get("symbol", ""))
        if ctrl.get("k") == "checkpoint" and symbol in self.sync:
            if not self.sync[symbol].known:
                self.engine.counters["ckpt_skipped"] += 1
                self._offer(symbol, rec, "checkpoint")
            else:
                self.waiting[symbol].append(orjson.loads(rec.payload))
                self._check_waiting(symbol)
        super().on_control(rec)

    def _check_waiting(self, symbol: str) -> None:
        """Compare checkpoints the stream has reached (sequence-aligned, DQ-04)."""
        sync = self.sync[symbol]
        if not self.waiting[symbol] or sync.last_u is None or not sync.known:
            return
        keep = []
        for body in self.waiting[symbol]:
            last_id = int(body["lastUpdateId"])
            if sync.last_u < last_id:
                keep.append(body)
                continue
            touched: set[tuple[int, float]] = set()
            for final, levels in self.recent[symbol]:
                if final >= last_id:
                    touched |= levels
            ok = compare(self.books[symbol], body, 100, touched)
            self.engine.counters["ckpt_match" if ok else "ckpt_mismatch"] += 1
        self.waiting[symbol] = keep

    def on_rest_snapshot(self, symbol: str, rec: RawRecord) -> None:
        if symbol in self.sync and not self.sync[symbol].known:
            self._offer(symbol, rec, "snapshot")

    def _lose_sync(self, symbol: str, kind: str, ts: int) -> None:
        sync = self.sync[symbol]
        self.engine.counters["ckpt_skipped"] += len(self.waiting[symbol])
        self.waiting[symbol] = []
        if sync.synced or sync.known:
            self.engine.open_gap(symbol, "book_l2", kind, ts)
        self._flush(symbol, sync)
        sync.synced, sync.known, sync.last_u, sync.pending = False, False, None, None

    # ------------------------------------------------------------------ frames

    def on_frame(self, rec: RawRecord) -> None:
        logical = rec.conn.split(".", 1)[0]
        if logical == "ticker":
            self.engine.counters["ticker_frames_skipped"] += 1
            return
        msg = orjson.loads(rec.payload)
        data = msg.get("data") if isinstance(msg, dict) else None
        if not isinstance(data, dict):
            self.engine.counters["control_frames"] += 1
            return
        kind = data.get("e")
        if kind == "aggTrade":
            self.engine.trade(
                data["s"],
                int(data["T"]) * MS,
                rec.ts,
                str(data["a"]),
                data["p"],
                data["q"],
                -1 if data["m"] else 1,  # m: buyer is maker, so the seller aggressed
            )
        elif kind == "depthUpdate":
            if logical == "public" and self.is_active(rec):
                self._diff(rec, data)
            else:
                self.engine.counters["standby_book_frames"] += 1
        else:
            self.engine.counters["unknown_frames"] += 1

    def _diff(self, rec: RawRecord, data: dict[str, Any]) -> None:
        symbol = data["s"]
        sync = self.sync.get(symbol)
        if sync is None:
            self.engine.counters["unknown_symbol"] += 1
            return
        diff = _Diff(
            rec.ts,
            int(data["T"]) * MS,
            int(data["U"]),
            int(data["u"]),
            int(data["pu"]),
            rec.conn,
            _levels(data.get("b", []), data.get("a", [])),
        )
        if sync.synced and diff.prev != sync.last_u:
            self.engine.open_gap(
                symbol, "book_l2", "seq_gap", rec.ts, seq_from=sync.last_u, seq_to=diff.prev
            )
            self.engine.counters["seq_gaps"] += 1
            self._flush(symbol, sync)
            sync.synced, sync.known = False, False
        if sync.synced and sync.known:
            self._emit(symbol, diff, diff.ts_recv)
            sync.last_u = diff.final
            if self.waiting[symbol]:
                self._check_waiting(symbol)
            return
        if sync.synced:
            sync.last_u = diff.final
        sync.buffer.append(diff)
        if sync.pending is not None and diff.final >= sync.pending.last_id:
            self._bridge(symbol, sync, rec.ts)
        elif len(sync.buffer) > BUFFER_MAX:
            self._flush(symbol, sync, keep=BUFFER_MAX // 2)

    def _flush(self, symbol: str, sync: _Sync, keep: int = 0) -> None:
        """Emit buffered diffs that no snapshot will bridge, in receive order."""
        cut = len(sync.buffer) - keep
        for diff in sync.buffer[:cut]:
            self._emit(symbol, diff, diff.ts_recv)
        del sync.buffer[:cut]

    def _emit(self, symbol: str, diff: _Diff, ts_recv: int) -> None:
        book = self.books[symbol]
        for side, price, qty in diff.levels:
            book.apply(side, price, qty)
        self.recent[symbol].append((diff.final, {(s, float(p)) for s, p, _ in diff.levels}))
        self.engine.book(
            symbol, diff.ts_exchange, ts_recv, diff.final, False, diff.conn, diff.levels
        )

    # ------------------------------------------------------------------ bridging

    def _offer(self, symbol: str, rec: RawRecord, recovery: str) -> None:
        sync = self.sync[symbol]
        sync.pending = _Pending(rec.ts, orjson.loads(rec.payload), recovery)
        if sync.buffer and sync.buffer[-1].final >= sync.pending.last_id:
            self._bridge(symbol, sync, rec.ts)

    def _bridge(self, symbol: str, sync: _Sync, known_at: int) -> None:
        pending = sync.pending
        if pending is None:
            return
        last_id = pending.last_id
        start = next(i for i, d in enumerate(sync.buffer) if d.final >= last_id)
        if sync.buffer[start].first > last_id:
            self.engine.counters["stale_snapshots"] += 1
            sync.pending = None  # older than the buffered stream: wait for the next one
            return
        chain = sync.buffer[start:]
        if any(chain[i].prev != chain[i - 1].final for i in range(1, len(chain))):
            self.engine.counters["bridge_breaks"] += 1
            sync.pending = None
            return
        for diff in sync.buffer[:start]:
            self._emit(symbol, diff, diff.ts_recv)
        self.engine.recovered_by(symbol, "book_l2", pending.recovery)
        body = pending.body
        levels = _levels(body.get("bids", []), body.get("asks", []))
        self.books[symbol].load(
            ((p, q) for side, p, q in levels if side == BID),
            ((p, q) for side, p, q in levels if side == ASK),
        )
        ts_exchange = int(body.get("T", 0)) * MS
        self.engine.book(symbol, ts_exchange, known_at, last_id, True, pending.recovery, levels)
        self.engine.counters[f"bridged_{pending.recovery}"] += 1
        for diff in chain:
            self._emit(symbol, diff, known_at)
        sync.buffer, sync.pending = [], None
        sync.synced, sync.known, sync.last_u = True, True, chain[-1].final

    def book_end(self) -> dict[str, dict[str, Any]]:
        out = {}
        for symbol, sync in self.sync.items():
            book = self.books[symbol]
            out[symbol] = {
                "known": sync.synced and sync.known,
                "last_u": sync.last_u,
                "bids": [list(lv) for lv in book.top(BID, len(book.bids))],
                "asks": [list(lv) for lv in book.top(ASK, len(book.asks))],
            }
        return out
