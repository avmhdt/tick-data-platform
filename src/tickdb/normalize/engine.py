"""Deterministic replay of one venue-day of raw records into canonical rows (FR-07).

The venue normalizers (``tickdb.normalize.<venue>``) are pure state machines fed records in
replay order; this module holds what they share: columnar row builders, trade dedupe
(DQ-03), batch ids, gap bookkeeping and counters. Rows come out already in archive order
(DQ-01: ``ts_recv``, then ``batch_id``/``level_idx`` or trade order), so no global sort is
needed and a day of hundreds of millions of book rows streams through bounded memory.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pyarrow as pa

from tickdb import schemas
from tickdb.normalize.rawlog import RawRecord
from tickdb.timeutil import day_bounds

Sink = Callable[[str, str, pa.RecordBatch], None]  # (dataset, symbol, batch)


class Builder:
    """Column lists for one (dataset, symbol), turned into Arrow batches."""

    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema
        self.cols: dict[str, list[Any]] = {name: [] for name in schema.names}

    def __len__(self) -> int:
        return len(self.cols[self.schema.names[0]])

    def take(self) -> pa.RecordBatch:
        batch = pa.RecordBatch.from_arrays(
            [pa.array(self.cols[f.name], type=f.type) for f in self.schema], schema=self.schema
        )
        for values in self.cols.values():
            values.clear()
        return batch


@dataclass
class Gap:
    venue: str
    symbol: str
    channel: str
    kind: str
    start_recv: int
    end_recv: int | None = None
    seq_from: int | None = None
    seq_to: int | None = None
    recovery: str = "none"
    status: str = "open"

    def gap_id(self) -> str:
        key = f"{self.venue}|{self.symbol}|{self.channel}|{self.start_recv}|{self.kind}"
        return hashlib.sha256(key.encode()).hexdigest()[:32]

    def row(self) -> dict[str, Any]:
        return {
            "gap_id": self.gap_id(),
            "venue": self.venue,
            "symbol": self.symbol,
            "channel": self.channel,
            "kind": self.kind,
            "start_recv": self.start_recv,
            "end_recv": self.end_recv if self.end_recv is not None else self.start_recv,
            "start_exchange": None,
            "end_exchange": None,
            "seq_from": self.seq_from,
            "seq_to": self.seq_to,
            "recovery": self.recovery,
            "status": self.status,
            "recovered_rows": 0,
        }


@dataclass
class Engine:
    """Shared output side of a venue-day replay."""

    venue: str
    day: date
    symbols: tuple[str, ...]
    sink: Sink
    flush_rows: int = 500_000
    carry: dict[str, dict[str, Any]] = field(default_factory=dict)  # previous day's book end
    counters: Counter[str] = field(default_factory=Counter)
    gaps: list[Gap] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.start_ns, self.end_ns = day_bounds(self.day)
        self._trades = {s: Builder(schemas.TRADES) for s in self.symbols}
        self._book = {s: Builder(schemas.BOOK_L2) for s in self.symbols}
        self._seen: dict[str, set[str]] = {s: set() for s in self.symbols}
        self._batch_id = dict.fromkeys(self.symbols, 0)
        self._open: dict[tuple[str, str], Gap] = {}
        self.last_good: dict[tuple[str, str], int] = {}

    # ------------------------------------------------------------------ rows

    def trade(
        self,
        symbol: str,
        ts_exchange: int,
        ts_recv: int,
        trade_id: str,
        price: str,
        qty: str,
        side: int,
    ) -> None:
        if symbol not in self._seen:
            self.counters["unknown_symbol"] += 1
            return
        seen = self._seen[symbol]
        if trade_id in seen:
            self.counters["dup_trades_removed"] += 1  # same trade on two connections
            return
        seen.add(trade_id)
        b = self._trades[symbol].cols
        b["ts_exchange"].append(ts_exchange)
        b["ts_recv"].append(ts_recv)
        b["venue"].append(self.venue)
        b["symbol"].append(symbol)
        b["trade_id"].append(trade_id)
        b["price"].append(float(price))
        b["qty"].append(float(qty))
        b["side"].append(side)
        b["source"].append("ws")
        b["ts_fetch"].append(None)
        self.good(symbol, "trades", ts_recv)
        if len(self._trades[symbol]) >= self.flush_rows:
            self.sink("trades", symbol, self._trades[symbol].take())

    def book(
        self,
        symbol: str,
        ts_exchange: int,
        ts_recv: int,
        seq: int,
        is_snapshot: bool,
        conn: str,
        levels: list[tuple[int, str, str]],
    ) -> None:
        """One venue message as one batch; ``levels`` = (side, price text, size text)."""
        batch_id = self._batch_id[symbol]
        self._batch_id[symbol] = batch_id + 1
        b = self._book[symbol].cols
        for i, (side, price, qty) in enumerate(levels):
            b["ts_exchange"].append(ts_exchange)
            b["ts_recv"].append(ts_recv)
            b["venue"].append(self.venue)
            b["symbol"].append(symbol)
            b["seq"].append(seq)
            b["side"].append(side)
            b["price"].append(float(price))
            b["qty"].append(float(qty))
            b["is_snapshot"].append(is_snapshot)
            b["batch_id"].append(batch_id)
            b["conn_id"].append(conn)
            b["level_idx"].append(i)
        self.counters["book_batches"] += 1
        if is_snapshot or not self.is_open(symbol, "book_l2"):
            self.good(symbol, "book_l2", ts_recv)  # a book gap ends only with a snapshot
        if len(self._book[symbol]) >= self.flush_rows:
            self.sink("book_l2", symbol, self._book[symbol].take())

    # ------------------------------------------------------------------ gaps

    def good(self, symbol: str, channel: str, ts_recv: int) -> None:
        self.last_good[(symbol, channel)] = ts_recv
        gap = self._open.pop((symbol, channel), None)
        if gap is not None:
            gap.end_recv = ts_recv
            if channel == "book_l2":
                # A snapshot restores the book; the L2 history inside the gap is unrecoverable
                # on free sources, so the best a book gap can be is "partial".
                gap.status = "partial" if gap.recovery == "snapshot" else "unrecoverable"
            else:
                gap.status = "open"  # trades: classified by REST backfill (FR-05)
            self.gaps.append(gap)

    def recovered_by(self, symbol: str, channel: str, recovery: str) -> None:
        gap = self._open.get((symbol, channel))
        if gap is not None:
            gap.recovery = recovery

    def open_gap(
        self,
        symbol: str,
        channel: str,
        kind: str,
        ts_recv: int,
        *,
        recovery: str = "none",
        seq_from: int | None = None,
        seq_to: int | None = None,
    ) -> None:
        """Open a gap starting at the stream's last good receive time (never hidden)."""
        if (symbol, channel) in self._open:
            return
        start = self.last_good.get((symbol, channel), self.start_ns)
        self._open[(symbol, channel)] = Gap(
            self.venue, symbol, channel, kind, min(start, ts_recv), None, seq_from, seq_to, recovery
        )
        self.counters[f"gap_{kind}"] += 1

    def is_open(self, symbol: str, channel: str) -> bool:
        return (symbol, channel) in self._open

    # ------------------------------------------------------------------ end

    def finish(self) -> None:
        for symbol in self.symbols:
            if len(self._trades[symbol]):
                self.sink("trades", symbol, self._trades[symbol].take())
            if len(self._book[symbol]):
                self.sink("book_l2", symbol, self._book[symbol].take())
        for gap in self._open.values():  # still open at the end of the day
            gap.end_recv = self.end_ns
            gap.status = "open"
            self.gaps.append(gap)
        self._open.clear()
        self.gaps.sort(key=lambda g: (g.symbol, g.channel, g.start_recv, g.kind))


class VenueNormalizer:
    """Base of the per-venue state machines."""

    venue: str = ""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.active: dict[str, str] = {}  # logical connection -> active conn id

    def feed(self, rec: RawRecord) -> None:
        if rec.ctrl is not None:
            self.on_control(rec)
        else:
            self.on_frame(rec)

    def on_control(self, rec: RawRecord) -> None:
        ctrl = rec.ctrl or {}
        kind = ctrl.get("k")
        if kind == "state":
            self.seed(ctrl.get("state", {}), rec.ts)
        elif kind == "conn_open":
            if not ctrl.get("standby"):
                self.active[str(ctrl["logical"])] = str(ctrl["conn"])
                self.on_connected(str(ctrl["logical"]), rec.ts)
        elif kind == "promote":
            self.active[str(ctrl["logical"])] = str(ctrl["conn"])
            self.engine.counters["promotions"] += 1
        elif kind == "conn_close":
            conn = str(ctrl["conn"])
            for logical, active in list(self.active.items()):
                if active == conn:
                    del self.active[logical]
                    self.on_disconnected(logical, str(ctrl.get("reason", "")), rec.ts)
        elif kind == "stale" and "symbol" in ctrl:
            self.on_stale(str(ctrl["symbol"]), rec.ts)
        elif kind == "snapshot" and "symbol" in ctrl:
            self.on_rest_snapshot(str(ctrl["symbol"]), rec)
        elif kind == "checkpoint":
            self.engine.counters["checkpoints"] += 1

    def is_active(self, rec: RawRecord) -> bool:
        return rec.conn in self.active.values()

    # hooks
    def seed(self, state: dict[str, Any], ts: int) -> None:
        self.active.update({str(k): str(v) for k, v in state.get("active", {}).items()})

    def on_connected(self, logical: str, ts: int) -> None: ...

    def on_disconnected(self, logical: str, reason: str, ts: int) -> None: ...

    def on_stale(self, symbol: str, ts: int) -> None: ...

    def on_rest_snapshot(self, symbol: str, rec: RawRecord) -> None: ...

    def on_frame(self, rec: RawRecord) -> None:
        raise NotImplementedError

    def book_end(self) -> dict[str, dict[str, Any]]:
        """End-of-day book per symbol, the next day's ``carry``."""
        return {}
