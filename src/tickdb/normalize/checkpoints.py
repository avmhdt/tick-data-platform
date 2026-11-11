"""Offline comparison of the rebuilt book with REST checkpoints (DQ-04, AC-W-05).

The shared capture spools a REST depth snapshot every 600 s per symbol (control records
``checkpoint``) for binance-usdm and coinbase. During replay each checkpoint is compared with
the book this replay rebuilt, on levels nobody touched around it:

- binance-usdm (sequence-aligned): the book after the diff that straddles ``lastUpdateId``,
  top 100 levels per side, excluding levels touched by that diff and by any later diff
  already applied when the comparison runs (the replay cannot rewind the book);
- coinbase (time-aligned): the book at the response's receive time, top 20 levels per side,
  excluding levels updated within +-500 ms; the check runs once replay passes ``t + 500 ms``.

Counts go to the engine counters: ``ckpt_match``, ``ckpt_mismatch``, ``ckpt_skipped``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tickdb.ingest.book import ASK, BID, L2Book
from tickdb.normalize.engine import Engine

WINDOW_NS = 500_000_000


@dataclass
class _Pending:
    symbol: str
    due: int  # replay time at which to compare
    body: dict[str, Any]
    ts: int
    touched: set[tuple[int, float]] = field(default_factory=set)


def _levels(rows: list[list[Any]], n: int) -> list[tuple[float, float]]:
    return [(float(r[0]), float(r[1])) for r in rows[:n]]


def compare(
    book: L2Book, body: dict[str, Any], depth: int, exclude: set[tuple[int, float]]
) -> bool:
    """True if every untouched level among the checkpoint's top ``depth`` matches the book."""
    for side, key in ((BID, "bids"), (ASK, "asks")):
        ours = {float(p): float(q) for p, q in book.top(side, depth + len(exclude))}
        for price, qty in _levels(body.get(key, []), depth):
            if (side, price) in exclude:
                continue
            if ours.get(price) != qty:
                return False
    return True


class CheckpointChecker:
    """Holds checkpoints until they can be compared; feeds results into ``engine.counters``."""

    def __init__(self, engine: Engine, depth: int) -> None:
        self.engine = engine
        self.depth = depth
        self.pending: list[_Pending] = []

    def add(self, symbol: str, ts: int, body: dict[str, Any], due: int) -> None:
        self.pending.append(_Pending(symbol, due, body, ts))

    def touch(self, symbol: str, ts: int, levels: list[tuple[int, str, str]]) -> None:
        for p in self.pending:
            if p.symbol == symbol and ts >= p.ts - WINDOW_NS:
                p.touched.update((side, float(price)) for side, price, _ in levels)

    def due(self, now: int, books: dict[str, L2Book], known: set[str]) -> None:
        keep = []
        for p in self.pending:
            if now < p.due:
                keep.append(p)
            elif p.symbol not in known:
                self.engine.counters["ckpt_skipped"] += 1
            elif compare(books[p.symbol], p.body, self.depth, p.touched):
                self.engine.counters["ckpt_match"] += 1
            else:
                self.engine.counters["ckpt_mismatch"] += 1
        self.pending = keep
