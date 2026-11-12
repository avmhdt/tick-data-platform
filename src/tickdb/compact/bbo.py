"""BBO ("quotes") derived by replaying book_l2 in archive order (FR-08, DQ-05).

A row is emitted only when one of bid_px, bid_qty, ask_px, ask_qty (or the resync flag)
changes, after a whole batch is applied (book state is defined only between batches). A
snapshot batch replaces the book. For a venue with a subscribed depth (Kraken: 1000) the book is
truncated to that depth after every batch, as the venue's own book is (D-06).
``in_resync`` is true while the book's levels are not known: before the day's first
snapshot batch, and from the start of a book_l2 gap until the next snapshot batch. Crossed
rows (bid >= ask) outside resync are counted for DQ-05.
"""

from __future__ import annotations

from bisect import bisect_left, insort
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tickdb import schemas

BOOK_COLUMNS = ["ts_exchange", "ts_recv", "seq", "side", "price", "qty", "is_snapshot", "batch_id"]


class _Side:
    __slots__ = ("keys", "qty")

    def __init__(self) -> None:
        self.qty: dict[float, float] = {}
        self.keys: list[float] = []

    def set(self, price: float, qty: float) -> None:
        if qty == 0.0:
            if self.qty.pop(price, None) is not None:
                del self.keys[bisect_left(self.keys, price)]
        else:
            if price not in self.qty:
                insort(self.keys, price)
            self.qty[price] = qty

    def clear(self) -> None:
        self.qty.clear()
        self.keys.clear()

    def truncate(self, depth: int, best_high: bool) -> None:
        """Keep the best ``depth`` levels (venues like Kraken send no delete for levels that
        leave the subscribed scope, so the replay must drop them as the venue does)."""
        extra = len(self.keys) - depth
        if extra <= 0:
            return
        dropped = self.keys[:extra] if best_high else self.keys[depth:]
        for price in dropped:
            del self.qty[price]
        self.keys = self.keys[extra:] if best_high else self.keys[:depth]


@dataclass
class BboStats:
    """Counters of one derivation, filled while iterating ``derive_bbo``."""

    crossed: int = 0  # crossed rows outside resync (DQ-05: must be 0)
    rows: int = 0
    resync_rows: int = 0


def _book_batches(path: Path) -> Iterator[pa.RecordBatch]:
    for file in sorted(path.glob("part-*.parquet")):
        yield from pq.ParquetFile(file).iter_batches(batch_size=1_000_000, columns=BOOK_COLUMNS)


def derive_bbo(
    book_dir: Path,
    venue: str,
    symbol: str,
    gap_starts: list[int],
    stats: BboStats | None = None,
    chunk: int = 500_000,
    depth: int | None = None,
) -> Iterator[pa.RecordBatch]:
    """Yield BBO record batches for one book_l2 partition. ``gap_starts``: book gap start
    times (``start_recv``) of this symbol, ascending."""
    bids, asks = _Side(), _Side()
    known = False
    gaps = iter(sorted(gap_starts))
    next_gap = next(gaps, None)
    last: tuple[float | None, ...] = (None, None, None, None, None)
    out: dict[str, list[object]] = {name: [] for name in schemas.BBO.names}
    cur_batch = None
    b_ts = b_recv = b_seq = 0
    stats = stats if stats is not None else BboStats()

    def close_batch() -> None:
        nonlocal last
        if depth is not None:
            bids.truncate(depth, best_high=True)
            asks.truncate(depth, best_high=False)
        bid_px = bids.keys[-1] if bids.keys else None
        ask_px = asks.keys[0] if asks.keys else None
        top = (
            bid_px,
            bids.qty[bid_px] if bid_px is not None else None,
            ask_px,
            asks.qty[ask_px] if ask_px is not None else None,
            not known,
        )
        if top == last:
            return
        last = top
        out["ts"].append(b_ts)
        out["venue"].append(venue)
        out["symbol"].append(symbol)
        out["bid_px"].append(top[0])
        out["bid_qty"].append(top[1])
        out["ask_px"].append(top[2])
        out["ask_qty"].append(top[3])
        out["ts_recv"].append(b_recv)
        out["seq"].append(b_seq)
        out["in_resync"].append(not known)
        stats.rows += 1
        stats.resync_rows += not known
        if known and bid_px is not None and ask_px is not None and bid_px >= ask_px:
            stats.crossed += 1

    def take() -> pa.RecordBatch:
        batch = pa.RecordBatch.from_arrays(
            [pa.array(out[f.name], type=f.type) for f in schemas.BBO], schema=schemas.BBO
        )
        for values in out.values():
            values.clear()
        return batch

    for rb in _book_batches(book_dir):
        cols = [rb.column(i).to_pylist() for i in range(rb.num_columns)]
        for ts_ex, ts_recv, seq, side, price, qty, is_snap, batch_id in zip(*cols, strict=True):
            if batch_id != cur_batch:
                if cur_batch is not None:
                    close_batch()
                    if len(out["ts"]) >= chunk:
                        yield take()
                cur_batch = batch_id
                b_ts, b_recv, b_seq = ts_ex, ts_recv, seq
                while next_gap is not None and ts_recv > next_gap:
                    known = False  # a gap began before this batch: levels are unknown again
                    next_gap = next(gaps, None)
                if is_snap:
                    bids.clear()
                    asks.clear()
                    known = True
            (bids if side == 1 else asks).set(price, qty)
    if cur_batch is not None:
        close_batch()
    if out["ts"]:
        yield take()
