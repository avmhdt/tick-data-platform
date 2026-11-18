"""Book state used by the ingestors to validate feeds (FR-03, DQ-04).

Levels keep the venue's price and size strings: Kraken's CRC32 is computed from the text on
the wire, and numbers keep their trailing zeros there. Each side also keeps its prices in a
sorted list, so best price, top-N and truncation are O(1)/O(N) instead of scans of the whole
book (a Coinbase level2 book holds tens of thousands of levels).
"""

from __future__ import annotations

import zlib
from bisect import bisect_left, insort
from collections.abc import Iterable, Sequence

BID, ASK = 1, -1


class _Side:
    __slots__ = ("keys", "levels")

    def __init__(self) -> None:
        self.levels: dict[float, tuple[str, str]] = {}
        self.keys: list[float] = []  # ascending

    def set(self, key: float, level: tuple[str, str]) -> None:
        if key not in self.levels:
            insort(self.keys, key)
        self.levels[key] = level

    def remove(self, key: float) -> None:
        if self.levels.pop(key, None) is not None:
            del self.keys[bisect_left(self.keys, key)]

    def clear(self) -> None:
        self.levels.clear()
        self.keys.clear()


class L2Book:
    """Price -> (price text, size text) per side. Size 0 removes the level."""

    def __init__(self) -> None:
        self._bids = _Side()
        self._asks = _Side()

    @property
    def bids(self) -> dict[float, tuple[str, str]]:
        return self._bids.levels

    @property
    def asks(self) -> dict[float, tuple[str, str]]:
        return self._asks.levels

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()

    def _side(self, side: int) -> _Side:
        return self._bids if side == BID else self._asks

    def apply(self, side: int, price: str, qty: str) -> None:
        key = float(price)
        if float(qty) == 0.0:
            self._side(side).remove(key)  # removing an unknown level is normal
        else:
            self._side(side).set(key, (price, qty))

    def load(self, bids: Iterable[tuple[str, str]], asks: Iterable[tuple[str, str]]) -> None:
        self.clear()
        for p, q in bids:
            self.apply(BID, p, q)
        for p, q in asks:
            self.apply(ASK, p, q)

    def top(self, side: int, n: int) -> list[tuple[str, str]]:
        """Best ``n`` levels as text, best first."""
        s = self._side(side)
        keys = reversed(s.keys[-n:]) if side == BID else s.keys[:n]
        return [s.levels[k] for k in keys]

    def truncate(self, depth: int) -> None:
        """Keep only the best ``depth`` levels per side (Kraken drops out-of-scope levels)."""
        for side in (BID, ASK):
            s = self._side(side)
            extra = len(s.keys) - depth
            if extra <= 0:
                continue
            dropped = s.keys[:extra] if side == BID else s.keys[depth:]
            for key in dropped:
                del s.levels[key]
            s.keys = s.keys[extra:] if side == BID else s.keys[:depth]

    def best(self) -> tuple[float | None, float | None]:
        bid = self._bids.keys[-1] if self._bids.keys else None
        ask = self._asks.keys[0] if self._asks.keys else None
        return bid, ask

    def crossed(self) -> bool:
        bid, ask = self.best()
        return bid is not None and ask is not None and bid >= ask


def _strip(value: str) -> str:
    return value.replace(".", "").lstrip("0")


def kraken_checksum(
    bids: Sequence[tuple[str, str]], asks: Sequence[tuple[str, str]], levels: int = 10
) -> int:
    """Kraken WS v2 book CRC32: top 10 asks (best first) then top 10 bids, each level as
    price text then size text with '.' and leading zeros removed; unsigned CRC32.
    https://docs.kraken.com/api/docs/guides/spot-ws-book-v2
    """
    parts = [_strip(p) + _strip(q) for p, q in asks[:levels]]
    parts += [_strip(p) + _strip(q) for p, q in bids[:levels]]
    return zlib.crc32("".join(parts).encode("ascii"))
