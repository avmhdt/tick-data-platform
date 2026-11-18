"""Disk-usage projection from measured daily rates (FR-22, ``tickdb disk project``).

Each store is measured per UTC date from its ``date=YYYY-MM-DD`` partitions. A store with a
retention limit levels off at ``rate x retention``; the others grow ``rate x horizon``.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DATE_DIR = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")


@dataclass(frozen=True)
class Store:
    name: str
    path: Path  # relative to the data root
    retention_days: int | None  # None = kept indefinitely


@dataclass(frozen=True)
class StoreRate:
    store: Store
    days_measured: int
    bytes_per_day: float
    bytes_now: int

    def projected(self, horizon_days: int) -> float:
        days = horizon_days
        if self.store.retention_days is not None:
            days = min(days, self.store.retention_days)
        return self.bytes_per_day * days


def per_day_bytes(root: Path) -> dict[date, int]:
    """Bytes under every ``date=`` directory below ``root``, summed per date."""
    totals: dict[date, int] = defaultdict(int)
    if not root.exists():
        return {}
    for dirpath, dirnames, filenames in os.walk(root):
        match = DATE_DIR.match(Path(dirpath).name)
        if match is None:
            continue
        day = date.fromisoformat(match.group(1))
        for sub, _, files in os.walk(dirpath):
            for name in files:
                totals[day] += (Path(sub) / name).stat().st_size
        dirnames.clear()  # already counted below this date directory
        del filenames
    return dict(totals)


def measure(data_root: Path, store: Store, today: date, window: int = 7) -> StoreRate:
    """Mean bytes/day over the last ``window`` complete days before ``today``."""
    days = per_day_bytes(data_root / store.path)
    complete = sorted(d for d in days if d < today)[-window:]
    rate = sum(days[d] for d in complete) / len(complete) if complete else 0.0
    return StoreRate(store, len(complete), rate, sum(days.values()))


def render(rates: list[StoreRate], horizons: list[int], free_bytes: int | None, floor: int) -> str:
    gb = 1e9
    head = f"{'store':<22}{'days':>5}{'GB/day':>9}{'now GB':>9}" + "".join(
        f"{f'{h} d GB':>10}" for h in horizons
    )
    lines = [head]
    for r in rates:
        lines.append(
            f"{r.store.name:<22}{r.days_measured:>5}{r.bytes_per_day / gb:>9.2f}"
            f"{r.bytes_now / gb:>9.1f}" + "".join(f"{r.projected(h) / gb:>10.1f}" for h in horizons)
        )
    totals = [sum(r.projected(h) for r in rates) for h in horizons]
    lines.append(f"{'total':<45}" + "".join(f"{t / gb:>10.1f}" for t in totals))
    if free_bytes is not None:
        lines.append(f"free now: {free_bytes / gb:.1f} GB; floor {floor / gb:.0f} GB")
        for h, t in zip(horizons, totals, strict=True):
            now = sum(r.bytes_now for r in rates)
            left = free_bytes - max(0.0, t - now)
            verdict = "ok" if left >= floor else "BELOW FLOOR"
            lines.append(f"  after {h} d: about {left / gb:.0f} GB free -> {verdict}")
    return "\n".join(lines)
