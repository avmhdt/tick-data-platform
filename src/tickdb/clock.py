"""Host clock-offset probe over NTP (FR-21, P-08)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import ntplib


@dataclass(frozen=True)
class ClockReading:
    """One NTP probe. ``offset_ms`` > 0 means the host clock is slow."""

    server: str
    offset_ms: float
    delay_ms: float
    stratum: int

    def within(self, limit_ms: float) -> bool:
        return abs(self.offset_ms) < limit_ms


def probe(server: str, timeout_s: float = 5.0, client: Any = None) -> ClockReading:
    """Query ``server`` once and return the measured offset and round-trip delay."""
    response = (client or ntplib.NTPClient()).request(server, version=3, timeout=timeout_s)
    return ClockReading(
        server=server,
        offset_ms=float(response.offset) * 1000.0,
        delay_ms=float(response.delay) * 1000.0,
        stratum=int(response.stratum),
    )
