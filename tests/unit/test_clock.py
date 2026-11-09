from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from tickdb import clock


class FakeNTP:
    def request(self, server: str, version: int, timeout: float) -> Any:
        assert version == 3
        return SimpleNamespace(offset=-0.026, delay=0.012, stratum=2)


def test_probe_converts_to_ms() -> None:
    reading = clock.probe("ntp.example", client=FakeNTP())
    assert reading.server == "ntp.example"
    assert round(reading.offset_ms, 6) == -26.0
    assert round(reading.delay_ms, 6) == 12.0
    assert reading.stratum == 2
    assert reading.within(50)
    assert not reading.within(20)
