"""AC-W-02: captured Binance USD-M BTCUSDT aggregate trade ids equal data.binance.vision's
daily aggTrades file on at least 3 sampled days. Needs the archive under DATA_ROOT and
downloads one ~50 MB file per day (8 MB/s cap): ``pytest -m data tests/live/test_binance_vision.py``.
Writes results/binance_vision_check.json (counts only)."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pytest

from tickdb.config import Settings, load_config
from tickdb.dq import vision

pytestmark = pytest.mark.data
DAYS = [
    d
    for d in os.environ.get("TICKDB_VISION_DAYS", "2026-11-08,2026-11-09,2026-11-10").split(",")
    if d
]


def test_aggtrade_ids_equal_binance_vision() -> None:
    cfg = load_config(None, Settings())
    root = Path(cfg.data_root).expanduser()
    results = [vision.check(root, date.fromisoformat(d)) for d in DAYS]
    out = Path("results") / "binance_vision_check.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps([r.as_dict() for r in results], indent=2) + "\n", encoding="utf-8")
    independent = [r for r in results if not r.circular and r.next_day_archived]
    assert len(independent) >= 3, [r.as_dict() for r in results]
    assert all(r.equal for r in independent), [r.as_dict() for r in independent]
