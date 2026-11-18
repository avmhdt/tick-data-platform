"""Resync times of this project's ingestor against mock venues (AC-W-05, labelled "tested").

Each venue's mock runs a session with the faults that trigger a resync: connect, a sequence
gap (binance-usdm), a checksum failure (kraken), a crossed book (coinbase) and a dropped
connection. Writes results/ingestor_resync.json: median and max seconds from detection to
the validated book, per venue. Run: uv run python scripts/ingestor_resync.py
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import tempfile
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.integration.ingest_harness import all_synced, run_with, wait_for
from tests.integration.mock_venues import MOCKS


async def one(venue: str) -> list[float]:
    mock = await MOCKS[venue](rate_per_s=300).start()

    async def scenario(ing, m) -> None:  # type: ignore[no-untyped-def]
        for _ in range(3):
            await wait_for(lambda: all_synced(ing))
            await asyncio.sleep(0.3)
            if venue == "binance-usdm":
                m.skip_seq("BTCUSDT")
            elif venue == "kraken":
                m.bad_checksum_next = "BTC/USD"
            else:
                m.cross_next = "BTC-USD"
            await asyncio.sleep(0.5)
            await wait_for(lambda: all_synced(ing))
            await m.drop_all()
            await asyncio.sleep(0.5)
        await wait_for(lambda: all_synced(ing))

    try:
        with tempfile.TemporaryDirectory() as tmp:
            ing = await run_with(venue, mock, Path(tmp), scenario)
    finally:
        await mock.stop()
    return [t for t in ing.resync_times if t > 0]


async def main() -> None:
    out = {}
    for venue in ("binance-usdm", "coinbase", "kraken"):
        times = await one(venue)
        out[venue] = {
            "source": "tickdb ingestor vs mock venue (tests); not the live shared recorder",
            "resyncs": len(times),
            "median_s": round(statistics.median(times), 3),
            "max_s": round(max(times), 3),
        }
    path = Path("results") / "ingestor_resync.json"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(orjson.dumps(out, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS) + b"\n")
    print(path.read_text())


if __name__ == "__main__":
    asyncio.run(main())
