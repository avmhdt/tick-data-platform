"""Throughput headroom (NFR-01, NFR-02, AC-W-06).

Each venue's mock runs in its own process at ``factor`` x the peak 1-minute message rate
measured on this PC's shared capture for the streams this project subscribes to (2026-11-10,
from the raw archive: binance-usdm aggTrade + depth 1,016/s on 2026-11-04 15h, kraken book +
trade 1,559/s on 11-05 15h, coinbase 354/s on 11-08 21h). The ingestor must keep up: every
frame spooled, the queue drained, event-loop lag p99 < 50 ms and ingest delay p99 < 5 ms.

``TICKDB_STRESS_FACTOR`` sets the factor: 5 for the published local run, 1 in CI (shared
runners are not the reference hardware).
"""

from __future__ import annotations

import asyncio
import os
import random
import statistics
import subprocess
import sys
from pathlib import Path

import orjson
import pytest

from tests.conftest import REPO
from tests.integration.ingest_harness import all_synced, mock_config, records, wait_for
from tickdb.ingest.metrics import Metrics
from tickdb.ingest.spool import SpoolWriter
from tickdb.ingest.venues.registry import INGESTORS

PEAK = {"binance-usdm": 1016.0, "kraken": 1559.0, "coinbase": 354.0}
LEVELS = {"binance-usdm": 20, "kraken": 1000, "coinbase": 5000}
FACTOR = float(os.environ.get("TICKDB_STRESS_FACTOR", "1"))
SECONDS = float(os.environ.get("TICKDB_STRESS_SECONDS", "8"))


class _Remote:
    def __init__(self, port: int) -> None:
        self.port = port

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


@pytest.mark.parametrize("venue", ["binance-usdm", "kraken", "coinbase"])
async def test_ingestor_keeps_up(venue: str, tmp_path: Path) -> None:
    rate = PEAK[venue] * FACTOR
    script = REPO / "tests" / "integration" / "mock_venues.py"
    proc = subprocess.Popen(
        [sys.executable, str(script), venue, str(rate), str(SECONDS + 6), str(LEVELS[venue])],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        port = int(proc.stdout.readline())
        tcfg, vcfg = mock_config(venue, _Remote(port), tmp_path, depth=1000)  # type: ignore[arg-type]
        depth_samples: list[int] = []
        spool = SpoolWriter(tmp_path, venue, on_depth=depth_samples.append)
        metrics = Metrics()
        ingestor = INGESTORS[venue](vcfg, tcfg, spool, metrics, rng=random.Random(1))

        async def scenario() -> None:
            await wait_for(lambda: all_synced(ingestor), limit_s=20)
            ingestor.loop_lags.clear()
            await asyncio.sleep(SECONDS)

        await ingestor.run(until=scenario())
        stats = proc.communicate(timeout=30)[0].strip().splitlines()
        frames_sent = orjson.loads(stats[-1])["frames_sent"]
    finally:
        proc.kill()

    recs = records(tmp_path)
    ws = [r for r in recs if r["kind"] == "ws"]
    lags = list(ingestor.loop_lags)
    delay = metrics.ingest_delay.labels(venue)
    buckets = {
        s.labels["le"]: s.value for s in delay.collect()[0].samples if s.name.endswith("_bucket")
    }
    count = buckets["+Inf"]
    under_5ms = buckets["0.005"]
    report = {
        "venue": venue,
        "factor": FACTOR,
        "target_rate": rate,
        "achieved_rate": len(ws) / SECONDS,
        "frames_sent": frames_sent,
        "frames_spooled": len(ws),
        "loop_lag_p99_ms": round(_percentile(lags, 0.99) * 1000, 2),
        "loop_lag_median_ms": round(statistics.median(lags) * 1000, 3),
        "ingest_delay_share_under_5ms": round(under_5ms / count, 5),
        "max_queue_depth": max(depth_samples or [0]),
    }
    print(orjson.dumps(report).decode())
    out = os.environ.get("TICKDB_STRESS_OUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(orjson.dumps(report).decode() + "\n")
    assert len(ws) >= frames_sent * 0.98  # all but frames still in flight at the stop
    assert report["loop_lag_p99_ms"] < 50.0
    assert report["ingest_delay_share_under_5ms"] >= 0.99  # NFR-02: p99 < 5 ms
    assert depth_samples[-1] == 0 if depth_samples else True
