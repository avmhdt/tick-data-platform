"""kill -9 of an ingestor loses at most ~1 s of frames and leaves decodable files (NFR-03)."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import orjson
import pytest
import yaml

from tests.conftest import CONFIG
from tests.integration.ingest_harness import records
from tests.integration.mock_venues import MockBinance
from tickdb.ingest.spool import decodable, recover_partials

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="kill -9 is POSIX")


def _config(tmp_path: Path, port: int) -> Path:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    raw["data_root"] = str(tmp_path / "data")
    raw["capture"]["mode"] = "own"
    venue = raw["venues"]["binance-usdm"]
    venue["ws_url"] = f"ws://127.0.0.1:{port}"
    venue["rest_url"] = f"http://127.0.0.1:{port}"
    venue["book"]["rest_checkpoint_s"] = None
    raw["clickhouse"] = {"host": "localhost", "port": 8123, "database": "tickdb"}
    path = tmp_path / "tickdb.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


async def test_kill9_loses_at_most_one_flush(tmp_path: Path) -> None:
    import asyncio

    mock = await MockBinance(["BTCUSDT", "ETHUSDT", "SOLUSDT"], rate_per_s=400).start()
    try:
        cfg = _config(tmp_path, mock.port)
        env = {**os.environ, "TICKDB_LOG_JSON": "true"}
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tickdb.cli",
                "ingest",
                "run",
                "--venue",
                "binance-usdm",
                "--config",
                str(cfg),
                "--metrics-port",
                "0",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(4.0)
        killed_at = time.time_ns()
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        sent_before = {s: [i for i in ids] for s, ids in mock.sent_trades.items()}
    finally:
        await mock.stop()

    data = tmp_path / "data"
    partials = list((data / "raw" / "spool").rglob("*.partial"))
    assert partials, "a killed writer leaves .partial files"
    recovered = recover_partials(data, "binance-usdm")
    assert len(recovered) == len(partials)
    for path in recovered:
        ok, n = decodable(path)
        assert n > 0, path  # every file has at least its flushed frames
        del ok  # a frame cut mid-write is allowed; read_spool stops cleanly there
    recs = records(data)
    last = max(r["ts_recv"] for r in recs if r["kind"] == "ws")
    assert killed_at - last <= 1_500_000_000  # flush interval 1 s plus scheduling slack
    got = {}
    for r in recs:
        if r["kind"] == "ws" and r["channel"] == "trades":
            d = orjson.loads(r["payload"])["data"]
            got.setdefault(d["s"], set()).add(int(d["a"]))
    for symbol, ids in got.items():
        lo, hi = min(ids), max(ids)
        assert ids == set(range(lo, hi + 1)), symbol  # nothing lost before the last flush
        assert hi <= max(sent_before[symbol])
