"""The shared-capture exporter (FR-18 to FR-21), the DQ site (FR-26) and the dashboards (FR-19)."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import orjson
import pyarrow as pa
import pytest
from prometheus_client import CollectorRegistry, generate_latest

from tickdb import schemas
from tickdb.dq import site
from tickdb.monitor import exporter

NOW = 1_795_040_124.0


def status(age_s: float = 2.0, stopped: bool = False) -> dict[str, object]:
    return {
        "ts_ns": int((NOW - age_s) * 1e9),
        "stopped": stopped,
        "disk": {"free_gb": 500.0, "used_pct": 40.0},
        "writer": {"flush_errors": 0},
        "raw": {"dropped": 3, "errors": 0},
        "venues": {
            "kraken": {
                "conns": ["ws.g3"],
                "frames": 1000,
                "parse_errors": 0,
                "feed_latency_ms_p50": 107.9,
                "clock_offset_ms": -5.78,
                "ingest_us_p99": 195.6,
                "streams": [
                    {
                        "symbol": "BTC/USD",
                        "stream": "book",
                        "state": "LIVE",
                        "age_s": 0.5,
                        "msgs": 900,
                        "resyncs": 2,
                        "crossed": 0,
                    },
                    {
                        "symbol": "BTC/USD",
                        "stream": "trades",
                        "state": "LIVE",
                        "age_s": 30.0,
                        "msgs": 100,
                    },
                ],
            }
        },
    }


@pytest.fixture
def roots(tmp_path: Path) -> tuple[Path, Path]:
    shared, data = tmp_path / "shared", tmp_path / "data"
    shared.mkdir()
    (data / "meta" / "mirror").mkdir(parents=True)
    (data / "meta" / "scheduler").mkdir(parents=True)
    (shared / "status.json").write_bytes(orjson.dumps(status()))
    (data / "meta" / "mirror" / "run.log").write_text(
        '{"errors": [], "stopped": "", "utc": "2026-11-18T22:25:15+00:00"}\n', encoding="utf-8"
    )
    (data / "meta" / "scheduler" / "job_log.csv").write_text(
        "timestamp_utc,job,date,venue,step,attempt,outcome,seconds,detail\n"
        "2026-11-18T02:00:00+00:00,daily,2026-11-17,kraken,compact,1,failed,1,x\n"
        "2026-11-18T02:05:00+00:00,daily,2026-11-17,kraken,compact,2,ok,1,x\n"
        "2026-11-18T02:10:00+00:00,daily,2026-11-17,kraken,load,1,skipped,1,x\n"
        "2026-11-18T03:00:00+00:00,daily,2026-11-17,coinbase,compact,3,failed,1,x\n",
        encoding="utf-8",
    )
    return shared, data


def scrape(shared: Path, data: Path, **kw: object) -> dict[str, float]:
    reg = CollectorRegistry()
    reg.register(exporter.SharedCaptureCollector(shared, data, now=lambda: NOW, **kw))  # type: ignore[arg-type]
    out = {}
    for line in generate_latest(reg).decode().splitlines():
        if line and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            out[name] = float(value)
    return out


def test_status_becomes_bounded_metrics(roots: tuple[Path, Path]) -> None:
    m = scrape(*roots, guard=roots[1])
    assert m["tickdb_capture_status_up"] == 1.0
    assert m["tickdb_capture_status_age_seconds"] == pytest.approx(2.0, abs=1e-3)
    assert m['tickdb_ws_connected{venue="kraken"}'] == 1.0
    assert m['tickdb_ws_connection_generation{group="ws",venue="kraken"}'] == 3.0
    assert m['tickdb_ws_frames_total{venue="kraken"}'] == 1000.0
    assert m['tickdb_feed_latency_p50_seconds{venue="kraken"}'] == pytest.approx(0.1079)
    book = 'channel="book_l2",symbol="BTC/USD",venue="kraken"'
    assert m[f"tickdb_last_frame_age_seconds{{{book}}}"] == pytest.approx(2.5, abs=1e-3)
    assert m[f"tickdb_stream_live{{{book}}}"] == 1.0
    assert m['tickdb_resyncs_total{symbol="BTC/USD",venue="kraken"}'] == 2.0
    assert m['tickdb_capture_errors_total{kind="raw_dropped"}'] == 3.0
    assert m['tickdb_disk_free_ratio{path="capture"}'] == pytest.approx(0.6)
    assert 'tickdb_disk_free_bytes{path="guard"}' in m
    assert m['tickdb_job_last_run_ok{job="mirror",venue=""}'] == 1.0
    assert m['tickdb_job_last_run_ok{job="daily",venue="kraken"}'] == 1.0  # retry succeeded
    assert m['tickdb_job_last_run_ok{job="daily",venue="coinbase"}'] == 0.0
    labels = {k.split("{")[1] for k in m if "{" in k}
    assert all(set(re_keys(lbl)) <= {"venue", "symbol", "channel", "group", "kind", "path",
                                     "job"} for lbl in labels)  # fmt: skip


def re_keys(labels: str) -> list[str]:
    return [part.split("=")[0] for part in labels.rstrip("}").split(",")]


def test_stale_or_missing_status(roots: tuple[Path, Path]) -> None:
    shared, data = roots
    (shared / "status.json").write_bytes(orjson.dumps(status(age_s=300)))
    m = scrape(shared, data)
    assert m['tickdb_ws_connected{venue="kraken"}'] == 0.0
    assert m['tickdb_stream_live{channel="book_l2",symbol="BTC/USD",venue="kraken"}'] == 0.0
    (shared / "status.json").unlink()
    m = scrape(shared, data)
    assert m["tickdb_capture_status_up"] == 0.0
    assert "tickdb_capture_status_age_seconds" not in m


@dataclass
class Reading:
    offset_ms: float
    delay_ms: float


def test_clock_probe_writes_readings(tmp_path: Path, roots: tuple[Path, Path]) -> None:
    probe = exporter.ClockProbe("ntp.test", tmp_path, probe=lambda _s: Reading(12.5, 30.0))
    probe.once()
    files = list((tmp_path / "meta" / "clock").glob("date=*.jsonl"))
    assert len(files) == 1
    rec = orjson.loads(files[0].read_bytes().splitlines()[0])
    assert rec["offset_ms"] == 12.5
    assert abs(rec["ts_ns"] / 1e9 - time.time()) < 60
    m = scrape(*roots, clock=probe)
    assert m["tickdb_clock_offset_seconds"] == pytest.approx(0.0125)

    def boom(_s: str) -> Reading:
        raise OSError("timeout")

    bad = exporter.ClockProbe("ntp.test", None, probe=boom)
    bad.once()
    assert bad.failures == 1
    assert bad.offset_s is None


def _dq_row(day: date, symbol: str, cov: float, status: str) -> dict[str, object]:
    return {
        "date": day, "venue": "kraken", "symbol": symbol, "dataset": "trades", "rows": 10,
        "dup_rows_removed": 0, "outliers": 0, "crossed_bbo": 0, "gap_count": 1,
        "gap_seconds_total": 5.0, "gap_seconds_unrecovered": 0.0, "coverage_pct": cov,
        "latency_p50_ms": 100.0, "latency_p99_ms": 400.0, "latency_max_ms": 900.0,
        "clock_offset_max_ms": 3.0, "status": status, "content_hash": "ab",
    }  # fmt: skip


def test_site_is_aggregates_only(tmp_path: Path) -> None:
    data = tmp_path / "data"
    days = [date(2026, 11, 17), date(2026, 11, 19)]
    for day, cov, st in ((days[0], 99.5, "pass"), (days[1], 96.0, "warn")):
        d = data / "meta" / "dq_daily" / f"date={day.isoformat()}"
        d.mkdir(parents=True)
        tbl = pa.Table.from_pylist([_dq_row(day, "BTC/USD", cov, st)], schema=schemas.DQ_DAILY)
        import pyarrow.parquet as pq

        pq.write_table(tbl, d / "venue=kraken.parquet")
    summary = site.build(data, tmp_path / "site")
    assert summary["days"] == 2
    assert summary["rows_by_status"] == {"pass": 1, "warn": 1}
    page = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    assert "kraken BTC/USD trades" in page
    assert page.count("class='cell'") == 3  # 11-17, 11-18 (no data), 11-19
    assert "no data" in page
    header = (tmp_path / "site" / "dq_daily.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "content_hash" not in header
    assert not {"price", "qty", "trade_id"} & set(header.split(","))
    assert json.loads((tmp_path / "site" / "summary.json").read_text())["streams"] == 1


def test_dashboards_are_generated_and_current() -> None:
    root = Path(__file__).resolve().parents[2]
    done = subprocess.run(
        [sys.executable, str(root / "scripts" / "gen_dashboards.py"), "--check"],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert done.returncode == 0, done.stdout
    for name in ("feed_health.json", "data_quality.json"):
        dash = json.loads((root / "docker" / "grafana" / "dashboards" / name).read_text())
        ids = [p["id"] for p in dash["panels"]]
        assert len(ids) == len(set(ids))
        for p in dash["panels"]:
            pos = p["gridPos"]
            assert pos["x"] + pos["w"] <= 24
            for t in p["targets"]:
                assert t["datasource"]["uid"] in ("prometheus", "clickhouse")
                assert "rawSql" in t or "expr" in t
