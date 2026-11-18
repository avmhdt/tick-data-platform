"""Prometheus exporter for the shared capture (FR-18 to FR-21 in shared-capture mode).

This project runs no recorder of its own (one shared crypto capture, docs/decisions.md), so
the "ingestor" Prometheus scrapes is this exporter. At every scrape it reads the shared
recorder's ``status.json`` (one small file, rewritten about every 10 s) and turns it into
``tickdb_`` metrics with bounded labels (venue, symbol, channel). It also:

- probes the host clock over NTP every 60 s (FR-21) and appends each reading to
  ``<data_root>/meta/clock/date=D.jsonl``, which the daily DQ checks use (DQ-08);
- reports free space of the data root, of the guarded host drive (the owner's free-space
  floor, ``/mnt/g``) and of the capture's disk;
- reports the last outcome of this project's jobs: the hourly mirror (``meta/mirror/run.log``)
  and the daily job (``meta/scheduler/job_log.csv``).

Nothing here writes to the shared capture.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

GIB = 1024**3
CHANNELS = {"book": "book_l2", "trades": "trades", "bbo": "bbo"}


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = orjson.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _last_line(path: Path, max_bytes: int = 65536) -> bytes:
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            lines = fh.read().strip().splitlines()
    except OSError:
        return b""
    return lines[-1] if lines else b""


def _iso(ts: str) -> float:
    return datetime.fromisoformat(ts).timestamp()


def mirror_state(data_root: Path) -> tuple[float | None, bool | None]:
    """(unix time of the last run, whether it ended without errors or a stop)."""
    line = _last_line(data_root / "meta" / "mirror" / "run.log")
    try:
        rec = orjson.loads(line) if line else None
    except ValueError:
        return None, None
    if not isinstance(rec, dict) or "utc" not in rec:
        return None, None
    return _iso(rec["utc"]), not rec.get("errors") and not rec.get("stopped")


def daily_state(data_root: Path) -> dict[str, tuple[float, bool, str]]:
    """Per venue: (unix time, ok, date) of its last daily run, from the job log. A run is ok
    when every step of that date ended ok or skipped."""
    path = data_root / "meta" / "scheduler" / "job_log.csv"
    try:
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
    except OSError:
        return {}
    last_date: dict[str, str] = {}
    for r in rows:
        if r.get("job") == "daily":
            last_date[r["venue"]] = r["date"]
    out: dict[str, tuple[float, bool, str]] = {}
    for venue, day in last_date.items():
        steps = [r for r in rows if r.get("venue") == venue and r.get("date") == day]
        final: dict[str, str] = {}
        for r in steps:
            final[r["step"]] = r["outcome"]  # the last attempt of each step wins
        ok = all(o in ("ok", "skipped") for o in final.values())
        out[venue] = (_iso(steps[-1]["timestamp_utc"]), ok, day)
    return out


class ClockProbe:
    """Background NTP probe every ``interval_s``; keeps the last reading."""

    def __init__(
        self,
        server: str,
        data_root: Path | None,
        interval_s: float = 60.0,
        probe: Callable[[str], Any] | None = None,
    ) -> None:
        from tickdb import clock

        self.server = server
        self.data_root = data_root
        self.interval_s = interval_s
        self._probe = probe or clock.probe
        self.offset_s: float | None = None
        self.delay_s: float | None = None
        self.last_ok: float | None = None
        self.failures = 0
        self._stop = threading.Event()

    def once(self) -> None:
        try:
            reading = self._probe(self.server)
        except Exception:
            self.failures += 1
            return
        now = time.time_ns()
        self.offset_s = reading.offset_ms / 1000.0
        self.delay_s = reading.delay_ms / 1000.0
        self.last_ok = now / 1e9
        if self.data_root is not None:
            day = datetime.fromtimestamp(now / 1e9, tz=UTC).date().isoformat()
            path = self.data_root / "meta" / "clock" / f"date={day}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            rec = {"ts_ns": now, "offset_ms": reading.offset_ms, "delay_ms": reading.delay_ms,
                   "server": self.server}  # fmt: skip
            with path.open("ab") as fh:
                fh.write(orjson.dumps(rec) + b"\n")

    def run(self) -> None:  # pragma: no cover - timing loop
        while not self._stop.is_set():
            self.once()
            self._stop.wait(self.interval_s)

    def start(self) -> threading.Thread:  # pragma: no cover - timing loop
        thread = threading.Thread(target=self.run, name="ntp-probe", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()


class SharedCaptureCollector(Collector):
    """Builds the metrics from the files at scrape time (no state kept between scrapes)."""

    def __init__(
        self,
        shared_root: Path,
        data_root: Path,
        clock: ClockProbe | None = None,
        now: Callable[[], float] = time.time,
        guard: Path | None = None,
    ) -> None:
        self.shared_root = shared_root
        self.data_root = data_root
        self.guard = guard
        self.clock = clock
        self.now = now

    def collect(self) -> Iterator[Metric]:
        now = self.now()
        status = _read_json(self.shared_root / "status.json")
        up = GaugeMetricFamily(
            "tickdb_capture_status_up", "1 if the shared recorder's status file was read"
        )
        up.add_metric([], 1.0 if status else 0.0)
        yield up
        if status:
            yield from self._status(status, now)
        yield from self._disk(status)
        yield from self._jobs()
        yield from self._clock()

    def _status(self, s: dict[str, Any], now: float) -> Iterator[Metric]:
        age = now - int(s.get("ts_ns", 0)) / 1e9
        g = GaugeMetricFamily(
            "tickdb_capture_status_age_seconds", "Seconds since the recorder wrote its status"
        )
        g.add_metric([], age)
        yield g
        stopped = GaugeMetricFamily("tickdb_capture_stopped", "1 if the recorder says it stopped")
        stopped.add_metric([], 1.0 if s.get("stopped") else 0.0)
        yield stopped
        fresh = age < 60 and not s.get("stopped")
        connected = GaugeMetricFamily(
            "tickdb_ws_connected", "Open websocket connections (0 while the status is stale)",
            labels=["venue"],
        )  # fmt: skip
        generation = GaugeMetricFamily(
            "tickdb_ws_connection_generation",
            "Connection generation per connection group (changes on every reconnect)",
            labels=["venue", "group"],
        )
        frames = CounterMetricFamily(
            "tickdb_ws_frames", "Websocket frames received", labels=["venue"]
        )
        latency = GaugeMetricFamily(
            "tickdb_feed_latency_p50_seconds",
            "Recorder's running median of ts_recv - ts_exchange",
            labels=["venue"],
        )
        vclock = GaugeMetricFamily(
            "tickdb_venue_clock_offset_seconds",
            "Recorder's offset estimate against the venue's server time",
            labels=["venue"],
        )
        ingest = GaugeMetricFamily(
            "tickdb_ingest_delay_p99_seconds",
            "Recorder's frame receipt to hand-off p99",
            labels=["venue"],
        )
        parse = CounterMetricFamily(
            "tickdb_parse_errors", "Frames the recorder could not parse", labels=["venue"]
        )
        msgs = CounterMetricFamily(
            "tickdb_stream_messages", "Messages per stream", labels=["venue", "symbol", "channel"]
        )
        age_g = GaugeMetricFamily(
            "tickdb_last_frame_age_seconds",
            "Seconds since the stream's last good message",
            labels=["venue", "symbol", "channel"],
        )
        live = GaugeMetricFamily(
            "tickdb_stream_live", "1 if the stream is LIVE", labels=["venue", "symbol", "channel"]
        )
        resyncs = CounterMetricFamily(
            "tickdb_resyncs", "Book resyncs since the recorder started", labels=["venue", "symbol"]
        )
        crossed = CounterMetricFamily(
            "tickdb_crossed_books", "Crossed books seen by the recorder", labels=["venue", "symbol"]
        )
        status_age = max(age, 0.0)
        for venue, block in sorted(s.get("venues", {}).items()):
            conns = block.get("conns", [])
            connected.add_metric([venue], float(len(conns)) if fresh else 0.0)
            for conn in conns:
                m = re.match(r"^(.*)\.g(\d+)$", str(conn))
                if m:
                    generation.add_metric([venue, m.group(1)], float(m.group(2)))
            frames.add_metric([venue], float(block.get("frames", 0)))
            parse.add_metric([venue], float(block.get("parse_errors", 0)))
            if block.get("feed_latency_ms_p50") is not None:
                latency.add_metric([venue], block["feed_latency_ms_p50"] / 1000.0)
            if block.get("clock_offset_ms") is not None:
                vclock.add_metric([venue], block["clock_offset_ms"] / 1000.0)
            if block.get("ingest_us_p99") is not None:
                ingest.add_metric([venue], block["ingest_us_p99"] / 1e6)
            for st in block.get("streams", []):
                channel = CHANNELS.get(st.get("stream", ""), st.get("stream", ""))
                labels = [venue, st.get("symbol", ""), channel]
                msgs.add_metric(labels, float(st.get("msgs", 0)))
                age_g.add_metric(labels, float(st.get("age_s", 0.0)) + status_age)
                live.add_metric(labels, 1.0 if fresh and st.get("state") == "LIVE" else 0.0)
                if channel == "book_l2":
                    resyncs.add_metric(labels[:2], float(st.get("resyncs", 0)))
                    crossed.add_metric(labels[:2], float(st.get("crossed", 0)))
        yield from (connected, generation, frames, parse, latency, vclock, ingest, msgs, age_g,
                    live, resyncs, crossed)  # fmt: skip
        raw = s.get("raw", {})
        writer = s.get("writer", {})
        errs = CounterMetricFamily(
            "tickdb_capture_errors", "Recorder raw and writer errors", labels=["kind"]
        )
        errs.add_metric(["raw_dropped"], float(raw.get("dropped", 0)))
        errs.add_metric(["raw_errors"], float(raw.get("errors", 0)))
        errs.add_metric(["flush_errors"], float(writer.get("flush_errors", 0)))
        yield errs

    def _disk(self, status: dict[str, Any] | None) -> Iterator[Metric]:
        free = GaugeMetricFamily("tickdb_disk_free_bytes", "Free bytes", labels=["path"])
        ratio = GaugeMetricFamily(
            "tickdb_disk_free_ratio", "Free share of the filesystem", labels=["path"]
        )
        for label, path in (("data_root", self.data_root), ("guard", self.guard)):
            if path is None:
                continue
            try:
                du = shutil.disk_usage(path)
            except OSError:
                continue
            free.add_metric([label], float(du.free))
            ratio.add_metric([label], du.free / du.total if du.total else 0.0)
        disk = (status or {}).get("disk", {})
        if disk.get("free_gb") is not None:
            free.add_metric(["capture"], disk["free_gb"] * GIB)
            if disk.get("used_pct") is not None:
                ratio.add_metric(["capture"], 1.0 - disk["used_pct"] / 100.0)
        yield free
        yield ratio

    def _jobs(self) -> Iterator[Metric]:
        last = GaugeMetricFamily(
            "tickdb_job_last_run_timestamp",
            "Unix time of the job's last run",
            labels=["job", "venue"],
        )
        ok = GaugeMetricFamily(
            "tickdb_job_last_run_ok", "1 if the job's last run succeeded", labels=["job", "venue"]
        )
        when, good = mirror_state(self.data_root)
        if when is not None:
            last.add_metric(["mirror", ""], when)
            ok.add_metric(["mirror", ""], 1.0 if good else 0.0)
        for venue, (t, good_day, _) in sorted(daily_state(self.data_root).items()):
            last.add_metric(["daily", venue], t)
            ok.add_metric(["daily", venue], 1.0 if good_day else 0.0)
        yield last
        yield ok

    def _clock(self) -> Iterator[Metric]:
        if self.clock is None:
            return
        g = GaugeMetricFamily("tickdb_clock_offset_seconds", "Host clock offset against NTP")
        d = GaugeMetricFamily("tickdb_clock_probe_delay_seconds", "NTP round trip of the probe")
        if self.clock.offset_s is not None:
            g.add_metric([], self.clock.offset_s)
        if self.clock.delay_s is not None:
            d.add_metric([], self.clock.delay_s)
        yield g
        yield d
        f = CounterMetricFamily("tickdb_clock_probe_failures", "Failed NTP probes")
        f.add_metric([], float(self.clock.failures))
        yield f


def serve(
    shared_root: Path,
    data_root: Path,
    port: int,
    ntp_server: str,
    addr: str = "0.0.0.0",
    guard: Path | None = None,
) -> None:  # pragma: no cover - network loop
    from prometheus_client import CollectorRegistry, start_http_server

    probe = ClockProbe(ntp_server, data_root)
    probe.start()
    registry = CollectorRegistry()
    registry.register(SharedCaptureCollector(shared_root, data_root, probe, guard=guard))
    start_http_server(port, addr=addr, registry=registry)
    while True:
        time.sleep(3600)
