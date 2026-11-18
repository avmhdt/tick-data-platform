"""Prometheus metrics for one ingestor process (FR-18, spec 10). Names are prefixed
``tickdb_`` and labels are bounded: venue, symbol, channel, reason, endpoint, status."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

LATENCY_BUCKETS = (0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0)
FAST_BUCKETS = (0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0)


class Metrics:
    """All ingestor metrics on a private registry (one per process, or per test)."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r = self.registry = registry or CollectorRegistry()
        self.ws_connected = Gauge(
            "tickdb_ws_connected", "Open websocket connections", ["venue"], registry=r
        )
        self.ws_frames = Counter(
            "tickdb_ws_frames", "Websocket frames received", ["venue", "channel"], registry=r
        )
        self.ws_reconnects = Counter(
            "tickdb_ws_reconnects", "Reconnects", ["venue", "reason"], registry=r
        )
        self.resyncs = Counter(
            "tickdb_resyncs", "Book resyncs", ["venue", "symbol", "reason"], registry=r
        )
        self.seq_gaps = Counter(
            "tickdb_seq_gaps", "Sequence gaps", ["venue", "symbol", "channel"], registry=r
        )
        self.checksum_failures = Counter(
            "tickdb_checksum_failures", "Book checksum mismatches", ["venue", "symbol"], registry=r
        )
        self.feed_latency = Histogram(
            "tickdb_feed_latency_seconds",
            "ts_recv - ts_exchange",
            ["venue", "channel"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.last_frame_age = Gauge(
            "tickdb_last_frame_age_seconds",
            "Seconds since the last frame",
            ["venue", "symbol", "channel"],
            registry=r,
        )
        self.spool_queue_depth = Gauge(
            "tickdb_spool_queue_depth", "Spool queue depth", ["venue"], registry=r
        )
        self.spool_bytes = Counter(
            "tickdb_spool_bytes", "Compressed spool bytes written", ["venue"], registry=r
        )
        self.event_loop_lag = Histogram(
            "tickdb_event_loop_lag_seconds",
            "Event-loop lag",
            ["venue"],
            buckets=FAST_BUCKETS,
            registry=r,
        )
        self.ingest_delay = Histogram(
            "tickdb_ingest_delay_seconds",
            "Frame receipt to spool enqueue",
            ["venue"],
            buckets=FAST_BUCKETS,
            registry=r,
        )
        self.clock_offset = Gauge(
            "tickdb_clock_offset_seconds", "Host clock offset against NTP", registry=r
        )
        self.rest_requests = Counter(
            "tickdb_rest_requests", "REST requests", ["venue", "endpoint", "status"], registry=r
        )
        self.disk_free = Gauge(
            "tickdb_disk_free_bytes", "Free bytes on a data path", ["path"], registry=r
        )
        self.job_last_success = Gauge(
            "tickdb_job_last_success_timestamp",
            "Unix time of the last successful job",
            ["job"],
            registry=r,
        )

    def serve(self, port: int, addr: str = "0.0.0.0") -> None:  # pragma: no cover - network
        """Expose /metrics (inside the container; compose never publishes it on the host)."""
        start_http_server(port, addr=addr, registry=self.registry)
