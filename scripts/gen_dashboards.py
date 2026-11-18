"""Generate the Grafana dashboards (FR-19) into docker/grafana/dashboards/.

    uv run python scripts/gen_dashboards.py          # write
    uv run python scripts/gen_dashboards.py --check  # exit 1 if the committed JSON differs

"Feed health" reads Prometheus (the tickdb exporter) plus the hourly latency table in
ClickHouse; "Data quality" reads the DQ tables in ClickHouse through the read-only user.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "docker" / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
V = 'venue=~"$venue"'
CH_VENUE = "venue IN (${venue:singlequote})"


class Layout:
    def __init__(self) -> None:
        self.x = 0
        self.y = 0
        self.row_h = 0
        self.next_id = 1

    def place(self, w: int, h: int) -> dict[str, int]:
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def id(self) -> int:
        self.next_id += 1
        return self.next_id - 1


def prom(expr: str, legend: str = "", ref: str = "A") -> dict[str, Any]:
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "range": True}


def ch(sql: str, ref: str = "A", fmt: str = "table") -> dict[str, Any]:
    return {
        "refId": ref,
        "datasource": CH,
        "editorType": "sql",
        "queryType": fmt,
        "format": 1 if fmt == "table" else 0,
        "rawSql": " ".join(sql.split()),
        "pluginVersion": "4.23.4",
    }


def panel(
    lay: Layout,
    kind: str,
    title: str,
    targets: list[dict[str, Any]],
    w: int = 12,
    h: int = 8,
    unit: str | None = None,
    desc: str = "",
    options: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ds = targets[0]["datasource"] if targets else PROM
    fd: dict[str, Any] = dict(defaults or {})
    if unit:
        fd["unit"] = unit
    return {
        "id": lay.id(),
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": ds,
        "gridPos": lay.place(w, h),
        "targets": targets,
        "fieldConfig": {"defaults": fd, "overrides": []},
        "options": options or {},
    }


def dashboard(uid: str, title: str, panels: list[dict[str, Any]], **kw: Any) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "tags": ["tickdb"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": kw.get("refresh", "30s"),
        "time": {"from": kw.get("time_from", "now-24h"), "to": "now"},
        "templating": {"list": kw.get("variables", [])},
        "annotations": {"list": []},
        "panels": panels,
    }


def venue_var(ds: dict[str, Any], query: Any) -> dict[str, Any]:
    return {
        "name": "venue",
        "label": "Venue",
        "type": "query",
        "datasource": ds,
        "query": query,
        "definition": query if isinstance(query, str) else query.get("rawSql", ""),
        "refresh": 2,
        "includeAll": True,
        "multi": True,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "sort": 1,
    }


THRESH_COVERAGE = {
    "mode": "absolute",
    "steps": [
        {"color": "red", "value": None},
        {"color": "orange", "value": 95},
        {"color": "green", "value": 99},
    ],
}


def feed_health() -> dict[str, Any]:
    lay = Layout()
    p = [
        panel(lay, "stat", "Connected websockets",
              [prom(f"tickdb_ws_connected{{{V}}}", "{{venue}}")], w=6, h=5,
              desc="Open connections of the shared recorder (0 while its status is stale)",
              defaults={"thresholds": {"mode": "absolute", "steps": [
                  {"color": "red", "value": None}, {"color": "green", "value": 1}]}}),
        panel(lay, "stat", "Recorder status age", [prom("tickdb_capture_status_age_seconds")],
              w=6, h=5, unit="s",
              defaults={"thresholds": {"mode": "absolute", "steps": [
                  {"color": "green", "value": None}, {"color": "red", "value": 120}]}}),
        panel(lay, "stat", "Host clock offset (NTP)", [prom("tickdb_clock_offset_seconds * 1000")],
              w=6, h=5, unit="ms",
              defaults={"thresholds": {"mode": "absolute", "steps": [
                  {"color": "green", "value": None}, {"color": "red", "value": 50}]}}),
        panel(lay, "alertlist", "Firing alerts", [], w=6, h=5,
              options={"alertListOptions": {"showOptions": "current"}, "maxItems": 20,
                       "sortOrder": 1, "stateFilter": {"firing": True, "pending": True},
                       "alertInstanceLabelFilter": "", "datasource": "Prometheus",
                       "viewMode": "list", "groupMode": "default"}),
        panel(lay, "timeseries", "Messages per second", [prom(
            f"sum by (venue, channel) (rate(tickdb_stream_messages_total{{{V}}}[1m]))",
            "{{venue}} {{channel}}")], unit="short"),
        panel(lay, "timeseries", "Frames per second", [prom(
            f"rate(tickdb_ws_frames_total{{{V}}}[1m])", "{{venue}}")], unit="short"),
        panel(lay, "timeseries", "Feed latency, live median", [prom(
            f"tickdb_feed_latency_p50_seconds{{{V}}}", "{{venue}}")], unit="s",
            desc="ts_recv - ts_exchange; the shared recorder publishes a running median"),
        panel(lay, "timeseries", "Feed latency p50 / p99 per hour (daily DQ)", [ch(f"""
            SELECT toDateTime(date) + toIntervalHour(hour) AS time,
                   concat(venue, ' ', dataset, ' p99') AS series, max(p99_ms) AS value
            FROM tickdb.dq_latency FINAL
            WHERE $__dateFilter(date) AND {CH_VENUE} AND n > 0
            GROUP BY time, series ORDER BY time""", fmt="timeseries"), ch(f"""
            SELECT toDateTime(date) + toIntervalHour(hour) AS time,
                   concat(venue, ' ', dataset, ' p50') AS series, max(p50_ms) AS value
            FROM tickdb.dq_latency FINAL
            WHERE $__dateFilter(date) AND {CH_VENUE} AND n > 0
            GROUP BY time, series ORDER BY time""", ref="B", fmt="timeseries")], unit="ms",
            desc="Worst symbol per venue and dataset; filled by the daily job for the day before"),
        panel(lay, "timeseries", "Reconnects per hour", [prom(
            f"sum by (venue) (changes(tickdb_ws_connection_generation{{{V}}}[1h]))",
            "{{venue}}")], unit="short"),
        panel(lay, "timeseries", "Book resyncs per hour", [prom(
            f"sum by (venue) (increase(tickdb_resyncs_total{{{V}}}[1h]))", "{{venue}}")],
            unit="short"),
        panel(lay, "timeseries", "Last-frame age (worst stream)", [prom(
            f"max by (venue, channel) (tickdb_last_frame_age_seconds{{{V}}})",
            "{{venue}} {{channel}}")], unit="s"),
        panel(lay, "timeseries", "Ingest delay p99 and recorder errors", [prom(
            f"tickdb_ingest_delay_p99_seconds{{{V}}}", "{{venue}} ingest p99"), prom(
            "tickdb_capture_errors_total", "{{kind}}", ref="B")], unit="s",
            desc="The shared recorder exports no queue depth; its ingest delay p99 and dropped "
                 "or failed writes stand in for it"),
        panel(lay, "timeseries", "Clock offset", [prom(
            "tickdb_clock_offset_seconds * 1000", "host vs NTP"), prom(
            f"tickdb_venue_clock_offset_seconds{{{V}}} * 1000", "{{venue}} server time",
            ref="B")], unit="ms"),
        panel(lay, "timeseries", "Disk free", [prom("tickdb_disk_free_ratio", "{{path}}")],
              unit="percentunit"),
        panel(lay, "stat", "Last job runs", [prom(
            "tickdb_job_last_run_ok", "{{job}} {{venue}}")], w=24, h=5,
            defaults={"mappings": [{"type": "value", "options": {
                "0": {"text": "failed", "color": "red"}, "1": {"text": "ok", "color": "green"}}}]}),
    ]  # fmt: skip
    var = venue_var(PROM, {"query": "label_values(tickdb_ws_connected, venue)", "refId": "v"})
    return dashboard("tickdb-feed-health", "Feed health", p, variables=[var])


def data_quality() -> dict[str, Any]:
    lay = Layout()
    p = [
        panel(lay, "state-timeline", "Coverage per stream (% of minutes)", [ch(f"""
            SELECT toDateTime(date) AS time, concat(venue, ' ', symbol, ' ', dataset) AS stream,
                   coverage_pct AS value
            FROM tickdb.dq_daily FINAL WHERE $__dateFilter(date) AND {CH_VENUE}
            ORDER BY time""", fmt="timeseries")], w=24, h=12, unit="percent",
            desc="DQ-10: minutes with at least one message / 1440 (maintenance excluded)",
            options={"showValue": "auto", "mergeValues": False, "rowHeight": 0.8},
            defaults={"thresholds": THRESH_COVERAGE, "color": {"mode": "thresholds"}}),
        panel(lay, "barchart", "Daily status", [ch(f"""
            SELECT toString(date) AS day, countIf(status = 'pass') AS pass,
                   countIf(status = 'warn') AS warn, countIf(status = 'fail') AS fail
            FROM tickdb.dq_daily FINAL WHERE $__dateFilter(date) AND {CH_VENUE}
            GROUP BY day ORDER BY day""")],
            options={"stacking": "normal", "xField": "day"}),
        panel(lay, "timeseries", "Duplicates removed (trades)", [ch(f"""
            SELECT toDateTime(date) AS time, concat(venue, ' ', symbol) AS stream,
                   sum(dup_rows_removed) AS value
            FROM tickdb.dq_daily FINAL
            WHERE $__dateFilter(date) AND {CH_VENUE} AND dataset = 'trades'
            GROUP BY time, stream ORDER BY time""", fmt="timeseries")], unit="short",
            options={"drawStyle": "bars"}),
        panel(lay, "timeseries", "Outliers flagged", [ch(f"""
            SELECT toDateTime(date) AS time, concat(venue, ' ', rule) AS series, count() AS value
            FROM tickdb.dq_outliers FINAL WHERE $__dateFilter(date) AND {CH_VENUE}
            GROUP BY time, series ORDER BY time""", fmt="timeseries")], unit="short",
            desc="DQ-09; flagged, never deleted"),
        panel(lay, "timeseries", "Crossed books outside resync", [ch(f"""
            SELECT toDateTime(date) AS time, concat(venue, ' ', symbol) AS stream,
                   sum(crossed_bbo) AS value
            FROM tickdb.dq_daily FINAL
            WHERE $__dateFilter(date) AND {CH_VENUE} AND dataset = 'bbo'
            GROUP BY time, stream ORDER BY time""", fmt="timeseries")], unit="short"),
        panel(lay, "table", "Gaps", [ch(f"""
            SELECT fromUnixTimestamp64Nano(start_recv) AS start, venue, symbol, channel, kind,
                   round((end_recv - start_recv) / 1e9, 1) AS seconds, recovery, status,
                   recovered_rows
            FROM tickdb.gaps FINAL
            WHERE $__timeFilter(fromUnixTimestamp64Nano(start_recv)) AND {CH_VENUE}
            ORDER BY start_recv DESC LIMIT 1000""")], w=24, h=10),
        panel(lay, "table", "Daily DQ rows", [ch(f"""
            SELECT date, venue, symbol, dataset, status, rows, coverage_pct, gap_count,
                   gap_seconds_unrecovered, dup_rows_removed, outliers, crossed_bbo,
                   round(latency_p50_ms, 1) AS p50_ms, round(latency_p99_ms, 1) AS p99_ms,
                   round(clock_offset_max_ms, 1) AS clock_ms
            FROM tickdb.dq_daily FINAL WHERE $__dateFilter(date) AND {CH_VENUE}
            ORDER BY date DESC, venue, symbol, dataset""")], w=24, h=12),
    ]  # fmt: skip
    var = venue_var(CH, ch("SELECT DISTINCT venue FROM tickdb.dq_daily ORDER BY venue", ref="v"))
    return dashboard("tickdb-data-quality", "Data quality", p, variables=[var],
                     time_from="now-30d", refresh="")  # fmt: skip


DASHBOARDS = {"feed_health.json": feed_health, "data_quality.json": data_quality}


def render() -> dict[str, str]:
    return {n: json.dumps(fn(), indent=2, sort_keys=True) + "\n" for n, fn in DASHBOARDS.items()}


def main() -> int:
    check = "--check" in sys.argv[1:]
    bad = 0
    for name, text in render().items():
        path = OUT / name
        if check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                print(f"{path} is out of date: run scripts/gen_dashboards.py")
                bad += 1
        else:
            path.write_text(text, encoding="utf-8", newline="\n")
            print(path)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
