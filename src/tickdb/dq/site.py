"""Static public DQ site (FR-26, AC-H-04): ``tickdb dq site --out site-dq``.

One self-contained ``index.html`` plus ``dq_daily.csv``, ``gaps.csv`` and ``summary.json``,
built from the DQ tables in the data root. Aggregates only (NFR-09): coverage, gap times and
durations, latency percentiles, row counts and statuses. Never a price, size or trade id.
"""

from __future__ import annotations

import csv
import html
import io
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import orjson

from tickdb.dq import daily
from tickdb.dq.report import load_gaps

PUBLIC_DAILY = (
    "date", "venue", "symbol", "dataset", "status", "rows", "coverage_pct", "gap_count",
    "gap_seconds_total", "gap_seconds_unrecovered", "dup_rows_removed", "outliers",
    "crossed_bbo", "latency_p50_ms", "latency_p99_ms", "latency_max_ms", "clock_offset_max_ms",
)  # fmt: skip
PUBLIC_GAPS = ("venue", "symbol", "channel", "kind", "start_utc", "end_utc", "seconds",
               "recovery", "status", "recovered_rows")  # fmt: skip

CSS = """
:root{--bg:#fbfaf7;--fg:#1d1d1b;--muted:#6b6a65;--line:#e4e2dc;--card:#fff;
--pass:#2f8f5b;--warn:#c98a1b;--fail:#c2412d;--none:#d9d7d0;--accent:#24548f}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--fg:#ecebe6;--muted:#a3a29b;
--line:#2f2e2b;--card:#1c1c1a;--none:#3a3935;--accent:#8db4e8}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1180px;margin:0 auto;padding:32px 16px 64px}
h1{font-size:28px;margin:0 0 4px}h2{font-size:19px;margin:40px 0 8px}
p.lede{color:var(--muted);margin:0 0 24px;max-width:70ch}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.card b{display:block;font-size:24px;font-variant-numeric:tabular-nums}
.card span{color:var(--muted);font-size:13px}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:10px;background:var(--card)}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;font-size:13px}
th,td{padding:5px 8px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left;position:sticky;left:0;background:var(--card)}
thead th{color:var(--muted);font-weight:600}
td.cell{padding:0;width:22px;min-width:22px;height:22px}
td.cell i{display:block;width:18px;height:18px;margin:2px auto;border-radius:3px}
.pass{background:var(--pass)}.warn{background:var(--warn)}.fail{background:var(--fail)}
.none{background:var(--none)}
.legend{display:flex;gap:16px;color:var(--muted);font-size:13px;margin:8px 0 0;flex-wrap:wrap}
.legend i{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;
vertical-align:-1px}
.tag{display:inline-block;padding:0 6px;border-radius:4px;color:#fff;font-size:12px}
footer{color:var(--muted);font-size:13px;margin-top:48px}
a{color:var(--accent)}
"""


def _utc(ns: int | None) -> str:
    if ns is None:
        return ""
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def _cov_class(cov: float | None, warn: float, fail: float) -> str:
    if cov is None:
        return "none"
    return "pass" if cov >= warn else ("warn" if cov >= fail else "fail")


def _csv(rows: list[dict[str, Any]], cols: tuple[str, ...]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def _fmt(v: Any, nd: int = 1) -> str:
    if v is None:
        return "&ndash;"
    if isinstance(v, float):
        return f"{v:,.{nd}f}"
    return f"{v:,}" if isinstance(v, int) else html.escape(str(v))


def build(
    data_root: Path,
    out: Path,
    first: date | None = None,
    last: date | None = None,
    limits: daily.Limits | None = None,
    title: str = "Tick data quality",
) -> dict[str, Any]:
    lim = limits or daily.Limits()
    table = daily.read_table(data_root, "dq_daily", first or date.min, last or date.max)
    rows = sorted(
        table.to_pylist(), key=lambda r: (r["date"], r["venue"], r["symbol"], r["dataset"])
    )
    out.mkdir(parents=True, exist_ok=True)
    days = sorted({r["date"] for r in rows})
    if days:
        first, last = first or days[0], last or days[-1]
    all_days = []
    d = first
    while d is not None and last is not None and d <= last:
        all_days.append(d)
        d += timedelta(days=1)
    streams = sorted({(r["venue"], r["symbol"], r["dataset"]) for r in rows})
    by = {(r["venue"], r["symbol"], r["dataset"], r["date"]): r for r in rows}
    gaps = [
        {
            "venue": g["venue"], "symbol": g["symbol"], "channel": g["channel"], "kind": g["kind"],
            "start_utc": _utc(g["start_recv"]), "end_utc": _utc(g["end_recv"]),
            "seconds": g["duration_s"], "recovery": g["recovery"], "status": g["status"],
            "recovered_rows": g["recovered_rows"],
        }
        for day in all_days
        for g in load_gaps(data_root, day)
    ]  # fmt: skip
    statuses: dict[str, int] = defaultdict(int)
    for r in rows:
        statuses[r["status"]] += 1
    summary: dict[str, Any] = {
        "generated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "first": first.isoformat() if first else None,
        "last": last.isoformat() if last else None,
        "days": len(days),
        "streams": len(streams),
        "rows_by_status": dict(sorted(statuses.items())),
        "gaps": len(gaps),
        "gap_seconds": round(sum(g["seconds"] for g in gaps), 1),
    }
    public = [{k: (r[k].isoformat() if k == "date" else r[k]) for k in PUBLIC_DAILY} for r in rows]
    (out / "dq_daily.csv").write_text(_csv(public, PUBLIC_DAILY), encoding="utf-8")
    (out / "gaps.csv").write_text(_csv(gaps, PUBLIC_GAPS), encoding="utf-8")
    (out / "summary.json").write_bytes(orjson.dumps(summary, option=orjson.OPT_INDENT_2))
    (out / "index.html").write_text(
        _page(title, summary, all_days, streams, by, rows, gaps, lim), encoding="utf-8"
    )
    return summary


def _page(
    title: str,
    summary: dict[str, Any],
    days: list[date],
    streams: list[tuple[str, str, str]],
    by: dict[tuple[str, str, str, date], dict[str, Any]],
    rows: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    lim: daily.Limits,
) -> str:
    e = html.escape
    head = "".join(f"<th title='{d.isoformat()}'>{d.day:02d}</th>" for d in days)
    heat = []
    for v, sym, ds in streams:
        cells = []
        for d in days:
            r = by.get((v, sym, ds, d))
            cov = r["coverage_pct"] if r else None
            cls = _cov_class(cov, lim.coverage_warn_pct, lim.coverage_fail_pct)
            tip = f"{v} {sym} {ds} {d}: " + (
                f"{cov:.2f} % of minutes, status {r['status']}" if r else "no data"
            )
            cells.append(f"<td class='cell'><i class='{cls}' title='{e(tip)}'></i></td>")
        heat.append(f"<tr><td>{e(v)} {e(sym)} {e(ds)}</td>{''.join(cells)}</tr>")
    status_rows = []
    for d in days:
        day_rows = [r for r in rows if r["date"] == d]
        if not day_rows:
            status_rows.append(f"<tr><td>{d}</td><td colspan='4'>no data</td></tr>")
            continue
        n = {k: sum(r["status"] == k for r in day_rows) for k in ("pass", "warn", "fail")}
        worst = min(r["coverage_pct"] for r in day_rows)
        overall = "fail" if n["fail"] else ("warn" if n["warn"] else "pass")
        status_rows.append(
            f"<tr><td>{d}</td><td><span class='tag {overall}'>{overall}</span></td>"
            f"<td>{n['pass']}</td><td>{n['warn']}</td><td>{n['fail']}</td><td>{worst:.2f}</td></tr>"
        )
    lat: dict[tuple[str, date], dict[str, Any]] = {}
    for r in rows:
        if r["dataset"] != "trades" and r["dataset"] != "book_l2":
            continue
        k = (r["venue"], r["date"])
        cur = lat.setdefault(k, {"p50": [], "p99": [], "clock": r["clock_offset_max_ms"]})
        if r["latency_p50_ms"] is not None:
            cur["p50"].append(r["latency_p50_ms"])
            cur["p99"].append(r["latency_p99_ms"])
    venues = sorted({v for v, _ in lat})
    lat_head = "".join(f"<th>{e(v)} p50</th><th>{e(v)} p99</th>" for v in venues)
    lat_rows = []
    for d in days:
        cells = []
        for v in venues:
            c = lat.get((v, d))
            if not c or not c["p50"]:
                cells.append("<td>&ndash;</td><td>&ndash;</td>")
                continue
            p50 = sorted(c["p50"])[len(c["p50"]) // 2]
            cells.append(f"<td>{p50:,.0f}</td><td>{max(c['p99']):,.0f}</td>")
        lat_rows.append(f"<tr><td>{d}</td>{''.join(cells)}</tr>")
    top = sorted(gaps, key=lambda g: -g["seconds"])[:200]
    gap_rows = "".join(
        f"<tr><td>{e(g['venue'])} {e(g['symbol'])}</td><td>{e(g['channel'])}</td>"
        f"<td>{e(g['kind'])}</td><td>{e(g['start_utc'])}</td><td>{g['seconds']:,.1f}</td>"
        f"<td>{e(g['recovery'])}</td><td>{e(g['status'])}</td></tr>"
        for g in top
    )
    s = summary
    cards = "".join(
        f"<div class='card'><b>{_fmt(val)}</b><span>{e(label)}</span></div>"
        for label, val in (
            ("days with DQ rows", s["days"]),
            ("streams", s["streams"]),
            ("pass rows", s["rows_by_status"].get("pass", 0)),
            ("warn rows", s["rows_by_status"].get("warn", 0)),
            ("fail rows", s["rows_by_status"].get("fail", 0)),
            ("gaps", s["gaps"]),
        )
    )
    legend = (
        f"<div class='legend'><span><i class='pass'></i>≥ {lim.coverage_warn_pct:g} %</span>"
        f"<span><i class='warn'></i>{lim.coverage_fail_pct:g} to {lim.coverage_warn_pct:g} %"
        f"</span><span><i class='fail'></i>below {lim.coverage_fail_pct:g} %</span>"
        "<span><i class='none'></i>no data</span></div>"
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{e(title)}</title><style>{CSS}</style></head>
<body><main>
<h1>{e(title)}</h1>
<p class="lede">Daily data-quality results for the captured crypto streams, {e(str(s["first"]))}
to {e(str(s["last"]))} (UTC). Aggregates only: coverage, gaps, latency and status; no prices.
Generated {e(s["generated_utc"])}. Data: <a href="dq_daily.csv">dq_daily.csv</a>,
<a href="gaps.csv">gaps.csv</a>, <a href="summary.json">summary.json</a>.</p>
<div class="cards">{cards}</div>

<h2>Coverage per stream</h2>
<p class="lede">Share of the day's minutes with at least one message (DQ-10). Hover a cell for
the value.</p>
<div class="scroll"><table><thead><tr><th>Stream</th>{head}</tr></thead>
<tbody>{"".join(heat)}</tbody></table></div>{legend}

<h2>Daily status</h2>
<p class="lede">A row fails on a remaining duplicate, a crossed book outside resync, an open
gap, coverage below {lim.coverage_fail_pct:g} % or a missing content hash; it warns on coverage
below {lim.coverage_warn_pct:g} %, outliers, unrecovered gap seconds or an unreliable latency
hour.</p>
<div class="scroll"><table><thead><tr><th>Date</th><th>Day</th><th>Pass</th><th>Warn</th>
<th>Fail</th><th>Lowest coverage %</th></tr></thead><tbody>{"".join(status_rows)}</tbody>
</table></div>

<h2>Feed latency (ms)</h2>
<p class="lede">Receive time minus exchange time of websocket messages: median over the venue's
symbols of the daily p50, and the worst daily p99. It includes the host clock offset.</p>
<div class="scroll"><table><thead><tr><th>Date</th>{lat_head}</tr></thead>
<tbody>{"".join(lat_rows)}</tbody></table></div>

<h2>Longest gaps</h2>
<p class="lede">The 200 longest of {s["gaps"]} gaps ({s["gap_seconds"]:,.0f} s in all); every
gap is in <a href="gaps.csv">gaps.csv</a>. A book gap is restored by a snapshot, but the L2
history inside it can't be recovered from free sources, so it is <code>partial</code>.</p>
<div class="scroll"><table><thead><tr><th>Stream</th><th>Channel</th><th>Kind</th>
<th>Start (UTC)</th><th>Seconds</th><th>Recovery</th><th>Status</th></tr></thead>
<tbody>{gap_rows}</tbody></table></div>
<footer>Generated by <code>tickdb dq site</code>.</footer>
</main></body></html>
"""
