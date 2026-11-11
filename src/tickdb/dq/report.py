"""Daily gap report and rolling summary (FR-16). Aggregates only: times, durations, kinds,
recovery and status of every gap, never a price (NFR-09).

Files: ``reports/dq/YYYY-MM-DD.md``, ``.csv`` and ``.json`` (one row per gap, the ``gaps``
schema plus duration), and ``reports/dq/summary.md`` over a date range.
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq

from tickdb import schemas


def _utc(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).strftime("%H:%M:%S")


def load_gaps(data_root: Path, day: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = data_root / "meta" / "gaps"
    for path in sorted(base.glob(f"venue=*/date={day.isoformat()}")):
        rows.extend(pq.read_table(path).to_pylist())
    for row in rows:
        row["duration_s"] = round((row["end_recv"] - row["start_recv"]) / 1e9, 3)
    rows.sort(key=lambda r: (r["venue"], r["symbol"], r["channel"], r["start_recv"]))
    return rows


def write_day(data_root: Path, day: date, out: Path) -> dict[str, Any]:
    """Write the three files for ``day``; return its per-stream totals."""
    rows = load_gaps(data_root, day)
    out.mkdir(parents=True, exist_ok=True)
    columns = [*schemas.GAPS.names, "duration_s"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows({c: r.get(c) for c in columns} for r in rows)
    (out / f"{day.isoformat()}.csv").write_text(buf.getvalue(), encoding="utf-8")
    totals: dict[str, dict[str, float]] = defaultdict(lambda: {"gaps": 0, "seconds": 0.0})
    for r in rows:
        key = f"{r['venue']} {r['symbol']} {r['channel']}"
        totals[key]["gaps"] += 1
        totals[key]["seconds"] += r["duration_s"]
    summary = {"date": day.isoformat(), "gaps": len(rows), "streams": dict(totals)}
    (out / f"{day.isoformat()}.json").write_bytes(
        orjson.dumps(
            {**summary, "rows": [{c: r.get(c) for c in columns} for r in rows]},
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        )
    )
    lines = [
        f"# Gap report {day.isoformat()}",
        "",
        f"{len(rows)} gaps. Times are UTC receive times (`start` = last good message, "
        "`end` = first good one). Book gaps end with a snapshot; the L2 history inside them "
        "is unrecoverable on free sources, so a recovered book gap is `partial`. Trade gaps "
        "stay `open` until REST backfill classifies them.",
        "",
        "| Venue | Symbol | Channel | Kind | Start | End | Seconds | Recovery | Status |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r['venue']} | {r['symbol']} | {r['channel']} | {r['kind']} | {_utc(r['start_recv'])}"
        f" | {_utc(r['end_recv'])} | {r['duration_s']:.1f} | {r['recovery']} | {r['status']} |"
        for r in rows
    ]
    (out / f"{day.isoformat()}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def write_summary(out: Path, first: date, last: date) -> Path:
    """``summary.md`` from the per-day JSON files already in ``out``."""
    lines = [
        f"# Gap summary {first.isoformat()} to {last.isoformat()}",
        "",
        "| Date | Gaps | Worst stream | Its gap seconds |",
        "|---|---|---|---|",
    ]
    day = first
    while day <= last:
        path = out / f"{day.isoformat()}.json"
        if path.exists():
            data = orjson.loads(path.read_bytes())
            streams = data.get("streams", {})
            worst = max(streams.items(), key=lambda kv: kv[1]["seconds"], default=None)
            name, secs = (worst[0], worst[1]["seconds"]) if worst else ("-", 0.0)
            lines.append(f"| {day.isoformat()} | {data['gaps']} | {name} | {secs:.1f} |")
        else:
            lines.append(f"| {day.isoformat()} | no report | - | - |")
        day += timedelta(days=1)
    target = out / "summary.md"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target
