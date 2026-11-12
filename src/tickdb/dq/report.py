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


COUNTED = ("checksum_ok", "checksum_fail", "ckpt_match", "ckpt_mismatch", "ckpt_skipped")


def book_checks(data_root: Path, first: date, last: date) -> dict[str, dict[str, Any]]:
    """AC-W-05 evidence per venue over [first, last] from compaction records and gaps."""
    out: dict[str, dict[str, Any]] = {}
    day = first
    while day <= last:
        for path in sorted((data_root / "meta" / "compaction").glob(f"venue=*/date={day}.json")):
            venue = path.parent.name.removeprefix("venue=")
            rec = orjson.loads(path.read_bytes())
            c = rec.get("counters", {})
            zero = dict.fromkeys(COUNTED, 0) | {"crossed_bbo": 0, "seq_gaps": 0}
            zero |= {"crossed_bbo_venue_confirmed": 0}
            v = out.setdefault(venue, {"days": 0, **zero, "resync_seconds": [], "exceptions": []})
            v["days"] += 1
            for key in (
                "checksum_ok",
                "checksum_fail",
                "ckpt_match",
                "ckpt_mismatch",
                "ckpt_skipped",
            ):
                v[key] += int(c.get(key, 0))
            for key, n in c.items():
                if not key.startswith("bbo_crossed/") or not n:
                    continue
                symbol = key.removeprefix("bbo_crossed/")
                confirmed = c.get(f"crossed_confirmed/{symbol}", 0)
                unverified = c.get(f"crossed_unverified/{symbol}", 0)
                if confirmed and not unverified:  # every crossed book passed the venue checksum
                    v["crossed_bbo_venue_confirmed"] += int(n)
                    v["exceptions"].append(
                        f"{day} {symbol}: {n} crossed BBO rows published by the venue "
                        "(checksum-confirmed); documented exception, owner 2026-11-12"
                    )
                else:
                    v["crossed_bbo"] += int(n)
        for gap in load_gaps(data_root, day):
            if gap["channel"] != "book_l2" or gap["venue"] not in out:
                continue
            v = out[gap["venue"]]
            if gap["kind"] == "seq_gap":
                v["seq_gaps"] += 1
                v.setdefault("seq_gaps_resynced", 0)
                v["seq_gaps_resynced"] += gap["status"] == "partial"
            if gap["status"] == "partial":  # closed by a snapshot: detection -> validated
                v["resync_seconds"].append(gap["duration_s"])
        day += timedelta(days=1)
    _coinbase_checkpoints(data_root, first, last, out)
    for v in out.values():
        secs = sorted(v.pop("resync_seconds"))
        v["resyncs"] = len(secs)
        v["resync_median_s"] = secs[len(secs) // 2] if secs else None
        total = v["checksum_ok"] + v["checksum_fail"]
        v["checksum_match_pct"] = round(100 * v["checksum_ok"] / total, 4) if total else None
        compared = v["ckpt_match"] + v["ckpt_mismatch"]
        v["ckpt_match_pct"] = round(100 * v["ckpt_match"] / compared, 3) if compared else None
    _label(out, data_root)
    tested = Path("results") / "ingestor_resync.json"
    if tested.exists():  # this project's ingestor against mock venues, labelled as such
        for venue, figures in orjson.loads(tested.read_bytes()).items():
            if venue in out:
                out[venue]["ingestor_tested_resync"] = figures
    return out


THRESHOLDS = {"checksum_match_pct": 100.0, "ckpt_binance": 100.0, "ckpt_coinbase": 99.0,
              "resync_median_s": 10.0}  # fmt: skip


def _label(out: dict[str, dict[str, Any]], data_root: Path) -> None:
    """Pass/miss per AC-W-05 item, with the owner's rulings of 2026-11-12 (thresholds kept)."""
    for venue, v in out.items():
        verdicts: dict[str, str] = {}
        if v["checksum_match_pct"] is not None:
            verdicts["checksums"] = "pass" if v["checksum_match_pct"] >= 100.0 else "miss"
        if v["ckpt_match_pct"] is not None:
            need = THRESHOLDS["ckpt_coinbase" if venue == "coinbase" else "ckpt_binance"]
            ok = v["ckpt_match_pct"] >= need
            verdicts["checkpoints"] = "pass" if ok else "miss"
            if venue == "coinbase":
                v["ckpt_method"] = "sequence-aligned via heartbeats, +-1 s (REST book ~1 s stale)"
                if not ok:
                    verdicts["checkpoints"] = "documented miss (owner 2026-11-12)"
        verdicts["crossed_bbo"] = "pass" if v["crossed_bbo"] == 0 else "miss"
        if v["resync_median_s"] is not None:
            ok = v["resync_median_s"] <= THRESHOLDS["resync_median_s"]
            v["resync_source"] = "shared recorder (crypto-lob-alpha), last good message to snapshot"
            verdicts["resync_median"] = (
                "pass" if ok else ("documented exception (owner 2026-11-12): shared recorder")
            )
        v["verdicts"] = verdicts
        v["seq_breaks_resynced"] = v.get("seq_gaps_resynced", 0) == v["seq_gaps"]


def _coinbase_checkpoints(
    data_root: Path, first: date, last: date, out: dict[str, dict[str, Any]]
) -> None:
    """Coinbase checkpoint results come from ``tickdb dq checkpoints`` (sequence-aligned)."""
    if "coinbase" not in out:
        return
    v = out["coinbase"]
    v["ckpt_match"] = v["ckpt_mismatch"] = 0
    day = first
    while day <= last:
        path = data_root / "meta" / "checkpoints" / "venue=coinbase" / f"date={day}.json"
        if path.exists():
            for sym in orjson.loads(path.read_bytes()).values():
                v["ckpt_match"] += sym["match_1.0s"]
                v["ckpt_mismatch"] += sym["compared"] - sym["match_1.0s"]
        day += timedelta(days=1)
