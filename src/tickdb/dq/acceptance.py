"""Evidence for AC-W-02 (gaps recovered without double counting) and the "DQ tables populated
daily" part of AC-W-09, over a range of days: ``tickdb dq acceptance --from D1 --to D2``.

- Classified: share of gaps whose status is not ``open`` after the daily job (target 100 %).
- Trade-gap seconds recovered, where venue REST (or the vendor file) covers the range: seconds
  of ``recovered`` trade gaps over the seconds of trade gaps not ``unrecoverable`` (target
  >= 95 %). ``partial`` counts as not recovered.
- Duplicate trade keys left in the archive after backfill (target 0), from the DQ notes.
- The data.binance.vision id-set check (``results/binance_vision_check.json``).
- Compacted days that have DQ rows.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import orjson

from tickdb.dq.report import load_gaps


def _days(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def evaluate(
    data_root: Path, first: date, last: date, vision: Path | None = None
) -> dict[str, Any]:
    gaps_by_status: dict[str, int] = defaultdict(int)
    open_gaps: list[str] = []
    trade: dict[str, dict[str, float]] = defaultdict(
        lambda: {"covered_s": 0.0, "recovered_s": 0.0, "unrecoverable_s": 0.0}
    )
    dup_left = 0
    dup_days = 0
    compacted: list[str] = []
    with_dq: list[str] = []
    for day in _days(first, last):
        for g in load_gaps(data_root, day):
            gaps_by_status[g["status"]] += 1
            if g["status"] == "open":
                open_gaps.append(f"{day} {g['venue']} {g['symbol']} {g['channel']}")
            if g["channel"] != "trades":
                continue
            t = trade[g["venue"]]
            if g["status"] == "unrecoverable":
                t["unrecoverable_s"] += g["duration_s"]
                continue
            t["covered_s"] += g["duration_s"]
            if g["status"] == "recovered":
                t["recovered_s"] += g["duration_s"]
        if list((data_root / "meta" / "compaction").glob(f"venue=*/date={day}.json")):
            compacted.append(day.isoformat())
        notes = sorted((data_root / "meta" / "dq_daily" / f"date={day}").glob("venue=*.json"))
        if notes:
            with_dq.append(day.isoformat())
            for path in notes:
                left = orjson.loads(path.read_bytes()).get("dup_trades_remaining", {})
                dup_left += sum(left.values())
                dup_days += 1
    total = sum(gaps_by_status.values())
    covered = sum(t["covered_s"] for t in trade.values())
    recovered = sum(t["recovered_s"] for t in trade.values())
    vision_rows = orjson.loads(vision.read_bytes()) if vision and vision.exists() else []
    independent = [r for r in vision_rows if not r["circular"] and r["next_day_archived"]]
    out: dict[str, Any] = {
        "first": first.isoformat(),
        "last": last.isoformat(),
        "gaps": total,
        "gaps_by_status": dict(sorted(gaps_by_status.items())),
        "classified_pct": round(100 * (total - gaps_by_status["open"]) / total, 3)
        if total
        else None,
        "open_gaps": open_gaps[:50],
        "trade_gap_seconds_covered": round(covered, 1),
        "trade_gap_seconds_recovered": round(recovered, 1),
        "trade_gap_recovered_pct": round(100 * recovered / covered, 3) if covered else None,
        "trade_gaps_by_venue": {
            v: {k: round(x, 1) for k, x in t.items()} for v, t in sorted(trade.items())
        },
        "duplicate_trade_keys_left": dup_left,
        "venue_days_with_dup_check": dup_days,
        "binance_vision_days_equal": [r["date"] for r in independent if r["equal"]],
        "binance_vision_days_checked": [r["date"] for r in independent],
        "compacted_days": compacted,
        "compacted_days_without_dq": sorted(set(compacted) - set(with_dq)),
    }
    out["ac_w02"] = {
        "classified_100pct": out["classified_pct"] == 100.0,
        "recovered_ge_95pct": out["trade_gap_recovered_pct"] is not None
        and out["trade_gap_recovered_pct"] >= 95.0,
        "zero_duplicates": dup_left == 0 and dup_days > 0,
        "vision_ge_3_days": len(out["binance_vision_days_equal"]) >= 3
        and len(out["binance_vision_days_equal"]) == len(independent),
    }
    out["ac_w02"]["pass"] = all(out["ac_w02"].values())
    out["dq_tables_daily"] = not out["compacted_days_without_dq"] and bool(compacted)
    return out
