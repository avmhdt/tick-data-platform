"""Does the mirror slow the shared recorder? Compare lob-recorder's per-minute ingest p99 in
minutes that overlap a mirror run against the 30 minutes before each run (same market
conditions; the recorder's p99 follows the day's message rate, so a whole-day baseline is
not comparable).

Usage (inside WSL):
    journalctl -u lob-recorder --since "2026-11-10 19:00" -o cat \
      | python scripts/recorder_impact.py ~/data/tick-data-platform/meta/mirror/run.log
"""

from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timedelta

BEFORE = timedelta(minutes=30)
PAD = timedelta(minutes=1)


def _windows(run_log: str) -> list[tuple[datetime, datetime]]:
    with open(run_log, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    out = []
    for line in lines:
        rec = json.loads(line)
        if "started" in rec:
            out.append((datetime.fromisoformat(rec["started"]), datetime.fromisoformat(rec["utc"])))
    return out


def main(run_log: str) -> int:
    runs = _windows(run_log)
    during: dict[str, list[float]] = {}
    before: dict[str, list[float]] = {}
    for line in sys.stdin:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("event") != "venue_stats" or rec.get("ingest_us_p99") is None:
            continue
        stamp = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
        value = float(rec["ingest_us_p99"])
        if any(a - PAD <= stamp <= b + PAD for a, b in runs):
            during.setdefault(rec["venue"], []).append(value)
        elif any(a - BEFORE <= stamp < a - PAD for a, _ in runs):
            before.setdefault(rec["venue"], []).append(value)
    for venue in sorted(set(during) | set(before)):
        a, b = during.get(venue, []), before.get(venue, [])
        print(
            json.dumps(
                {
                    "venue": venue,
                    "runs": len(runs),
                    "minutes_during": len(a),
                    "minutes_before": len(b),
                    "p99_us_median_during": round(statistics.median(a), 1) if a else None,
                    "p99_us_median_before": round(statistics.median(b), 1) if b else None,
                    "p99_us_max_during": round(max(a), 1) if a else None,
                    "p99_us_max_before": round(max(b), 1) if b else None,
                }
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
