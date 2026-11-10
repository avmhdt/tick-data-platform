"""Does the mirror slow the shared recorder? Compare lob-recorder's per-minute ingest p99 in
minutes that overlap a mirror run against all other minutes.

Usage (inside WSL):
    journalctl -u lob-recorder --since "2026-11-10 19:00" -o cat \
      | python scripts/recorder_impact.py ~/data/tick-data-platform/meta/mirror/run.log
"""

from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timedelta


def main(run_log: str) -> int:
    windows = []
    for line in open(run_log, encoding="utf-8"):
        rec = json.loads(line)
        if "started" in rec:
            start = datetime.fromisoformat(rec["started"]) - timedelta(minutes=1)
            end = datetime.fromisoformat(rec["utc"]) + timedelta(minutes=1)
            windows.append((start, end))
    inside: dict[str, list[float]] = {}
    outside: dict[str, list[float]] = {}
    for line in sys.stdin:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("event") != "venue_stats" or rec.get("ingest_us_p99") is None:
            continue
        stamp = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
        bucket = inside if any(a <= stamp <= b for a, b in windows) else outside
        bucket.setdefault(rec["venue"], []).append(float(rec["ingest_us_p99"]))
    for venue in sorted(set(inside) | set(outside)):
        a, b = inside.get(venue, []), outside.get(venue, [])
        print(
            json.dumps(
                {
                    "venue": venue,
                    "minutes_during_mirror": len(a),
                    "minutes_otherwise": len(b),
                    "p99_us_median_during": round(statistics.median(a), 1) if a else None,
                    "p99_us_median_otherwise": round(statistics.median(b), 1) if b else None,
                    "p99_us_max_during": round(max(a), 1) if a else None,
                    "p99_us_max_otherwise": round(max(b), 1) if b else None,
                }
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
