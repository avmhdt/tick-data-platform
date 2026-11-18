"""Retention (FR-22, spec 7d; D-10 owner decision 2026-11-09: raw kept 7 days).

``apply`` deletes a raw venue-day (the mirrored capture and the REST backfill spool) only
when it is older than ``raw_spool_days`` **and** its replay hash check passed
(``meta/verify/venue=<v>/date=<D>.json`` with ``ok: true``). Unverified days are never
selected, whatever their age. A deleted day leaves a tombstone in ``meta/retention/`` so the
mirror never copies it again. The Parquet archive, gaps and DQ results are kept (ClickHouse
``book_l2`` has its own 14-day TTL). Staging and replay scratch older than a day are removed.
"""

from __future__ import annotations

import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import orjson

RAW_ROOTS = (Path("raw") / "external" / "lob-alpha", Path("raw") / "backfill")
DATE_RE = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")


@dataclass
class Plan:
    delete: list[Path] = field(default_factory=list)
    keep_unverified: list[str] = field(default_factory=list)
    bytes: int = 0


def verified(data_root: Path, venue: str, day: date) -> bool:
    path = data_root / "meta" / "verify" / f"venue={venue}" / f"date={day.isoformat()}.json"
    return path.exists() and bool(orjson.loads(path.read_bytes()).get("ok"))


def record_verify(data_root: Path, venue: str, day: date, report: dict[str, object]) -> Path:
    path = data_root / "meta" / "verify" / f"venue={venue}" / f"date={day.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
    return path


def retired(data_root: Path, venue: str, day: date) -> bool:
    return (data_root / "meta" / "retention" / f"venue={venue}" / f"date={day}.json").exists()


def _size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def plan(data_root: Path, raw_days: int, today: date | None = None) -> Plan:
    today = today or datetime.now(UTC).date()
    cutoff = today - timedelta(days=raw_days)
    out = Plan()
    for rel in RAW_ROOTS:
        for venue_dir in sorted((data_root / rel).glob("venue=*")):
            venue = venue_dir.name.removeprefix("venue=")
            for day_dir in sorted(venue_dir.glob("date=*")):
                match = DATE_RE.match(day_dir.name)
                if match is None:
                    continue
                day = date.fromisoformat(match.group(1))
                if day >= cutoff:
                    continue
                if not verified(data_root, venue, day):
                    out.keep_unverified.append(f"{venue} {day}")
                    continue
                out.delete.append(day_dir)
                out.bytes += _size(day_dir)
    return out


def apply(data_root: Path, raw_days: int, *, dry_run: bool, today: date | None = None) -> Plan:
    result = plan(data_root, raw_days, today)
    if dry_run:
        return result
    for day_dir in result.delete:
        venue = day_dir.parent.name.removeprefix("venue=")
        day = day_dir.name.removeprefix("date=")
        tomb = data_root / "meta" / "retention" / f"venue={venue}" / f"date={day}.json"
        tomb.parent.mkdir(parents=True, exist_ok=True)
        tomb.write_bytes(orjson.dumps({"deleted_utc": datetime.now(UTC).isoformat()}))
        shutil.rmtree(day_dir)
    for scratch in ("staging", "replay"):
        for old in (data_root / "interim" / scratch).glob("*"):
            if time.time() - old.stat().st_mtime > 86_400:
                shutil.rmtree(old, ignore_errors=True)
    return result
