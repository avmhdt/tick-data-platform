"""Daily job (FR-31): for day D-1 after 01:30 UTC, per venue,
compact -> backfill -> compact again if REST rows arrived -> load into ClickHouse (skipped,
and logged as skipped, while ClickHouse is not configured) -> replay verify -> gap report;
then the rolling summary and retention.

Each step is idempotent, holds a file lock (one daily job at a time), is retried 3 times
with backoff, and is logged to ``results/job_log.csv`` (git-ignored). The hourly step of the
spec is the mirror's own cron entry; intraday interim parts are left to M4
(docs/decisions.md).
"""

from __future__ import annotations

import csv
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import orjson

LOG_FIELDS = [
    "timestamp_utc",
    "job",
    "date",
    "venue",
    "step",
    "attempt",
    "outcome",
    "seconds",
    "detail",
]


@dataclass
class StepResult:
    step: str
    ok: bool
    detail: str = ""
    skipped: bool = False


@contextmanager
def job_lock(path: Path) -> Any:
    import fcntl  # Unix only; the scheduler runs in WSL

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _log(log: Path, row: dict[str, Any]) -> None:
    log.parent.mkdir(parents=True, exist_ok=True)
    new = not log.exists()
    with log.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=LOG_FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)


def run_step(
    name: str,
    fn: Callable[..., StepResult],
    *,
    log: Path,
    day: date,
    venue: str,
    retries: int = 3,
    backoff_s: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> StepResult:
    """Run ``fn`` up to ``retries`` times; exceptions and ``ok=False`` both count as failures."""
    result = StepResult(name, False, "not run")
    for attempt in range(1, retries + 1):
        started = time.monotonic()
        try:
            result = fn()
        except Exception as exc:
            result = StepResult(name, False, f"{type(exc).__name__}: {exc}"[:500])
        outcome = "skipped" if result.skipped else ("ok" if result.ok else "failed")
        _log(log, {
            "timestamp_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "job": "daily", "date": day.isoformat(), "venue": venue, "step": name,
            "attempt": attempt, "outcome": outcome,
            "seconds": round(time.monotonic() - started, 1), "detail": result.detail,
        })  # fmt: skip
        if result.ok or result.skipped:
            return result
        if attempt < retries:
            sleep(backoff_s * 2 ** (attempt - 1))
    return result


def daily_steps(
    venue: str,
    day: date,
    actions: dict[str, Callable[..., StepResult]],
    log: Path,
    sleep: Callable[[float], None] = time.sleep,
) -> list[StepResult]:
    """Run the venue's steps in order; stop at the first failure (later steps need it)."""
    done = []
    for name in ("compact", "backfill", "recompact", "load", "replay", "report"):
        if name not in actions:
            continue
        result = run_step(name, actions[name], log=log, day=day, venue=venue, sleep=sleep)
        done.append(result)
        if not (result.ok or result.skipped):
            break
    return done


def status_line(results: dict[str, list[StepResult]]) -> str:
    return orjson.dumps(
        {v: [(r.step, "skipped" if r.skipped else r.ok) for r in rs] for v, rs in results.items()}
    ).decode()
