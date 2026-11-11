"""Retention never selects an unverified day (AC-W-10); scheduler retries and order."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

from tickdb import retention, scheduler

TODAY = date(2026, 11, 30)


def _day(root: Path, rel: str, venue: str, day: str) -> Path:
    path = root / rel / f"venue={venue}" / f"date={day}"
    path.mkdir(parents=True)
    (path / "00-ws.g1-20261102T234621.jsonl.zst").write_bytes(b"x" * 10)
    return path


def test_retention_only_deletes_old_verified_days(tmp_path: Path) -> None:
    old_ok = _day(tmp_path, "raw/external/lob-alpha", "kraken", "2026-11-12")
    old_bad = _day(tmp_path, "raw/external/lob-alpha", "kraken", "2026-11-13")
    old_unchecked = _day(tmp_path, "raw/backfill", "kraken", "2026-11-14")
    recent = _day(tmp_path, "raw/external/lob-alpha", "kraken", "2026-11-25")
    retention.record_verify(tmp_path, "kraken", date(2026, 11, 12), {"ok": True})
    retention.record_verify(tmp_path, "kraken", date(2026, 11, 13), {"ok": False})
    retention.record_verify(tmp_path, "kraken", date(2026, 11, 25), {"ok": True})
    dry = retention.apply(tmp_path, 7, dry_run=True, today=TODAY)
    assert dry.delete == [old_ok]
    assert sorted(dry.keep_unverified) == ["kraken 2026-11-13", "kraken 2026-11-14"]
    assert old_ok.exists()  # a dry run deletes nothing
    retention.apply(tmp_path, 7, dry_run=False, today=TODAY)
    assert not old_ok.exists()
    assert old_bad.exists()
    assert old_unchecked.exists()
    assert recent.exists()
    assert retention.retired(tmp_path, "kraken", date(2026, 11, 12))
    assert not retention.retired(tmp_path, "kraken", date(2026, 11, 13))


def test_mirror_never_recopies_a_retired_day(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    from tests.unit.test_mirror import DAY, make_source
    from tickdb.capture import mirror

    source = make_source(
        tmp_path / "src", {"kraken/2026-11-08/00-ws.g1-20261102T234621.jsonl.zst": b"a"}
    )
    tomb = tmp_path / "dst" / "meta" / "retention" / "venue=kraken" / "date=2026-11-08.json"
    tomb.parent.mkdir(parents=True)
    tomb.write_text("{}")
    result = mirror.mirror(
        source, tmp_path / "dst", DAY, now=lambda: datetime(2026, 11, 9, 1, tzinfo=UTC),
        limiter=mirror.RateLimiter(0), mem_probe=lambda: None,
    )  # fmt: skip
    assert result.copied == 0


def test_run_step_retries_then_succeeds(tmp_path: Path) -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def flaky() -> scheduler.StepResult:
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("transient")
        return scheduler.StepResult("x", True, "done")

    log = tmp_path / "job_log.csv"
    result = scheduler.run_step("x", flaky, log=log, day=TODAY, venue="kraken", sleep=slept.append)
    assert result.ok
    assert slept == [30.0, 60.0]
    lines = log.read_text().splitlines()
    assert lines[0].startswith("timestamp_utc,job,date")
    assert [line.split(",")[6] for line in lines[1:]] == ["failed", "failed", "ok"]


def test_daily_steps_stop_at_first_failure(tmp_path: Path) -> None:
    order: list[str] = []

    def step(name: str, ok: bool, skipped: bool = False) -> object:
        def run() -> scheduler.StepResult:
            order.append(name)
            return scheduler.StepResult(name, ok, skipped=skipped)

        return run

    actions = {
        "compact": step("compact", True),
        "backfill": step("backfill", True),
        "load": step("load", False, skipped=True),
        "replay": step("replay", False),
        "report": step("report", True),
    }
    results = scheduler.daily_steps(
        "kraken", TODAY, actions, tmp_path / "log.csv", sleep=lambda s: None
    )  # type: ignore[arg-type]
    assert order == ["compact", "backfill", "load", "replay", "replay", "replay"]
    assert [r.step for r in results] == ["compact", "backfill", "load", "replay"]
    assert '"kraken"' in scheduler.status_line({"kraken": results})


@pytest.mark.skipif(sys.platform == "win32", reason="flock is Unix-only")
def test_job_lock(tmp_path: Path) -> None:
    with scheduler.job_lock(tmp_path / ".lock") as first:
        assert first
        with scheduler.job_lock(tmp_path / ".lock") as second:
            assert not second
