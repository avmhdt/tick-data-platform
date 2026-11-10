from __future__ import annotations

import sys
from datetime import UTC, date, datetime
from pathlib import Path

import orjson
import pytest
from typer.testing import CliRunner

from tickdb import cli
from tickdb.capture import mirror

DAY = date(2026, 11, 8)
AFTER_DAY = datetime(2026, 11, 9, 0, 30, tzinfo=UTC)
RUN = "20261102T234621"
EXCL = {"binance-usdm": ("ticker",)}


def make_source(root: Path, files: dict[str, bytes]) -> Path:
    """files: 'venue/YYYY-MM-DD/name' -> content."""
    for key, content in files.items():
        venue, day, name = key.split("/")
        path = root / "raw" / "ws" / f"venue={venue}" / f"date={day}" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return root


@pytest.fixture
def source(tmp_path: Path) -> Path:
    return make_source(
        tmp_path / "src",
        {
            f"kraken/2026-11-08/00-ws.g20-{RUN}.jsonl.zst": b"a" * 3000,
            f"kraken/2026-11-08/23-control-{RUN}.jsonl.zst": b"c" * 10,
            f"binance-usdm/2026-11-08/05-ticker.g8-{RUN}.jsonl.zst": b"t" * 50,
            f"binance-usdm/2026-11-08/05-public.g7-{RUN}.jsonl.zst": b"p" * 70,
            f"binance-usdm/2026-11-07/05-public.g7-{RUN}.jsonl.zst": b"old",
            "binance-usdm/2026-11-08/notes.txt": b"ignored",
            f"binance-usdm/date-bad/00-ws.g1-{RUN}.jsonl.zst": b"x",
        },
    )


def run(
    source: Path,
    dest: Path,
    now: datetime = AFTER_DAY,
    guard_path: Path | None = None,
    floor_bytes: int = 0,
) -> mirror.MirrorResult:
    return mirror.mirror(
        source,
        dest,
        DAY,
        excludes=EXCL,
        now=lambda: now,
        limiter=mirror.RateLimiter(0),
        mem_probe=lambda: None,
        guard_path=guard_path,
        floor_bytes=floor_bytes,
    )


def test_copies_closed_files_byte_for_byte(source: Path, tmp_path: Path) -> None:
    dest = tmp_path / "dst"
    result = run(source, dest)
    assert result.copied == 3
    assert result.skipped_excluded == 1
    assert result.copied_bytes == 3000 + 10 + 70
    day_dir = dest / "raw/external/lob-alpha/venue=kraken/date=2026-11-08"
    assert (day_dir / f"00-ws.g20-{RUN}.jsonl.zst").read_bytes() == b"a" * 3000
    assert not list(dest.rglob("*.tmp"))
    assert not (dest / "raw/external/lob-alpha/venue=binance-usdm/date=2026-11-07").exists()
    lines = (dest / "meta/mirror/date=2026-11-08.jsonl").read_bytes().splitlines()
    records = [orjson.loads(line) for line in lines]
    assert {r["file"] for r in records} == {
        f"00-ws.g20-{RUN}.jsonl.zst",
        f"23-control-{RUN}.jsonl.zst",
        f"05-public.g7-{RUN}.jsonl.zst",
    }
    assert all(len(r["sha256"]) == 64 for r in records)


def test_second_run_is_a_no_op(source: Path, tmp_path: Path) -> None:
    dest = tmp_path / "dst"
    run(source, dest)
    again = run(source, dest)
    assert again.copied == 0
    assert again.skipped_existing == 3


def test_open_hours_wait_for_grace(source: Path, tmp_path: Path) -> None:
    # 23:05 on the day: hour 23 is still open; hours 00 and 05 closed long ago.
    result = run(source, tmp_path / "dst", now=datetime(2026, 11, 8, 23, 5, tzinfo=UTC))
    assert result.copied == 2
    assert result.skipped_open == 1
    # 00:05 next day: hour 23 ended 5 min ago, still inside the 10 min grace.
    result = run(source, tmp_path / "dst", now=datetime(2026, 11, 9, 0, 5, tzinfo=UTC))
    assert result.skipped_open == 1


def test_changed_size_is_recopied(source: Path, tmp_path: Path) -> None:
    dest = tmp_path / "dst"
    run(source, dest)
    victim = next(dest.rglob(f"00-ws.g20-{RUN}.jsonl.zst"))
    victim.write_bytes(b"short")
    result = run(source, dest)
    assert result.copied == 1
    assert victim.read_bytes() == b"a" * 3000


def test_disk_floor_stops_the_run(source: Path, tmp_path: Path) -> None:
    result = run(source, tmp_path / "dst", guard_path=tmp_path, floor_bytes=10**18)
    assert result.copied == 0
    assert "floor" in result.stopped


def test_memory_limit_stops_the_run(source: Path, tmp_path: Path) -> None:
    result = mirror.mirror(
        source,
        tmp_path / "dst",
        DAY,
        now=lambda: AFTER_DAY,
        limiter=mirror.RateLimiter(0),
        mem_probe=lambda: 1,
    )
    assert result.stopped == "free memory below the limit"


def test_vanished_and_unreadable_files_are_errors(
    source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(src: Path, dst: Path, limiter: mirror.RateLimiter) -> tuple[int, str]:
        raise OSError("disk error")

    monkeypatch.setattr(mirror, "copy_file", boom)
    result = run(source, tmp_path / "dst")
    assert len(result.errors) == 3
    assert result.copied == 0


def test_to_day_bounds_the_scan(source: Path) -> None:
    days = {f.day for f in mirror.scan(source, date(2026, 11, 1), date(2026, 11, 7))}
    assert days == {date(2026, 11, 7)}


def test_status_reports_missing_then_complete(source: Path, tmp_path: Path) -> None:
    dest = tmp_path / "dst"
    before = mirror.status(source, dest, DAY, excludes=EXCL, now=AFTER_DAY)
    assert [(r.venue, len(r.missing)) for r in before] == [("binance-usdm", 1), ("kraken", 2)]
    assert before[0].excluded == 1
    run(source, dest)
    after = mirror.status(source, dest, DAY, excludes=EXCL, now=AFTER_DAY)
    assert all(r.complete for r in after)


def test_rate_limiter_sleeps_when_ahead() -> None:
    clock = iter([0.0, 0.0, 0.5])
    slept: list[float] = []
    limiter = mirror.RateLimiter(100.0, clock=lambda: next(clock), sleep=slept.append)
    limiter.consume(100)  # 1 s of budget at t=0 -> sleep 1 s
    limiter.consume(100)  # 2 s of budget at t=0.5 -> sleep 1.5 s
    assert slept == [1.0, 1.5]


def test_parse_excludes() -> None:
    assert mirror.parse_excludes(["a:x", "a:y", "b:z"]) == {"a": ("x", "y"), "b": ("z",)}
    with pytest.raises(ValueError, match="VENUE:GROUP_PREFIX"):
        mirror.parse_excludes(["nocolon"])


def test_mem_available(tmp_path: Path) -> None:
    info = tmp_path / "meminfo"
    info.write_text("MemTotal: 10 kB\nMemAvailable:   2048 kB\n")
    assert mirror.mem_available_bytes(info) == 2048 * 1024
    info.write_text("MemTotal: 10 kB\n")
    assert mirror.mem_available_bytes(info) is None
    assert mirror.mem_available_bytes(tmp_path / "absent") is None


unix_only = pytest.mark.skipif(sys.platform == "win32", reason="flock is Unix-only")


@unix_only
def test_lock_is_exclusive(tmp_path: Path) -> None:
    lock = tmp_path / "x" / ".lock"
    with mirror.run_lock(lock) as first:
        assert first
        with mirror.run_lock(lock) as second:
            assert not second


@unix_only
def test_cli_run_and_status(source: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    dest = tmp_path / "dst"
    common = ["--source", str(source), "--dest", str(dest), "--from", "2026-11-08"]
    common += ["--exclude", "binance-usdm:ticker"]
    status = runner.invoke(cli.app, ["mirror", "status", *common])
    assert status.exit_code == cli.EXIT_MISMATCH
    assert "MISSING" in status.stdout
    result = runner.invoke(cli.app, ["mirror", "run", *common, "--rate-mb", "0", "--grace-s", "0"])
    assert result.exit_code == 0, result.output
    assert orjson.loads(result.stdout)["copied"] == 3
    status = runner.invoke(cli.app, ["mirror", "status", *common])
    assert status.exit_code == 0
    assert "MISSING" not in status.stdout


@unix_only
def test_cli_run_locked_and_stopped(source: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    dest = tmp_path / "dst"
    args = ["mirror", "run", "--source", str(source), "--dest", str(dest)]
    with mirror.run_lock(dest / "meta" / "mirror" / ".lock"):
        assert "holds the lock" in runner.invoke(cli.app, args).stdout
    stopped = runner.invoke(
        cli.app, [*args, "--guard-path", str(tmp_path), "--floor-gb", "1e12", "--grace-s", "0"]
    )
    assert stopped.exit_code == cli.EXIT_CONFIG


def test_cli_bad_date(tmp_path: Path) -> None:
    args = ["mirror", "status", "--from", "nope", "--dest", str(tmp_path)]
    assert CliRunner().invoke(cli.app, args).exit_code == cli.EXIT_CONFIG
