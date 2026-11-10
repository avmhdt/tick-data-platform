"""Read-only mirror of the shared capture's raw archive (owner decision 2026-11-10).

crypto-lob-alpha's ``lob-recorder`` is the one live recorder of binance-usdm, coinbase and
kraken on this PC. Its raw archive keeps every websocket frame and control record as
``ts_recv\\tpayload`` lines in hourly zstd files, but only for 7 days. This module copies each
closed hour, byte for byte, into this project's data root before that cleanup, so the
capture window's raw input survives for compaction (M3) and replay checks (AC-W-03).

Source layout (read-only): ``<source>/raw/ws/venue=<v>/date=<D>/<HH>-<group>-<run>.jsonl.zst``.
Destination: ``<dest>/raw/external/lob-alpha/venue=<v>/date=<D>/<same name>``, plus one
manifest line per copied file in ``<dest>/meta/mirror/date=<D>.jsonl`` (bytes and SHA-256).

Rules: only hours that closed at least ``grace_s`` ago are copied; a file whose copy already
has the same size is skipped; copies go to ``.tmp`` and are renamed after fsync; reads are
rate-limited; the run stops before D: falls under the floor or free memory under 30 GB; one
run at a time (file lock).
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import orjson

FILE_RE = re.compile(r"^(?P<hour>\d{2})-(?P<group>.+)-(?P<run>\d{8}T\d{6})\.jsonl\.zst$")
DEST_SUBDIR = Path("raw") / "external" / "lob-alpha"
CHUNK = 1 << 20
GB = 1_000_000_000


@dataclass(frozen=True)
class SourceFile:
    venue: str
    day: date
    hour: int
    group: str
    path: Path

    @property
    def hour_end(self) -> datetime:
        start = datetime(self.day.year, self.day.month, self.day.day, tzinfo=UTC)
        return start + timedelta(hours=self.hour + 1)


@dataclass
class MirrorResult:
    copied: int = 0
    copied_bytes: int = 0
    skipped_existing: int = 0
    skipped_open: int = 0
    skipped_excluded: int = 0
    stopped: str = ""
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "copied": self.copied,
            "copied_bytes": self.copied_bytes,
            "skipped_existing": self.skipped_existing,
            "skipped_open": self.skipped_open,
            "skipped_excluded": self.skipped_excluded,
            "stopped": self.stopped,
            "errors": self.errors,
        }


def parse_excludes(items: list[str]) -> dict[str, tuple[str, ...]]:
    """``["binance-usdm:ticker"]`` -> ``{"binance-usdm": ("ticker",)}`` (group-name prefixes)."""
    out: dict[str, list[str]] = {}
    for item in items:
        venue, _, prefix = item.partition(":")
        if not venue or not prefix:
            raise ValueError(f"bad exclude {item!r}; use VENUE:GROUP_PREFIX")
        out.setdefault(venue, []).append(prefix)
    return {k: tuple(v) for k, v in out.items()}


def scan(source: Path, from_day: date, to_day: date | None = None) -> Iterator[SourceFile]:
    """Every raw file of the shared archive with ``from_day <= date <= to_day``."""
    root = source / "raw" / "ws"
    for venue_dir in sorted(root.glob("venue=*")):
        venue = venue_dir.name.removeprefix("venue=")
        for day_dir in sorted(venue_dir.glob("date=*")):
            try:
                day = date.fromisoformat(day_dir.name.removeprefix("date="))
            except ValueError:
                continue
            if day < from_day or (to_day is not None and day > to_day):
                continue
            for path in sorted(day_dir.iterdir()):
                match = FILE_RE.match(path.name)
                if match is None or not path.is_file():
                    continue
                yield SourceFile(venue, day, int(match["hour"]), match["group"], path)


def dest_path(dest: Path, src: SourceFile) -> Path:
    return dest / DEST_SUBDIR / f"venue={src.venue}" / f"date={src.day.isoformat()}" / src.path.name


class RateLimiter:
    """Sleeps so that reads average at most ``bytes_per_s``."""

    def __init__(
        self,
        bytes_per_s: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.rate = bytes_per_s
        self.clock = clock
        self.sleep = sleep
        self.start = clock()
        self.done = 0

    def consume(self, n: int) -> None:
        self.done += n
        if self.rate <= 0:
            return
        ahead = self.done / self.rate - (self.clock() - self.start)
        if ahead > 0:
            self.sleep(ahead)


def copy_file(src: Path, dst: Path, limiter: RateLimiter) -> tuple[int, str]:
    """Copy ``src`` to ``dst`` atomically; return (bytes, sha256 hex)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    digest = hashlib.sha256()
    total = 0
    with src.open("rb") as fin, tmp.open("wb") as fout:
        while chunk := fin.read(CHUNK):
            fout.write(chunk)
            digest.update(chunk)
            total += len(chunk)
            limiter.consume(len(chunk))
        fout.flush()
        os.fsync(fout.fileno())
    os.replace(tmp, dst)
    return total, digest.hexdigest()


def mem_available_bytes(meminfo: Path = Path("/proc/meminfo")) -> int | None:
    """``MemAvailable`` in bytes, or None where /proc/meminfo is missing."""
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


@contextmanager
def run_lock(path: Path) -> Iterator[bool]:
    """Non-blocking exclusive lock; yields False if another run holds it."""
    import fcntl  # Unix only; the mirror runs in WSL

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


def _manifest(dest: Path, src: SourceFile, nbytes: int, sha: str, now: datetime) -> None:
    path = dest / "meta" / "mirror" / f"date={src.day.isoformat()}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    line = {
        "venue": src.venue,
        "date": src.day.isoformat(),
        "file": src.path.name,
        "bytes": nbytes,
        "sha256": sha,
        "copied_utc": now.isoformat(timespec="seconds"),
    }
    with path.open("ab") as handle:
        handle.write(orjson.dumps(line) + b"\n")


def mirror(
    source: Path,
    dest: Path,
    from_day: date,
    *,
    to_day: date | None = None,
    excludes: dict[str, tuple[str, ...]] | None = None,
    grace_s: float = 600.0,
    rate_bytes_per_s: float = 8e6,
    guard_path: Path | None = None,
    floor_bytes: int = 150 * GB,
    min_mem_bytes: int = 30 * GB,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    limiter: RateLimiter | None = None,
    mem_probe: Callable[[], int | None] = mem_available_bytes,
) -> MirrorResult:
    """Copy every closed, not-yet-copied hour file from ``from_day`` on."""
    excludes = excludes or {}
    result = MirrorResult()
    limiter = limiter or RateLimiter(rate_bytes_per_s)
    for src in scan(source, from_day, to_day):
        if src.group.startswith(excludes.get(src.venue, ())):
            result.skipped_excluded += 1
            continue
        if now() < src.hour_end + timedelta(seconds=grace_s):
            result.skipped_open += 1
            continue
        dst = dest_path(dest, src)
        try:
            size = src.path.stat().st_size
        except FileNotFoundError:  # pruned by the recorder between scan and copy
            result.errors.append(f"{src.path}: vanished before copy")
            continue
        if dst.exists() and dst.stat().st_size == size:
            result.skipped_existing += 1
            continue
        if guard_path is not None and shutil.disk_usage(guard_path).free - size < floor_bytes:
            result.stopped = f"free space on {guard_path} would fall below the floor"
            break
        mem = mem_probe()
        if mem is not None and mem < min_mem_bytes:
            result.stopped = "free memory below the limit"
            break
        try:
            nbytes, sha = copy_file(src.path, dst, limiter)
        except OSError as exc:
            result.errors.append(f"{src.path}: {exc}")
            continue
        _manifest(dest, src, nbytes, sha, now())
        result.copied += 1
        result.copied_bytes += nbytes
    return result


@dataclass(frozen=True)
class DayStatus:
    venue: str
    day: date
    source_files: int
    closed_files: int
    copied_files: int
    missing: tuple[str, ...]
    excluded: int

    @property
    def complete(self) -> bool:
        return not self.missing and self.closed_files == self.source_files


def status(
    source: Path,
    dest: Path,
    from_day: date,
    *,
    to_day: date | None = None,
    excludes: dict[str, tuple[str, ...]] | None = None,
    grace_s: float = 600.0,
    now: datetime | None = None,
) -> list[DayStatus]:
    """Per venue and day: closed source files and which of them have a same-size copy."""
    excludes = excludes or {}
    moment = now or datetime.now(UTC)
    groups: dict[tuple[str, date], list[SourceFile]] = {}
    excluded: dict[tuple[str, date], int] = {}
    for src in scan(source, from_day, to_day):
        key = (src.venue, src.day)
        if src.group.startswith(excludes.get(src.venue, ())):
            excluded[key] = excluded.get(key, 0) + 1
            continue
        groups.setdefault(key, []).append(src)
    out = []
    for (venue, day), files in sorted(groups.items()):
        closed = [f for f in files if moment >= f.hour_end + timedelta(seconds=grace_s)]
        missing = []
        for f in closed:
            dst = dest_path(dest, f)
            if not dst.exists() or dst.stat().st_size != f.path.stat().st_size:
                missing.append(f.path.name)
        out.append(
            DayStatus(
                venue,
                day,
                len(files),
                len(closed),
                len(closed) - len(missing),
                tuple(missing),
                excluded.get((venue, day), 0),
            )
        )
    return out
