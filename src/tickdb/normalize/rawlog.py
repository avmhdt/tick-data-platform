"""Reader for the shared capture's raw archive (the mirrored spool, spec 7d ``raw/external``).

Files: ``<root>/venue=<v>/date=<D>/<HH>-<conn>-<run>.jsonl.zst``, one zstd stream per hour per
connection (or ``control``). Lines are ``ts_recv\\tpayload`` for websocket frames and
``ts_recv\\t!<json>\\t<body>`` for control records (connection events, REST bodies, clock
probes, the hourly ``state`` record). A file whose writer died has no frame footer; every
complete line is still read.

Records of one hour are merged across files by ``(ts_recv, conn, line)``, which is the
recorder's own replay order, so normalizing the immutable files always yields the same rows.
"""

from __future__ import annotations

import heapq
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import orjson

from tickdb.ingest.spool import _decode

FILE_RE = re.compile(r"^(?P<hour>\d{2})-(?P<conn>.+)-(?P<run>\d{8}T\d{6})\.jsonl\.zst$")
CONTROL = "control"


@dataclass(slots=True)
class RawRecord:
    ts: int
    conn: str  # connection id, e.g. "public.g526", or "control"
    line: int
    payload: bytes  # frame text, or the REST body of a control record
    ctrl: dict[str, Any] | None = None


def _lines(path: Path) -> Iterator[bytes]:
    pending = b""
    for out in _decode(path):
        if out is None:
            break
        pending += out
        *lines, pending = pending.split(b"\n")
        yield from lines


def read_file(path: Path) -> Iterator[RawRecord]:
    match = FILE_RE.match(path.name)
    if match is None:
        return
    conn = match["conn"]
    for i, line in enumerate(_lines(path)):
        stamp, _, rest = line.partition(b"\t")
        if not rest or not stamp.isdigit():
            continue
        if conn == CONTROL:
            meta, _, body = rest.removeprefix(b"!").partition(b"\t")
            yield RawRecord(int(stamp), conn, i, body, orjson.loads(meta))
        else:
            yield RawRecord(int(stamp), conn, i, rest)


def hour_files(root: Path, venue: str, day: date, hour: int) -> list[Path]:
    base = root / f"venue={venue}" / f"date={day.isoformat()}"
    return sorted(base.glob(f"{hour:02d}-*.jsonl.zst"))


def day_records(root: Path, venue: str, day: date) -> Iterator[RawRecord]:
    """Every record of a UTC day in replay order, one hour at a time (bounded memory)."""
    for hour in range(24):
        streams = [read_file(p) for p in hour_files(root, venue, day, hour)]
        yield from heapq.merge(*streams, key=lambda r: (r.ts, r.conn, r.line))
