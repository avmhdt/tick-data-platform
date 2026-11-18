"""AC-W-02: the archived Binance USD-M aggregate trade ids of a day equal the ids in
data.binance.vision's daily aggTrades file.

The file holds the trades whose exchange time falls on the UTC day; the archive is
partitioned by receive time (DQ-02), so the archive side takes the trades with ``ts_exchange``
on the day from the partitions of D and D+1 (a trade late on D can arrive after midnight).
Every source counts (websocket, REST backfill, vendor). A day whose trade gaps were filled from
this same file is circular and is reported as such.
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from tickdb.compact.compactor import partition_dir
from tickdb.dq.report import load_gaps
from tickdb.ingest import binance_vision
from tickdb.timeutil import day_bounds


@dataclass
class VisionCheck:
    date: str
    symbol: str
    archive_ids: int
    vision_ids: int
    missing_in_archive: int
    extra_in_archive: int
    first_missing: list[int]
    first_extra: list[int]
    vendor_rows: int
    circular: bool
    next_day_archived: bool

    @property
    def equal(self) -> bool:
        return self.missing_in_archive == 0 and self.extra_in_archive == 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self) | {"equal": self.equal}


def vision_ids(path: Path) -> set[int]:
    out: set[int] = set()
    with zipfile.ZipFile(path) as zf, zf.open(zf.namelist()[0]) as raw:
        for rec in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
            if rec and rec[0].isdigit():
                out.add(int(rec[0]))
    return out


def archive_ids(data_root: Path, symbol: str, day: date) -> tuple[set[int], int, bool]:
    """(ids with ts_exchange on ``day``, rows from the vendor, whether D+1 is archived)."""
    start, end = day_bounds(day)
    frames = []
    nxt = False
    for d in (day, day + timedelta(days=1)):
        files = sorted(
            partition_dir(data_root, "trades", "binance-usdm", symbol, d).glob("*.parquet")
        )
        nxt = nxt or (d != day and bool(files))
        if files:
            frames.append(pl.read_parquet(files, columns=["trade_id", "ts_exchange", "source"]))
    if not frames:
        return set(), 0, nxt
    df = pl.concat(frames).filter((pl.col("ts_exchange") >= start) & (pl.col("ts_exchange") < end))
    vendor = df.filter(pl.col("source") == "vendor").height
    return set(df["trade_id"].cast(pl.Int64).to_list()), vendor, nxt


def check(data_root: Path, day: date, symbol: str = "BTCUSDT") -> VisionCheck:
    path = binance_vision.fetch(data_root, symbol, day)
    want = vision_ids(path)
    have, vendor, nxt = archive_ids(data_root, symbol, day)
    circular = any(
        g["venue"] == "binance-usdm" and g["symbol"] == symbol and g["recovery"] == "binance_vision"
        for g in load_gaps(data_root, day)
    )
    missing = sorted(want - have)
    extra = sorted(have - want)
    return VisionCheck(
        day.isoformat(), symbol, len(have), len(want), len(missing), len(extra),
        missing[:10], extra[:10], vendor, circular, nxt,
    )  # fmt: skip
