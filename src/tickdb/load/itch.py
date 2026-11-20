"""ITCH bulk-load (FR-25, AC-H-06): itch-feed-handler's canonical Parquet -> the archive.

itch-feed-handler parses the Nasdaq TotalView-ITCH 5.0 file and writes per-symbol ``book_l3``
and ``trades`` partitions (``<src>/<dataset>/venue=XNAS/symbol=S/date=D/``). This module
doesn't parse ITCH (D-09); it maps that output to tickdb's canonical schemas:

- ``book_l3``: the canonical columns. The producer's ``msg_type``, ``tracking_number``,
  ``mpid`` and ``printable`` are left out.
- ``trades``: printable trades only. Non-printable executions aren't tape volume, and the
  canonical schema has no flag for them. ``source = 'import'``; the ITCH message type and
  cross type are left out.

A historical file has no receive time, so ``ts_recv = ts_exchange`` (DQ-07): latency
statistics leave these rows out. Rows are ordered by (``ts_recv``, the producer's ``seq``)
and partitioned by the UTC date of ``ts_recv`` (DQ-02). Nasdaq's post-market runs to
20:00 ET, so the last hours of an ITCH day land in the next UTC date's partition. Partitions
are written to staging and swapped in atomically, like compaction.

Cross-check against itch-feed-handler: the imported trade count equals the producer's
printable trade rows; per symbol, the imported quantity equals ``trade_volume`` in the
producer's build report (``reports/build_D.json``).
"""

from __future__ import annotations

import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import orjson
import polars as pl

from tickdb import schemas
from tickdb.compact.compactor import _manifest, _publish, _write, partition_dir

VENUE = "XNAS"
NS_DAY = 86_400_000_000_000


@dataclass
class ImportReport:
    day: date
    symbols: list[str] = field(default_factory=list)
    rows: dict[str, int] = field(default_factory=dict)
    dates: list[str] = field(default_factory=list)
    seconds: float = 0.0
    bytes_in: dict[str, int] = field(default_factory=dict)
    bytes_out: dict[str, int] = field(default_factory=dict)
    producer_printable_trades: int = 0
    non_printable_skipped: int = 0
    volume_mismatches: dict[str, list[int]] = field(default_factory=dict)
    report_symbols_missing: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.rows.get("trades", -1) == self.producer_printable_trades
            and not self.volume_mismatches
            and not self.report_symbols_missing
        )

    def as_dict(self) -> dict[str, Any]:
        out = {k: v for k, v in self.__dict__.items()}
        out["day"] = self.day.isoformat()
        total = sum(self.rows.values())
        out["rows_per_s"] = round(total / self.seconds) if self.seconds else None
        out["bytes_per_row"] = {
            ds: round(self.bytes_out[ds] / self.rows[ds], 2) for ds in self.rows if self.rows[ds]
        }
        out["ok"] = self.ok
        return out


def symbols_in(src: Path, day: date) -> list[str]:
    base = src / "trades" / f"venue={VENUE}"
    return sorted(
        unquote(p.parent.name.removeprefix("symbol="))
        for p in base.glob(f"symbol=*/date={day.isoformat()}")
    )


def _src_files(src: Path, dataset: str, symbol: str, day: date) -> list[Path]:
    return sorted(
        (src / dataset / f"venue={VENUE}" / f"symbol={symbol}" / f"date={day}").glob("*.parquet")
    )


def _utc_day(ns: int) -> date:
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).date()


def canonical_trades(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """(printable trades in canonical form and archive order, non-printable rows dropped)."""
    keep = df.filter(pl.col("printable"))
    out = keep.sort("ts_recv", "seq").select(
        "ts_exchange", "ts_recv", "venue", "symbol", "trade_id", "price", "qty", "side",
        pl.lit("import").alias("source"), pl.lit(None, pl.Int64).alias("ts_fetch"),
    )  # fmt: skip
    return out, df.height - keep.height


def canonical_book_l3(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort("ts_recv", "seq").select(schemas.BOOK_L3.names)


def _split_by_day(df: pl.DataFrame) -> dict[date, pl.DataFrame]:
    if df.is_empty():
        return {}
    keyed = df.with_columns((pl.col("ts_recv") // NS_DAY).alias("_d"))
    return {
        _utc_day(int(k[0]) * NS_DAY): part.drop("_d")
        for k, part in keyed.partition_by("_d", as_dict=True, maintain_order=True).items()
    }


def import_day(
    src: Path, data_root: Path, day: date, symbols: list[str] | None = None
) -> ImportReport:
    rep = ImportReport(day)
    rep.symbols = symbols or symbols_in(src, day)
    build = src / "reports" / f"build_{day.isoformat()}.json"
    volumes = orjson.loads(build.read_bytes()).get("symbols", {}) if build.exists() else {}
    staging = data_root / "interim" / "staging" / f"itch-{uuid.uuid4().hex[:8]}"
    partitions: dict[str, dict[str, Any]] = {}
    days: set[date] = set()
    started = time.perf_counter()
    for symbol in rep.symbols:
        for dataset in ("trades", "book_l3"):
            files = _src_files(src, dataset, symbol, day)
            rep.bytes_in[dataset] = rep.bytes_in.get(dataset, 0) + sum(
                f.stat().st_size for f in files
            )
            raw = pl.read_parquet(files) if files else None
            if raw is None:
                continue
            if dataset == "trades":
                df, skipped = canonical_trades(raw)
                rep.producer_printable_trades += raw.filter(pl.col("printable")).height
                rep.non_printable_skipped += skipped
                want = volumes.get(symbol, {}).get("trade_volume")
                got = int(df["qty"].sum())
                if want is None:
                    rep.report_symbols_missing.append(symbol)
                elif int(want) != got:
                    rep.volume_mismatches[symbol] = [got, int(want)]
            else:
                df = canonical_book_l3(raw)
            schema = schemas.SCHEMAS[dataset]
            for d, part_df in _split_by_day(df).items():
                days.add(d)
                rel = partition_dir(Path("."), dataset, VENUE, symbol, d)
                table = part_df.to_arrow().cast(schema)
                part = _write(
                    staging / rel, dataset, symbol, table.to_batches(max_chunksize=1_000_000)
                )
                partitions[f"{dataset}/{symbol}/{d}"] = _manifest(
                    part, dataset, VENUE, symbol, d, rel
                )
                rep.rows[dataset] = rep.rows.get(dataset, 0) + part.rows
                rep.bytes_out[dataset] = rep.bytes_out.get(dataset, 0) + sum(
                    f.stat().st_size for f in (staging / rel).glob("*.parquet")
                )
    _publish(staging, data_root, partitions)
    rep.seconds = round(time.perf_counter() - started, 1)
    rep.dates = sorted(d.isoformat() for d in days)
    meta = data_root / "meta" / "import" / "itch"
    meta.mkdir(parents=True, exist_ok=True)
    parts = {
        k: {"rows": v["rows"], "content_hash": v["content_hash"]} for k, v in partitions.items()
    }
    body = rep.as_dict() | {"source": str(src), "partitions": parts}
    (meta / f"date={day.isoformat()}.json").write_bytes(
        orjson.dumps(body, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    )
    shutil.rmtree(staging, ignore_errors=True)  # partitions were swapped out of it
    return rep
