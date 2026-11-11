"""Daily compaction (FR-09): raw records of a venue-day -> canonical Parquet partitions.

Rows stream from the normalizer into one Parquet writer per (dataset, symbol) under
``interim/staging/<run>/``, with the spec's writer settings (zstd 3, dictionary strings,
statistics, 1,000,000-row groups, files capped near 512 MB), and into a content hasher
(FR-10). When the day is done each partition gets ``_manifest.json`` and is swapped into
``<dataset>/venue=<v>/symbol=<sym>/date=<D>/`` by directory rename; the previous version is
removed only after the swap. ``gaps`` and the run's counters go to ``meta/``.
"""

from __future__ import annotations

import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from tickdb import __version__, schemas
from tickdb.compact.hashing import StreamHasher
from tickdb.normalize import NORMALIZERS
from tickdb.normalize.engine import Engine
from tickdb.normalize.rawlog import day_records

ROW_GROUP = 1_000_000
FILE_ROWS = 40_000_000  # about 512 MB at the measured bytes/row of book_l2
STRING_COLUMNS = {
    name: [f.name for f in schema if pa.types.is_string(f.type)]
    for name, schema in schemas.SCHEMAS.items()
}


def partition_dir(root: Path, dataset: str, venue: str, symbol: str, day: date) -> Path:
    """``<dataset>/venue=<v>/symbol=<uri-encoded>/date=<D>`` (spec 7b: BTC/USD -> BTC%2FUSD)."""
    return (
        root
        / dataset
        / f"venue={venue}"
        / f"symbol={quote(symbol, safe='')}"
        / f"date={day.isoformat()}"
    )


@dataclass
class _Part:
    dataset: str
    symbol: str
    staging: Path
    hasher: StreamHasher
    writer: pq.ParquetWriter | None = None
    file_index: int = 0
    file_rows: int = 0
    rows: int = 0

    def write(self, batch: pa.RecordBatch, schema: pa.Schema) -> None:
        if self.writer is None or self.file_rows >= FILE_ROWS:
            if self.writer is not None:
                self.writer.close()
                self.file_index += 1
            self.staging.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                self.staging / f"part-{self.file_index:04d}.parquet",
                schema,
                compression="zstd",
                compression_level=3,
                use_dictionary=STRING_COLUMNS[self.dataset],
                write_statistics=True,
            )
            self.file_rows = 0
        self.writer.write_batch(batch, row_group_size=ROW_GROUP)
        self.file_rows += batch.num_rows
        self.rows += batch.num_rows
        self.hasher.update(batch)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()


@dataclass
class CompactResult:
    venue: str
    day: date
    partitions: dict[str, dict[str, Any]] = field(default_factory=dict)  # key "dataset/symbol"
    counters: dict[str, int] = field(default_factory=dict)
    gaps: int = 0
    seconds: float = 0.0
    raw_files: list[str] = field(default_factory=list)


def _raw_files(raw_root: Path, venue: str, day: date) -> list[str]:
    base = raw_root / f"venue={venue}" / f"date={day.isoformat()}"
    return sorted(p.name for p in base.glob("*.jsonl.zst"))


def load_book_end(root: Path, venue: str, day: date) -> dict[str, dict[str, Any]]:
    """The end-of-day books of ``day`` (the next day's carry), or {} if not compacted."""
    path = (
        root / "meta" / "book_end" / f"venue={venue}" / f"date={day.isoformat()}" / "book_end.json"
    )
    if not path.exists():
        return {}
    data: dict[str, dict[str, Any]] = orjson.loads(path.read_bytes())
    return data


def normalize_to(
    raw_root: Path,
    out_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    *,
    kraken_depth: int = 1000,
    carry_root: Path | None = None,
) -> CompactResult:
    """Normalize a venue-day into partitions under ``out_root`` (staging or scratch)."""
    started = time.monotonic()
    parts: dict[tuple[str, str], _Part] = {}

    def sink(dataset: str, symbol: str, batch: pa.RecordBatch) -> None:
        key = (dataset, symbol)
        part = parts.get(key)
        if part is None:
            part = _Part(
                dataset,
                symbol,
                partition_dir(out_root, dataset, venue, symbol, day),
                StreamHasher(schemas.SCHEMAS[dataset]),
            )
            parts[key] = part
        part.write(batch, schemas.SCHEMAS[dataset])

    carry = load_book_end(carry_root or out_root, venue, day - timedelta(days=1))
    engine = Engine(venue, day, symbols, sink, carry=carry)
    cls = NORMALIZERS[venue]
    normalizer = cls(engine, kraken_depth) if venue == "kraken" else cls(engine)  # type: ignore[call-arg]
    for record in day_records(raw_root, venue, day):
        normalizer.feed(record)
    engine.finish()
    end_dir = out_root / "meta" / "book_end" / f"venue={venue}" / f"date={day.isoformat()}"
    end_dir.mkdir(parents=True, exist_ok=True)
    (end_dir / "book_end.json").write_bytes(orjson.dumps(normalizer.book_end()))
    for part in parts.values():
        part.close()
    result = CompactResult(venue, day, counters=dict(engine.counters))
    result.raw_files = _raw_files(raw_root, venue, day)
    for (dataset, symbol), part in sorted(parts.items()):
        result.partitions[f"{dataset}/{symbol}"] = {
            "dataset": dataset,
            "venue": venue,
            "symbol": symbol,
            "date": day.isoformat(),
            "rows": part.rows,
            "content_hash": part.hasher.hexdigest(),
            "tickdb_version": __version__,
            "spool_files": result.raw_files,
        }
    gaps = pa.Table.from_pylist([g.row() for g in engine.gaps], schema=schemas.GAPS)
    gap_dir = out_root / "meta" / "gaps" / f"venue={venue}" / f"date={day.isoformat()}"
    gap_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(gaps, gap_dir / "part-0000.parquet", compression="zstd")
    result.gaps = gaps.num_rows
    result.seconds = round(time.monotonic() - started, 1)
    return result


def _swap(staging: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    old = final.with_name(final.name + f".old-{uuid.uuid4().hex[:8]}")
    if final.exists():
        os.replace(final, old)
    os.replace(staging, final)
    if old.exists():
        shutil.rmtree(old)


def compact_day(
    raw_root: Path,
    data_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    *,
    kraken_depth: int = 1000,
) -> CompactResult:
    """Normalize into staging, write manifests, then swap every partition into the archive."""
    run = f"{venue}-{day.isoformat()}-{uuid.uuid4().hex[:8]}"
    staging = data_root / "interim" / "staging" / run
    result = normalize_to(
        raw_root, staging, venue, day, symbols, kraken_depth=kraken_depth, carry_root=data_root
    )
    created = datetime.now(UTC).isoformat(timespec="seconds")
    for manifest in result.partitions.values():
        src = partition_dir(staging, manifest["dataset"], venue, manifest["symbol"], day)
        (src / "_manifest.json").write_bytes(
            orjson.dumps({**manifest, "created_utc": created}, option=orjson.OPT_INDENT_2)
        )
        _swap(src, partition_dir(data_root, manifest["dataset"], venue, manifest["symbol"], day))
    for meta in ("gaps", "book_end"):
        sub = Path("meta") / meta / f"venue={venue}" / f"date={day.isoformat()}"
        _swap(staging / sub, data_root / sub)
    run_log = data_root / "meta" / "compaction" / f"venue={venue}"
    run_log.mkdir(parents=True, exist_ok=True)
    (run_log / f"date={day.isoformat()}.json").write_bytes(
        orjson.dumps(
            {
                "venue": venue,
                "date": day.isoformat(),
                "seconds": result.seconds,
                "gaps": result.gaps,
                "counters": result.counters,
                "partitions": {
                    k: {"rows": v["rows"], "content_hash": v["content_hash"]}
                    for k, v in result.partitions.items()
                },
                "created_utc": created,
            },
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        )
    )
    shutil.rmtree(staging, ignore_errors=True)
    return result


def replay_verify(
    raw_root: Path,
    data_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    *,
    keep_scratch: bool = False,
    kraken_depth: int = 1000,
) -> dict[str, Any]:
    """Rebuild a day into scratch and compare content hashes with the archive (FR-10)."""
    scratch = data_root / "interim" / "replay" / f"{venue}-{day.isoformat()}-{uuid.uuid4().hex[:8]}"
    result = normalize_to(
        raw_root, scratch, venue, day, symbols, kraken_depth=kraken_depth, carry_root=data_root
    )
    compared = {}
    for key, manifest in result.partitions.items():
        path = partition_dir(data_root, manifest["dataset"], venue, manifest["symbol"], day)
        stored = path / "_manifest.json"
        archived = orjson.loads(stored.read_bytes())["content_hash"] if stored.exists() else None
        compared[key] = {
            "rows": manifest["rows"],
            "replay": manifest["content_hash"],
            "archive": archived,
            "equal": archived == manifest["content_hash"],
        }
    if not keep_scratch:
        shutil.rmtree(scratch, ignore_errors=True)
    return {
        "venue": venue,
        "date": day.isoformat(),
        "seconds": result.seconds,
        "partitions": compared,
        "ok": bool(compared) and all(v["equal"] for v in compared.values()),
    }
