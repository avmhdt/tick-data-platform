"""Parquet archive -> ClickHouse (FR-11).

A venue-day is the unit: for each table, the day's Parquet partitions of every symbol are
inserted into a fresh staging table with the same structure, then swapped in with
``ALTER TABLE ... REPLACE PARTITION (date, venue) FROM staging``. Loading the same day twice
therefore leaves exactly one copy (AC-W-03). After the swap, a fingerprint per symbol
(row count and the wrap-around sum of the time column, both exact integers) is read back from
ClickHouse and compared with the same fingerprint computed from the Parquet files.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tickdb import schemas
from tickdb.compact.compactor import partition_dir

SCHEMA_SQL = (
    Path(__file__).resolve().parents[3] / "docker" / "clickhouse" / "init" / "001_schema.sql"
)
MASK64 = (1 << 64) - 1

# table -> (archive dataset, sub-directory inside the partition, fingerprint column)
TABLES: dict[str, tuple[str, str, str]] = {
    "trades": ("trades", "", "ts_recv"),
    "book_l2": ("book_l2", "", "ts_recv"),
    "bbo": ("bbo", "", "ts_recv"),
    "bars_1s": ("bars", "interval=1s", "ts"),
    "bars_1m": ("bars", "interval=1m", "ts"),
}


def schema_statements(sql: str | None = None) -> list[str]:
    text = sql if sql is not None else SCHEMA_SQL.read_text(encoding="utf-8")
    text = re.sub(r"--[^\n]*", "", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def apply_schema(client: Any, sql: str | None = None) -> int:
    statements = schema_statements(sql)
    for statement in statements:
        client.command(statement)
    return len(statements)


def _files(path: Path) -> list[Path]:
    return sorted(path.glob("*.parquet")) if path.exists() else []


def parquet_fingerprint(files: list[Path], column: str) -> tuple[int, int]:
    rows, total = 0, 0
    for file in files:
        table = pq.read_table(file, columns=[column])
        rows += table.num_rows
        if table.num_rows:
            total = (total + int(pc.sum(table[column].cast(pa.int64())).as_py() or 0)) & MASK64
    return rows, total


@dataclass
class LoadReport:
    venue: str
    day: date
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(t["ok"] for t in self.tables.values())


def load_venue_day(
    client: Any,
    data_root: Path,
    venue: str,
    day: date,
    symbols: tuple[str, ...],
    tables: tuple[str, ...] = tuple(TABLES),
    database: str = "tickdb",
    batch_rows: int = 1_000_000,
) -> LoadReport:
    report = LoadReport(venue, day)
    part = f"('{day.isoformat()}', '{venue}')"
    for table in tables:
        dataset, sub, fp_col = TABLES[table]
        schema = schemas.SCHEMAS[dataset]
        staging = f"{database}._stage_{table}_{uuid.uuid4().hex[:12]}"
        client.command(f"CREATE TABLE {staging} AS {database}.{table}")
        try:
            expected: dict[str, tuple[int, int]] = {}
            for symbol in symbols:
                path = partition_dir(data_root, dataset, venue, symbol, day)
                files = _files(path / sub if sub else path)
                expected[symbol] = parquet_fingerprint(files, fp_col)
                for file in files:
                    for batch in pq.ParquetFile(file).iter_batches(batch_size=batch_rows):
                        client.insert_arrow(staging, pa.Table.from_batches([batch], schema=schema))
            client.command(
                f"ALTER TABLE {database}.{table} REPLACE PARTITION {part} FROM {staging}"
            )
        finally:
            client.command(f"DROP TABLE IF EXISTS {staging}")
        got = {
            row[0]: (int(row[1]), int(row[2]) & MASK64)
            for row in client.query(
                f"SELECT symbol, count(), sumWithOverflow(toUInt64({fp_col})) "
                f"FROM {database}.{table} WHERE date = toDate('{day.isoformat()}') "
                f"AND venue = '{venue}' GROUP BY symbol"
            ).result_rows
        }
        checks = {s: {"parquet": expected[s], "clickhouse": got.get(s, (0, 0))} for s in symbols}
        report.tables[table] = {
            "rows": sum(v["parquet"][0] for v in checks.values()),
            "ok": all(v["parquet"] == v["clickhouse"] for v in checks.values()),
            "symbols": checks,
        }
    return report


def connect_settings(settings: Any, readonly: bool = False) -> Any:
    """clickhouse-connect client from Settings (the password never leaves SecretStr)."""
    import clickhouse_connect

    user = settings.clickhouse_readonly_user if readonly else settings.clickhouse_user
    secret = settings.clickhouse_readonly_password if readonly else settings.clickhouse_password
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_http_port,
        username=user,
        password=secret.get_secret_value() if secret else "",
    )
