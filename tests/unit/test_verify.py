"""tickdb verify: an independent raw path (DuckDB over decoded frames) against the archive."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.unit.test_normalize import DAY, binance_day, coinbase_day, kraken_day
from tickdb import verify
from tickdb.compact.compactor import compact_day, partition_dir

CASES = [
    ("binance-usdm", "BTCUSDT", binance_day),
    ("coinbase", "BTC-USD", coinbase_day),
    ("kraken", "BTC/USD", kraken_day),
]


@pytest.mark.parametrize(("venue", "symbol", "make"), CASES)
def test_raw_path_matches_parquet(tmp_path: Path, venue: str, symbol: str, make: object) -> None:
    raw = make(tmp_path / "raw")  # type: ignore[operator]
    data = tmp_path / "data"
    compact_day(raw, data, venue, DAY, (symbol,))
    checks = verify.verify_day([raw], data, venue, DAY, (symbol,))
    result = verify.report(checks)
    assert result["ok"], result
    assert result["total"] == 2  # aggregates and 1 m OHLCV against Parquet


def test_tampered_archive_is_caught(tmp_path: Path) -> None:
    raw = binance_day(tmp_path / "raw")
    data = tmp_path / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    part = next(partition_dir(data, "trades", "binance-usdm", "BTCUSDT", DAY).glob("*.parquet"))
    table = pq.read_table(part)
    prices = table.column("price").to_pylist()
    prices[1] += 1e-6  # one trade's price off by a hair
    import pyarrow as pa

    pq.write_table(
        table.set_column(table.schema.get_field_index("price"), "price", pa.array(prices)), part
    )
    result = verify.report(verify.verify_day([raw], data, "binance-usdm", DAY, ("BTCUSDT",)))
    assert not result["ok"]
    assert {c["name"] for c in result["checks"] if not c["ok"]} == {
        "BTCUSDT trades aggregates raw=parquet",
        "BTCUSDT 1m OHLCV raw=parquet",
    }


def test_close_and_rows_equal() -> None:
    assert verify.close(1.0, 1.0 + 1e-13)
    assert not verify.close(1.0, 1.0 + 1e-9)
    assert verify.close(None, None)
    assert not verify.close(1.0, None)
    assert verify.rows_equal([(1, 2.0)], [(1, 2.0)])
    assert not verify.rows_equal([(1, 2.0)], [(2, 2.0)])
    assert not verify.rows_equal([(1,)], [])


def test_experiment_log(tmp_path: Path) -> None:
    path = tmp_path / "log.csv"
    verify.log_experiment("tickdb verify", {"date": "x"}, {"passed": 1}, True, path)
    verify.log_experiment("tickdb verify", {"date": "y"}, {"passed": 0}, False, path)
    lines = path.read_text().splitlines()
    assert lines[0].startswith("timestamp_utc,git_sha,command")
    assert lines[1].endswith(",ok")
    assert lines[2].endswith(",mismatch")
