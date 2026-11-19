"""DQ checks DQ-05 to DQ-10 (FR-17) on synthetic compacted days and hand-made frames."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import orjson
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.unit.test_normalize import DAY, binance_day, kraken_day
from tickdb import schemas
from tickdb.compact.compactor import compact_day
from tickdb.dq import daily
from tickdb.timeutil import day_bounds

START, _ = day_bounds(DAY)
S = 1_000_000_000


@pytest.fixture(scope="module")
def archive(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("dq")
    raw = binance_day(tmp / "raw")
    kraken_day(raw)
    data = tmp / "data"
    compact_day(raw, data, "binance-usdm", DAY, ("BTCUSDT",))
    compact_day(raw, data, "kraken", DAY, ("BTC/USD",))
    return data


def test_day_rows_cover_every_dataset(archive: Path, tmp_path: Path) -> None:
    result = daily.check_day(archive, "binance-usdm", DAY, ("BTCUSDT",), shared_root=tmp_path)
    assert result.daily.schema == schemas.DQ_DAILY
    rows = {r["dataset"]: r for r in result.daily.to_pylist()}
    assert set(rows) == {"trades", "book_l2", "bbo"}
    trades = rows["trades"]
    assert trades["rows"] == 3
    assert trades["dup_rows_removed"] == 1  # the fixture repeats one aggTrade id
    assert result.notes["dup_trades_remaining"] == {"BTCUSDT": 0}
    assert trades["content_hash"]
    assert trades["latency_p50_ms"] is not None
    assert rows["bbo"]["latency_p50_ms"] is None
    assert 0 < trades["coverage_pct"] < 1  # a few seconds of data in one minute
    assert all(r["status"] == "fail" for r in rows.values())  # coverage far below 95 %
    assert result.notes["clock_source"] == "none"
    assert result.latency.schema == daily.DQ_LATENCY
    assert result.latency.num_rows == 48  # 24 hours x (trades, book_l2)
    assert result.notes["book_dup_check"] == {"BTCUSDT": "ordered scan"}


def test_write_read_and_client_round_trip(archive: Path, tmp_path: Path) -> None:
    from tickdb.client import connect
    from tickdb.config import Settings

    result = daily.check_day(archive, "kraken", DAY, ("BTC/USD",), shared_root=tmp_path)
    paths = daily.write_day(archive, result)
    assert set(paths) == {"dq_daily", "dq_outliers", "dq_latency"}
    back = daily.read_table(archive, "dq_daily", DAY, DAY)
    assert back.num_rows >= 3
    db = connect("parquet", settings=Settings(_env_file=None), root=str(archive))  # type: ignore[call-arg]
    assert db.dq_daily("2026-11-09", "2026-11-10").filter(pl.col("venue") == "kraken").height == 3
    notes = orjson.loads((paths["dq_daily"].with_suffix(".json")).read_bytes())
    assert "unreliable_hours" in notes


def _trades(prices: list[float], *, t0: int = START, step: int = S) -> pl.DataFrame:
    n = len(prices)
    return pl.DataFrame(
        {
            "ts_exchange": [t0 + i * step for i in range(n)],
            "ts_recv": [t0 + i * step + 1000 for i in range(n)],
            "trade_id": [str(i) for i in range(n)],
            "price": prices,
            "qty": [1.0] * n,
            "source": ["ws"] * n,
        }
    )


def _bbo(times: list[int], bid: float, ask: float, resync: bool = False) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ts": times,
            "ts_recv": times,
            "seq": list(range(len(times))),
            "bid_px": [bid] * len(times),
            "ask_px": [ask] * len(times),
            "in_resync": [resync] * len(times),
        }
    )


NO_BBO = _bbo([], 0.0, 0.0)


def test_rolling_mad_needs_both_mad_and_bps() -> None:
    prices = [100.0, 100.01, 99.99, 100.0, 100.02, 99.98] * 5
    spike = [*prices, 101.0, *prices]
    out = daily.flag_outliers(_trades(spike), NO_BBO, None, daily.Limits())
    assert out["rule"].to_list() == ["rolling_mad"]
    assert out["price"].to_list() == [101.0]
    assert out["deviation_bps"][0] == pytest.approx(100.0, rel=1e-3)
    small = [*prices, 100.4, *prices]  # 40 bps: many MADs away but under 50 bps
    assert daily.flag_outliers(_trades(small), NO_BBO, None, daily.Limits()).height == 0


def test_tick_and_non_positive_rules() -> None:
    out = daily.flag_outliers(_trades([100.0, 100.05, 0.0]), NO_BBO, 0.1, daily.Limits())
    out = out.filter(pl.col("rule") != "rolling_mad")  # 0.0 is also far from the median
    assert sorted(zip(out["price"], out["rule"], strict=True)) == [
        (0.0, "non_positive"),
        (100.05, "tick"),
    ]
    # float noise from decimal parsing is not off the grid (relative tolerance 1e-9)
    clean = daily.flag_outliers(_trades([64566.8, 0.3 * 3]), NO_BBO, 0.1, daily.Limits())
    assert clean.height == 0


def test_outside_bbo_uses_only_live_quotes() -> None:
    trades = _trades([100.0, 102.0, 102.0], step=20 * S)
    live = _bbo([START + 19 * S], 99.9, 100.1)
    out = daily.flag_outliers(trades, live, None, daily.Limits())
    flagged = out.filter(pl.col("rule") == "outside_bbo")
    assert flagged["trade_id"].to_list() == ["1"]  # trade 2's last quote is 21 s old
    assert flagged["ref_price"].to_list() == [100.1]
    resync = _bbo([START + 19 * S], 99.9, 100.1, resync=True)
    assert (
        daily.flag_outliers(trades, resync, None, daily.Limits())
        .filter(pl.col("rule") == "outside_bbo")
        .is_empty()
    )


def test_clock_offsets_prefer_ntp(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    probe_dir = shared / "clock" / "venue=kraken" / f"date={DAY.isoformat()}"
    probe_dir.mkdir(parents=True)
    probes = pa.table(
        {
            "ts_recv": [START + i * 60 * S for i in range(6)],
            "venue": ["kraken"] * 6,
            "server_time_ns": [0] * 6,
            "rtt_ns": [100_000_000] * 5 + [9_000_000_000],
            "offset_ns": [10_000_000, 12_000_000, 11_000_000, 9_000_000, 10_000_000, 4_000_000_000],
        }
    )
    pq.write_table(probes, probe_dir / "part-00.parquet")
    hours, source = daily.clock_offsets(tmp_path / "data", shared, "kraken", DAY)
    assert source == "venue_time_probe"
    assert hours["offset_ms"].to_list() == [pytest.approx(10.0)]  # the 9 s round trip is dropped
    ntp = tmp_path / "data" / "meta" / "clock" / f"date={DAY.isoformat()}.jsonl"
    ntp.parent.mkdir(parents=True)
    ntp.write_bytes(
        b'{"ts_ns": %d, "offset_ms": -3.0}\n{"ts_ns": %d, "offset_ms": 70.0}\n'
        % (START, START + 2 * 3600 * S)
    )
    hours, source = daily.clock_offsets(tmp_path / "data", shared, "kraken", DAY)
    assert source == "ntp"
    assert dict(zip(hours["hour"], hours["offset_ms"], strict=True)) == {0: 3.0, 2: 70.0}


def test_maintenance_minutes(tmp_path: Path) -> None:
    doc = tmp_path / "maintenance.md"
    doc.write_text(
        "| Venue | Start UTC | End UTC | Source |\n|---|---|---|---|\n"
        "| kraken | 2026-11-09 23:50 | 2026-11-10 00:30 | https://status.kraken.com |\n",
        encoding="utf-8",
    )
    assert daily.maintenance_minutes("kraken", DAY, doc) == set(range(1430, 1440))
    assert daily.maintenance_minutes("kraken", date(2026, 11, 10), doc) == set(range(30))
    assert daily.maintenance_minutes("coinbase", DAY, doc) == set()


def test_acceptance_counts_classified_and_recovered(tmp_path: Path) -> None:
    from tickdb.dq import acceptance

    def gap(channel: str, status: str, secs: int, gid: str) -> dict[str, object]:
        return {
            "gap_id": gid, "venue": "kraken", "symbol": "BTC/USD", "channel": channel,
            "kind": "disconnect", "start_recv": START, "end_recv": START + secs * S,
            "start_exchange": None, "end_exchange": None, "seq_from": None, "seq_to": None,
            "recovery": "rest_backfill", "status": status, "recovered_rows": 0,
        }  # fmt: skip

    rows = [gap("trades", "recovered", 90, "a"), gap("trades", "partial", 10, "b"),
            gap("trades", "unrecoverable", 50, "c"), gap("book_l2", "partial", 30, "d")]  # fmt: skip
    path = tmp_path / "meta" / "gaps" / "venue=kraken" / f"date={DAY.isoformat()}"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schemas.GAPS), path)
    notes = tmp_path / "meta" / "dq_daily" / f"date={DAY.isoformat()}"
    notes.mkdir(parents=True)
    (notes / "venue=kraken.json").write_bytes(orjson.dumps({"dup_trades_remaining": {"x": 0}}))
    comp = tmp_path / "meta" / "compaction" / "venue=kraken"
    comp.mkdir(parents=True)
    (comp / f"date={DAY.isoformat()}.json").write_text("{}")
    out = acceptance.evaluate(tmp_path, DAY, DAY)
    assert out["classified_pct"] == 100.0
    assert out["trade_gap_seconds_covered"] == 100.0  # the unrecoverable 50 s are outside
    assert out["trade_gap_recovered_pct"] == 90.0
    assert out["ac_w02"]["recovered_ge_95pct"] is False
    assert out["ac_w02"]["zero_duplicates"] is True
    assert out["ac_w02"]["vision_ge_3_days"] is False
    assert out["dq_tables_daily"] is True


def test_uncompacted_day_is_refused(archive: Path, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not compacted"):
        daily.check_day(archive, "coinbase", DAY, ("BTC-USD",), shared_root=tmp_path)
