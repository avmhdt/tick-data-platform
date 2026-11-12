"""Bar math (FR-13): property tests against a brute-force reference."""

from __future__ import annotations

import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

from tickdb.compact.bars import compute_bars, resample_bars

S = 1_000_000_000
T0 = 1_794_182_400 * S  # 2026-11-09 00:00 UTC

trade = st.tuples(
    st.integers(0, 600 * S - 1),  # within the first 10 minutes
    st.sampled_from([100.0, 100.5, 101.0, 99.5]),
    st.floats(0.001, 5.0, allow_nan=False),
    st.sampled_from([1, -1, 0]),
)


def frame(rows: list[tuple[int, float, float, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "venue": ["kraken"] * len(rows),
            "symbol": ["BTC/USD"] * len(rows),
            "ts_exchange": [T0 + r[0] for r in rows],
            "trade_id": [str(i) for i in range(len(rows))],
            "price": [r[1] for r in rows],
            "qty": [r[2] for r in rows],
            "side": [r[3] for r in rows],
        },
        schema_overrides={"side": pl.Int8},
    )


@settings(max_examples=60, deadline=None)
@given(st.lists(trade, min_size=1, max_size=200))
def test_resampled_seconds_equal_minutes(rows: list[tuple[int, float, float, int]]) -> None:
    df = frame(rows)
    secs = pl.from_arrow(compute_bars(df, "1s", T0, T0 + 86_400 * S))
    mins = pl.from_arrow(compute_bars(df, "1m", T0, T0 + 86_400 * S))
    again = resample_bars(secs, "1m")  # type: ignore[arg-type]
    assert again.select(["ts", "open", "high", "low", "close", "n_trades"]).equals(
        mins.select(["ts", "open", "high", "low", "close", "n_trades"])  # type: ignore[union-attr]
    )
    for col in ("volume", "buy_volume", "sell_volume", "notional"):
        a, b = again[col].to_list(), mins[col].to_list()  # type: ignore[index]
        assert all(abs(x - y) <= 1e-9 * max(1.0, abs(y)) for x, y in zip(a, b, strict=True))


@settings(max_examples=60, deadline=None)
@given(st.lists(trade, min_size=1, max_size=200))
def test_minute_bars_match_brute_force(rows: list[tuple[int, float, float, int]]) -> None:
    bars = compute_bars(frame(rows), "1m", T0, T0 + 86_400 * S).to_pylist()
    ordered = sorted(enumerate(rows), key=lambda x: (x[1][0], x[0]))  # (ts, trade id)
    buckets: dict[int, list[tuple[float, float, int]]] = {}
    for _, (ts, px, qty, side) in ordered:
        buckets.setdefault(T0 + ts - ts % (60 * S), []).append((px, qty, side))
    assert [b["ts"] for b in bars] == sorted(buckets)
    for bar in bars:
        group = buckets[bar["ts"]]
        assert bar["open"] == group[0][0]
        assert bar["close"] == group[-1][0]
        assert bar["high"] == max(g[0] for g in group)
        assert bar["n_trades"] == len(group)
        vol = sum(g[1] for g in group)
        assert abs(bar["volume"] - vol) < 1e-9 * max(1.0, vol)
        assert abs(bar["buy_volume"] - sum(g[1] for g in group if g[2] == 1)) < 1e-9 * max(1.0, vol)
        assert abs(bar["vwap"] - sum(g[0] * g[1] for g in group) / vol) < 1e-9 * 101


def test_window_bounds_and_empty() -> None:
    df = frame([(10 * S, 100.0, 1.0, 1)])
    assert compute_bars(df, "1s", T0 + 11 * S, T0 + 86_400 * S).num_rows == 0
    assert compute_bars(pl.DataFrame(), "1m", T0, T0 + S).num_rows == 0
