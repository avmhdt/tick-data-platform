"""As-of join equals a brute-force O(n*m) reference on random series (FR-14, AC-W-07)."""

from __future__ import annotations

import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

from tickdb.query.asof import asof_bruteforce, asof_polars

times = st.integers(0, 60)  # small range: many ties and exact matches


@st.composite
def series(draw: st.DrawFn) -> tuple[pl.DataFrame, pl.DataFrame]:
    n, m = draw(st.integers(0, 25)), draw(st.integers(0, 25))
    t_ex = draw(st.lists(times, min_size=n, max_size=n))
    t_rv = [t + draw(st.integers(0, 5)) for t in t_ex]
    trades = pl.DataFrame(
        {
            "ts_exchange": t_ex,
            "ts_recv": t_rv,
            "venue": ["kraken"] * n,
            "symbol": ["BTC/USD"] * n,
            "trade_id": [str(i) for i in range(n)],
            "price": draw(
                st.lists(st.sampled_from([99.0, 100.0, 100.5, 101.0]), min_size=n, max_size=n)
            ),
            "qty": [1.0] * n,
            "side": [1] * n,
        },
        schema={
            "ts_exchange": pl.Int64, "ts_recv": pl.Int64, "venue": pl.String,
            "symbol": pl.String, "trade_id": pl.String, "price": pl.Float64,
            "qty": pl.Float64, "side": pl.Int8,
        },
    )  # fmt: skip
    q_ts = draw(st.lists(times, min_size=m, max_size=m))
    quotes = pl.DataFrame(
        {
            "ts": q_ts,
            "ts_recv": [t + draw(st.integers(0, 5)) for t in q_ts],
            "seq": list(range(m)),
            "bid_px": draw(st.lists(st.sampled_from([99.0, 99.5, 100.0]), min_size=m, max_size=m)),
            "bid_qty": [1.0] * m,
            "ask_px": draw(st.lists(st.sampled_from([100.5, 101.0]), min_size=m, max_size=m)),
            "ask_qty": [2.0] * m,
            "in_resync": draw(st.lists(st.booleans(), min_size=m, max_size=m)),
        },
        schema={
            "ts": pl.Int64, "ts_recv": pl.Int64, "seq": pl.Int64, "bid_px": pl.Float64,
            "bid_qty": pl.Float64, "ask_px": pl.Float64, "ask_qty": pl.Float64,
            "in_resync": pl.Boolean,
        },
    )  # fmt: skip
    return trades, quotes


@settings(max_examples=300, deadline=None)
@given(
    series(),
    st.sampled_from(["exchange", "recv"]),
    st.booleans(),
    st.sampled_from([None, 0, 3, 10]),
)
def test_asof_equals_bruteforce(
    data: tuple[pl.DataFrame, pl.DataFrame], on: str, exact: bool, tol: int | None
) -> None:
    trades, quotes = data
    got = asof_polars(trades, quotes, on=on, allow_exact_matches=exact, tolerance_ns=tol)  # type: ignore[arg-type]
    want = asof_bruteforce(trades, quotes, on=on, allow_exact_matches=exact, tolerance_ns=tol)  # type: ignore[arg-type]
    assert got.columns == want.columns
    assert got.to_dicts() == want.to_dicts()


def test_semantics_by_hand() -> None:
    trades = pl.DataFrame(
        {"ts_exchange": [10, 20, 30], "ts_recv": [11, 21, 31], "venue": ["v"] * 3,
         "symbol": ["s"] * 3, "trade_id": ["1", "2", "3"], "price": [100.2, 99.0, 101.0],
         "qty": [1.0] * 3, "side": [1, -1, 1]},
        schema_overrides={"side": pl.Int8},
    )  # fmt: skip
    quotes = pl.DataFrame(
        {"ts": [10, 15, 15, 29], "ts_recv": [12, 16, 17, 30], "seq": [1, 2, 3, 4],
         "bid_px": [99.0, 99.5, 99.8, 100.0], "bid_qty": [1.0] * 4,
         "ask_px": [101.0, 100.5, 100.4, 100.6], "ask_qty": [1.0] * 4,
         "in_resync": [False, False, False, True]},
    )  # fmt: skip
    out = asof_polars(trades, quotes, on="exchange", allow_exact_matches=False, tolerance_ns=8)
    rows = out.to_dicts()
    assert rows[0]["quote_ts"] is None  # the quote at ts=10 is not strictly before
    assert rows[1]["quote_ts"] == 15
    assert rows[1]["bid_px"] == 99.8  # of two quotes at the same ts, the last one wins
    assert rows[1]["sign"] == -1  # 99.0 is below the mid 100.1
    assert rows[2]["quote_ts"] is None  # 29 is in resync; 15 is older than the tolerance
    exact = asof_polars(trades, quotes, allow_exact_matches=True, tolerance_ns=None).to_dicts()
    assert exact[0]["quote_ts"] == 10
    assert exact[0]["mid"] == 100.0
    assert exact[0]["sign"] == 1
    assert exact[0]["spread"] == 2.0
    assert exact[0]["quote_age_ns"] == 0
