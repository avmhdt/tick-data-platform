"""Coinbase REST checkpoints against the rebuilt book, aligned by sequence (DQ-04, AC-W-05).

The spec compares a Coinbase checkpoint with the book at the response's receive time,
excluding levels updated within +-500 ms. Measured on 2026-11-09 that cannot work: the REST
book is typically about 1 s stale (median 0.99 s, 90th percentile 2.1 s), so a receive-time
comparison matched 0 of 143 checkpoints while every one matched once older updates were
excluded. ``level2_batch`` carries no sequence, but the REST book's ``sequence`` and the
``heartbeat`` channel's per-product ``sequence`` (once a second) are the same counter. So each
checkpoint is aligned to the first heartbeat at or past its sequence, ``t*``, and compared
with the book at ``t*`` on its top 20 levels per side, excluding levels updated in
``[t* - window, t*]``. The heartbeat resolution is 1 s, hence ``window = 1 s``; 0.5 s and 1.5 s
are reported beside it (docs/decisions.md, M3).

Two passes over the day: one collects heartbeats and checkpoints, one replays the book.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import orjson

from tickdb.ingest.book import ASK, BID
from tickdb.normalize.coinbase import CoinbaseNormalizer
from tickdb.normalize.engine import Engine
from tickdb.normalize.rawlog import day_records

WINDOWS_S = (0.5, 1.0, 1.5)
DEPTH = 20


def _collect(raw_roots: list[Path], day: date, symbols: tuple[str, ...]) -> Any:
    beats: dict[str, tuple[list[int], list[int]]] = {s: ([], []) for s in symbols}
    ckpts: dict[str, list[tuple[int, dict[str, Any]]]] = {s: [] for s in symbols}
    for rec in day_records(raw_roots, "coinbase", day):
        if rec.ctrl is not None:
            if rec.ctrl.get("k") == "checkpoint" and rec.ctrl.get("symbol") in ckpts:
                ckpts[str(rec.ctrl["symbol"])].append((rec.ts, orjson.loads(rec.payload)))
        elif b'"type":"heartbeat"' in rec.payload:
            msg = orjson.loads(rec.payload)
            if msg.get("product_id") in beats:
                seqs, times = beats[msg["product_id"]]
                seqs.append(int(msg["sequence"]))
                times.append(rec.ts)
    return beats, ckpts


def check_day(
    raw_roots: list[Path],
    carry: dict[str, dict[str, Any]],
    day: date,
    symbols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    beats, ckpts = _collect(raw_roots, day, symbols)
    due: list[tuple[int, str, dict[str, Any]]] = []
    unaligned: dict[str, int] = defaultdict(int)
    for symbol in symbols:
        seqs, times = beats[symbol]
        for _ts, body in ckpts[symbol]:
            i = bisect.bisect_left(seqs, int(body.get("sequence", -1)))
            if i < len(seqs):
                due.append((times[i], symbol, body))
            else:
                unaligned[symbol] += 1
    due.sort(key=lambda d: d[0])
    engine = Engine("coinbase", day, symbols, lambda *a: None, carry=carry)
    norm = CoinbaseNormalizer(engine)
    touches: dict[tuple[str, int, float], list[int]] = defaultdict(list)
    update = norm.update

    def recording(symbol: str, tse: int, tsr: int, conn: str, levels: Any) -> bool:
        ok = update(symbol, tse, tsr, conn, levels)
        for side, price, _ in levels:
            touches[(symbol, side, float(price))].append(tsr)
        return ok

    norm.update = recording  # type: ignore[method-assign,assignment]
    out: dict[str, dict[str, Any]] = {
        s: {"checkpoints": len(ckpts[s]), "unaligned": unaligned[s], "skipped": 0}
        | {f"match_{w}s": 0 for w in WINDOWS_S}
        for s in symbols
    }
    k = 0
    for rec in day_records(raw_roots, "coinbase", day):
        norm.feed(rec)
        while k < len(due) and rec.ts >= due[k][0]:
            t, symbol, body = due[k]
            k += 1
            if symbol not in norm.known:
                out[symbol]["skipped"] += 1
                continue
            book = norm.books[symbol]
            ours = {
                side: {float(p): float(q) for p, q in book.top(side, 400)} for side in (BID, ASK)
            }
            for w in WINDOWS_S:
                start = t - int(w * 1e9)
                ok = True
                for side, key in ((BID, "bids"), (ASK, "asks")):
                    for p, q, *_ in body.get(key, [])[:DEPTH]:
                        price = float(p)
                        hits = touches.get((symbol, side, price), [])
                        j = bisect.bisect_left(hits, start)
                        if j < len(hits) and hits[j] <= t:
                            continue
                        if ours[side].get(price) != float(q):
                            ok = False
                            break
                    if not ok:
                        break
                out[symbol][f"match_{w}s"] += ok
    for v in out.values():
        compared = v["checkpoints"] - v["unaligned"] - v["skipped"]
        v["compared"] = compared
        v["match_pct_1.0s"] = round(100 * v["match_1.0s"] / compared, 2) if compared else None
    return out


def carry_for(data_root: Path, day: date) -> dict[str, dict[str, Any]]:
    from tickdb.compact.compactor import load_book_end

    return load_book_end(data_root, "coinbase", day - timedelta(days=1))
