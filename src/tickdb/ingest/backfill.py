"""REST backfill of trade gaps (FR-05, DQ-06).

For every trades gap of a compacted day that is still ``open`` or ``partial`` and has a known
missing id range (``seq_from``..``seq_to``), fetch exactly that range from the venue's public
REST API, rate-limited to at most 50 % of the published limits (NFR-14), and spool each
response verbatim as a control record ``{"k": "rest_backfill", ...}`` under
``raw/backfill/venue=<v>/date=<D>/``, in the shared capture's line format. Re-compacting the
day turns them into ``source = 'rest'`` rows; dedupe keeps any websocket copy (DQ-03), and the
gap is classified recovered, partial or unrecoverable.

Endpoints (checked 2026-11-11):
- binance-usdm ``GET /fapi/v1/aggTrades?symbol&fromId&limit=1000`` (weight 20 of 2400/min):
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Compressed-Aggregate-Trades-List
- coinbase ``GET /products/{id}/trades?after=<id>&limit=1000`` (pages backwards from ``after``):
  https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades
- kraken ``GET /0/public/Trades?pair&since=<ns>&count=1000``:
  https://docs.kraken.com/api/docs/rest-api/get-recent-trades
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import orjson
import pyarrow.parquet as pq
import zstandard

from tickdb.ingest.ratelimit import TokenBucket
from tickdb.smoke import USER_AGENT, kraken_rest_pair

MAX_PAGES = 2_000
RATES = {"binance-usdm": (20.0, 40.0), "coinbase": (5.0, 5.0), "kraken": (0.5, 1.0)}
WEIGHT = {"binance-usdm": 20.0, "coinbase": 1.0, "kraken": 1.0}


@dataclass(frozen=True)
class Need:
    symbol: str
    lo: int  # first missing trade id
    hi: int  # last missing trade id
    start_ns: int  # gap start (receive time), for venues that page by time


def plan(data_root: Path, venue: str, day: date) -> list[Need]:
    path = data_root / "meta" / "gaps" / f"venue={venue}" / f"date={day.isoformat()}"
    if not path.exists():
        return []
    needs = []
    for row in pq.read_table(path).to_pylist():
        if row["channel"] != "trades" or row["status"] not in ("open", "partial"):
            continue
        if row["seq_from"] is None or row["seq_to"] is None or row["seq_to"] < row["seq_from"]:
            continue
        needs.append(Need(row["symbol"], row["seq_from"], row["seq_to"], row["start_recv"]))
    return needs


def _ids(venue: str, body: Any) -> list[int]:
    if venue == "binance-usdm":
        return [int(t["a"]) for t in body]
    if venue == "coinbase":
        return [int(t["trade_id"]) for t in body]
    result = body.get("result", {})
    rows: list[list[Any]] = next((v for k, v in result.items() if k != "last"), [])
    return [int(r[6]) for r in rows]


class Backfiller:
    def __init__(
        self,
        venue: str,
        rest_url: str,
        http: httpx.AsyncClient | None = None,
        bucket: TokenBucket | None = None,
    ) -> None:
        self.venue = venue
        self.rest = rest_url.rstrip("/")
        self.http = http or httpx.AsyncClient(headers={"User-Agent": USER_AGENT}, timeout=10.0)
        self.bucket = bucket or TokenBucket(*RATES[venue])
        self.records: list[bytes] = []
        self.requests = 0
        self.errors: list[str] = []

    def _spool(self, need: Need, ctrl: dict[str, Any], body: bytes, fetched: int) -> None:
        ctrl = {"k": "rest_backfill", "symbol": need.symbol, "need": [need.lo, need.hi], **ctrl}
        line = b"%d\t!%s\t%s" % (fetched, orjson.dumps(ctrl), body.replace(b"\n", b" "))
        self.records.append(line)

    async def _get(self, url: str, params: dict[str, Any], need: Need) -> Any:
        await self.bucket.acquire(WEIGHT[self.venue])
        response = await self.http.get(url, params=params)
        fetched = time.time_ns()
        self.requests += 1
        response.raise_for_status()
        ctrl = {
            "k": "rest_backfill",
            "symbol": need.symbol,
            "endpoint": url.removeprefix(self.rest),
            "params": params,
            "need": [need.lo, need.hi],
        }
        body = response.content.replace(b"\n", b" ")
        self.records.append(b"%d\t!%s\t%s" % (fetched, orjson.dumps(ctrl), body))
        return orjson.loads(response.content)

    async def fetch(self, need: Need) -> int:
        """Fetch ``need.lo..need.hi``; return how many ids of that range were received. A venue
        rejection (e.g. Binance only searches the last two days) is recorded, not raised."""
        try:
            return await self._fetch(need)
        except httpx.HTTPStatusError as exc:
            self.errors.append(f"{need.symbol} {need.lo}-{need.hi}: {exc.response.text[:200]}")
            return 0

    def vision(self, need: Need, data_root: Path, days: list[date]) -> int:
        """binance-usdm fallback: the missing ids from data.binance.vision daily files."""
        from tickdb.ingest import binance_vision as bv

        got: list[dict[str, Any]] = []
        for day in days:
            try:
                path = bv.fetch(data_root, need.symbol, day)
            except (httpx.HTTPError, ValueError) as exc:
                self.errors.append(f"binance-vision {need.symbol} {day}: {exc}"[:300])
                continue
            got += bv.rows(path, need.lo, need.hi)
        if got:
            ctrl = {
                "source": "vendor",
                "endpoint": "binance-vision",
                "files": [str(d) for d in days],
            }
            self._spool(need, ctrl, orjson.dumps(got), time.time_ns())
        return len({r["a"] for r in got})

    async def _fetch(self, need: Need) -> int:
        got: set[int] = set()
        if self.venue == "binance-usdm":
            next_id = need.lo
            for _ in range(MAX_PAGES):
                params = {"symbol": need.symbol, "fromId": next_id, "limit": 1000}
                ids = _ids(
                    self.venue, await self._get(f"{self.rest}/fapi/v1/aggTrades", params, need)
                )
                got |= {i for i in ids if need.lo <= i <= need.hi}
                if not ids or max(ids) >= need.hi:
                    break
                next_id = max(ids) + 1
        elif self.venue == "coinbase":
            after = need.hi + 1
            for _ in range(MAX_PAGES):
                url = f"{self.rest}/products/{need.symbol}/trades"
                ids = _ids(self.venue, await self._get(url, {"after": after, "limit": 1000}, need))
                got |= {i for i in ids if need.lo <= i <= need.hi}
                if not ids or min(ids) <= need.lo:
                    break
                after = min(ids)
        else:
            since = need.start_ns
            for _ in range(MAX_PAGES):
                params = {"pair": kraken_rest_pair(need.symbol), "since": since, "count": 1000}
                body = await self._get(f"{self.rest}/0/public/Trades", params, need)
                if body.get("error"):
                    break
                ids = _ids(self.venue, body)
                got |= {i for i in ids if need.lo <= i <= need.hi}
                last = int(body.get("result", {}).get("last", since))
                if not ids or max(ids) >= need.hi or last <= since:
                    break
                since = last
        return len(got)

    def write(self, data_root: Path, day: date) -> Path | None:
        """Spool the responses for ``day`` (one zstd file per run); None if nothing was fetched."""
        if not self.records:
            return None
        base = data_root / "raw" / "backfill" / f"venue={self.venue}" / f"date={day.isoformat()}"
        base.mkdir(parents=True, exist_ok=True)
        run = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        path = base / f"23-backfill.{uuid.uuid4().hex[:6]}-{run}.jsonl.zst"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(zstandard.ZstdCompressor().compress(b"\n".join(self.records) + b"\n"))
        tmp.replace(path)
        return path
