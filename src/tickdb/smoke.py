"""Data-access smoke test (FR-32): each enabled venue's websocket for N seconds plus one REST
snapshot. It reports reachability, geo-blocks (HTTP 403/451) and auth requirements, and writes
nothing to the archive.

It opens one short-lived connection per websocket route. It is a reachability check, not a
recorder: nothing it receives is kept (owner decision on the one shared capture).

Endpoint references (checked 2026-11-09):

- Binance USD-M: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
  and .../market-data/rest-api/Order-Book
- Coinbase Exchange: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
  and https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book
- Kraken: https://docs.kraken.com/api/docs/websocket-v2/book,
  https://docs.kraken.com/api/docs/websocket-v2/trade and
  https://docs.kraken.com/api/docs/rest-api/get-order-book
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from tickdb import __version__
from tickdb.config import VenueConfig

USER_AGENT = f"tickdb/{__version__} (+https://github.com/avmhdt/tick-data-platform)"
GEO_BLOCK_STATUS = frozenset({403, 451})
AUTH_STATUS = frozenset({401})
DATA_CHANNELS = ("trades", "book_l2")

WsPlan = list[tuple[str, list[str]]]
RestPlan = tuple[str, dict[str, str | int]]


@dataclass
class VenueSmoke:
    """Outcome for one venue."""

    venue: str
    channels: list[str]
    frames: Counter[str] = field(default_factory=Counter)
    ws_errors: list[str] = field(default_factory=list)
    first_frame_ms: float | None = None
    rest_url: str = ""
    rest_status: int | None = None
    rest_bytes: int = 0
    rest_error: str = ""
    geo_blocked: bool = False
    auth_required: bool = False

    @property
    def ws_ok(self) -> bool:
        wanted = [c for c in self.channels if c in DATA_CHANNELS]
        return not self.ws_errors and all(self.frames[c] > 0 for c in wanted)

    @property
    def rest_ok(self) -> bool:
        return self.rest_status == 200 and not self.rest_error

    @property
    def ok(self) -> bool:
        return self.ws_ok and self.rest_ok and not self.geo_blocked and not self.auth_required

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "ok": self.ok,
            "ws_ok": self.ws_ok,
            "rest_ok": self.rest_ok,
            "frames": dict(self.frames),
            "ws_errors": self.ws_errors,
            "first_frame_ms": self.first_frame_ms,
            "rest_url": self.rest_url,
            "rest_status": self.rest_status,
            "rest_bytes": self.rest_bytes,
            "rest_error": self.rest_error,
            "geo_blocked": self.geo_blocked,
            "auth_required": self.auth_required,
        }


# ---------------------------------------------------------------- frame classification


def classify_binance(msg: dict[str, Any]) -> str:
    stream = str(msg.get("stream", ""))
    if stream.endswith("@aggTrade") or stream.endswith("@trade"):
        return "trades"
    if "@depth" in stream:
        return "book_l2"
    if "error" in msg or "code" in msg:
        return "error"
    return "control"


def classify_coinbase(msg: dict[str, Any]) -> str:
    kind = msg.get("type")
    if kind in ("match", "last_match"):
        return "trades"
    if kind in ("snapshot", "l2update"):
        return "book_l2"
    if kind == "heartbeat":
        return "heartbeats"
    if kind == "error":
        return "error"
    return "control"


def classify_kraken(msg: dict[str, Any]) -> str:
    channel = msg.get("channel")
    if channel == "trade":
        return "trades"
    if channel == "book":
        return "book_l2"
    if channel == "heartbeat":
        return "heartbeats"
    if msg.get("success") is False or "error" in msg:
        return "error"
    return "control"


CLASSIFIERS: dict[str, Callable[[dict[str, Any]], str]] = {
    "binance-usdm": classify_binance,
    "coinbase": classify_coinbase,
    "kraken": classify_kraken,
}


def classify(venue: str, frame: str | bytes) -> str:
    """Channel a raw frame belongs to: trades, book_l2, heartbeats, control or error."""
    try:
        msg = orjson.loads(frame)
    except orjson.JSONDecodeError:
        return "error"
    if not isinstance(msg, dict):
        return "control"
    return CLASSIFIERS[venue](msg)


def _needs_auth(msg: dict[str, Any]) -> bool:
    text = " ".join(str(msg.get(k, "")) for k in ("message", "reason", "error")).lower()
    return "auth" in text or "unauthori" in text


# ---------------------------------------------------------------- subscription plans


def kraken_rest_pair(symbol: str) -> str:
    """Kraken REST pair name for a WS v2 symbol (``BTC/USD`` -> ``XBTUSD``)."""
    base, quote = symbol.split("/")
    return ("XBT" if base == "BTC" else base) + quote


def ws_plan(venue: str, cfg: VenueConfig) -> WsPlan:
    """(url, messages to send after connect) per websocket connection."""
    if venue == "binance-usdm":
        plan: WsPlan = []
        streams = {"trades": cfg.trades.stream or "aggTrade", "book_l2": cfg.book.stream or "depth"}
        for channel in cfg.channels:
            if channel not in streams:
                continue
            names = "/".join(f"{s.lower()}@{streams[channel]}" for s in cfg.symbols)
            route = cfg.ws_routes.get(channel, "/stream")
            plan.append((f"{cfg.ws_url.rstrip('/')}{route}?streams={names}", []))
        return plan
    if venue == "coinbase":
        channel_names = {
            "trades": cfg.trades.stream or "matches",
            "book_l2": cfg.book.stream or "level2_batch",
            "heartbeats": "heartbeat",
        }
        sub = {
            "type": "subscribe",
            "product_ids": cfg.symbols,
            "channels": [channel_names[c] for c in cfg.channels],
        }
        return [(cfg.ws_url, [orjson.dumps(sub).decode()])]
    if venue == "kraken":
        messages = []
        if "book_l2" in cfg.channels:
            params: dict[str, Any] = {"channel": "book", "symbol": cfg.symbols}
            params["depth"] = cfg.book.depth or 10
            messages.append(orjson.dumps({"method": "subscribe", "params": params}).decode())
        if "trades" in cfg.channels:
            params = {"channel": "trade", "symbol": cfg.symbols}
            messages.append(orjson.dumps({"method": "subscribe", "params": params}).decode())
        return [(cfg.ws_url, messages)]
    raise ValueError(f"no smoke plan for venue {venue!r}")


def rest_plan(venue: str, cfg: VenueConfig) -> RestPlan:
    """(url, params) of the one REST snapshot request."""
    base = cfg.rest_url.rstrip("/")
    symbol = cfg.symbols[0]
    if venue == "binance-usdm":
        return f"{base}/fapi/v1/depth", {"symbol": symbol, "limit": cfg.book.snapshot_limit or 100}
    if venue == "coinbase":
        return f"{base}/products/{symbol}/book", {"level": 2}
    if venue == "kraken":
        return f"{base}/0/public/Depth", {"pair": kraken_rest_pair(symbol), "count": 100}
    raise ValueError(f"no smoke plan for venue {venue!r}")


# ---------------------------------------------------------------- runners


async def _ws_probe(
    venue: str, url: str, messages: list[str], seconds: float, result: VenueSmoke
) -> None:
    start = time.monotonic()
    try:
        async with connect(url, user_agent_header=USER_AGENT, open_timeout=10, max_size=None) as ws:
            for message in messages:
                await ws.send(message)
            deadline = start + seconds
            while (left := deadline - time.monotonic()) > 0:
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=left)
                except TimeoutError:
                    break
                channel = classify(venue, frame)
                result.frames[channel] += 1
                if channel in DATA_CHANNELS and result.first_frame_ms is None:
                    result.first_frame_ms = round((time.monotonic() - start) * 1000, 1)
                if channel == "error":
                    detail = frame if isinstance(frame, str) else frame.decode(errors="replace")
                    result.ws_errors.append(detail[:300])
                    try:
                        parsed = orjson.loads(frame)
                    except orjson.JSONDecodeError:
                        parsed = {}
                    if isinstance(parsed, dict) and _needs_auth(parsed):
                        result.auth_required = True
    except InvalidStatus as exc:
        status = exc.response.status_code
        result.ws_errors.append(f"handshake HTTP {status}")
        result.geo_blocked |= status in GEO_BLOCK_STATUS
        result.auth_required |= status in AUTH_STATUS
    except (OSError, TimeoutError) as exc:
        result.ws_errors.append(f"{type(exc).__name__}: {exc}")
    except Exception as exc:  # report, never crash the whole smoke run
        result.ws_errors.append(f"{type(exc).__name__}: {exc}")


async def _rest_probe(
    venue: str, cfg: VenueConfig, result: VenueSmoke, transport: httpx.AsyncBaseTransport | None
) -> None:
    url, params = rest_plan(venue, cfg)
    result.rest_url = url
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT}, timeout=10.0, transport=transport
        ) as client:
            response = await client.get(url, params=params)
    except httpx.HTTPError as exc:
        result.rest_error = f"{type(exc).__name__}: {exc}"
        return
    result.rest_status = response.status_code
    result.rest_bytes = len(response.content)
    result.geo_blocked |= response.status_code in GEO_BLOCK_STATUS
    result.auth_required |= response.status_code in AUTH_STATUS
    if response.status_code != 200:
        result.rest_error = response.text[:300]
        return
    try:
        body = response.json()
    except ValueError:
        result.rest_error = "response is not JSON"
        return
    result.rest_error = _rest_body_error(venue, body)


def _rest_body_error(venue: str, body: Any) -> str:
    if venue == "kraken":
        if not isinstance(body, dict) or body.get("error"):
            return f"kraken error: {body.get('error') if isinstance(body, dict) else body}"
        return ""
    if not isinstance(body, dict) or not body.get("bids") or not body.get("asks"):
        return "snapshot has no bids or asks"
    return ""


async def smoke_venue(
    venue: str,
    cfg: VenueConfig,
    seconds: float,
    transport: httpx.AsyncBaseTransport | None = None,
) -> VenueSmoke:
    """Run the websocket and REST probes for one venue concurrently."""
    result = VenueSmoke(venue=venue, channels=list(cfg.channels))
    probes = [_ws_probe(venue, url, msgs, seconds, result) for url, msgs in ws_plan(venue, cfg)]
    await asyncio.gather(*probes, _rest_probe(venue, cfg, result, transport))
    return result


async def smoke_all(
    venues: dict[str, VenueConfig],
    seconds: float,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[VenueSmoke]:
    """Smoke every venue in ``venues`` at the same time."""
    return list(
        await asyncio.gather(
            *(smoke_venue(name, cfg, seconds, transport) for name, cfg in venues.items())
        )
    )


def render(results: list[VenueSmoke]) -> str:
    """Plain-text table for the CLI."""
    lines = [f"{'venue':<14}{'ok':<5}{'ws':<5}{'rest':<6}frames"]
    for r in results:
        frames = ", ".join(f"{k}={v}" for k, v in sorted(r.frames.items()))
        lines.append(
            f"{r.venue:<14}{'yes' if r.ok else 'NO':<5}{'ok' if r.ws_ok else 'FAIL':<5}"
            f"{r.rest_status or '-'!s:<6}{frames}"
        )
        if r.geo_blocked:
            lines.append(f"  {r.venue}: GEO-BLOCKED (HTTP 403/451). Stop and ask the owner (R-01).")
        if r.auth_required:
            lines.append(f"  {r.venue}: authentication required (P-09).")
        lines.extend(f"  ws error: {e}" for e in r.ws_errors)
        if r.rest_error:
            lines.append(f"  rest error: {r.rest_error}")
    return "\n".join(lines)
