"""Smoke-test logic against local mock venues. Frames are synthetic, written from the venue docs."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
import orjson
import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Response

from tests.conftest import CONFIG
from tickdb import smoke
from tickdb.config import VenueConfig, load_config

Script = Callable[[str, list[str]], list[str]]

BINANCE_TRADE = {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "a": 1, "p": "1.0"}}
BINANCE_DEPTH = {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "U": 1, "u": 2}}
COINBASE = [
    {"type": "subscriptions", "channels": []},
    {"type": "snapshot", "product_id": "BTC-USD", "bids": [], "asks": []},
    {"type": "l2update", "product_id": "BTC-USD", "changes": []},
    {"type": "last_match", "trade_id": 1},
    {"type": "match", "trade_id": 2},
    {"type": "heartbeat", "sequence": 9},
]
KRAKEN = [
    {"channel": "status", "type": "update", "data": []},
    {"method": "subscribe", "success": True, "result": {"channel": "book"}},
    {"channel": "book", "type": "snapshot", "data": []},
    {"channel": "trade", "type": "update", "data": []},
    {"channel": "heartbeat"},
]


def venue_script(path: str, received: list[str]) -> list[str]:
    if "streams=" in path:
        frame = BINANCE_TRADE if "/market/" in path else BINANCE_DEPTH
        return [orjson.dumps(frame).decode()] * 3
    first = orjson.loads(received[0]) if received else {}
    frames = COINBASE if first.get("type") == "subscribe" else KRAKEN
    return [orjson.dumps(f).decode() for f in frames]


@asynccontextmanager
async def mock_venue(
    script: Script = venue_script, reject: int | None = None
) -> AsyncIterator[str]:
    async def handler(ws: ServerConnection) -> None:
        received: list[str] = []
        while True:
            try:
                received.append(str(await asyncio.wait_for(ws.recv(), timeout=0.2)))
            except TimeoutError:
                break
        for frame in script(ws.request.path if ws.request else "", received):
            await ws.send(frame)
        await ws.wait_closed()

    def process_request(_conn: ServerConnection, _request: object) -> Response | None:
        if reject is None:
            return None
        return Response(reject, "blocked", Headers(), b"")

    async with serve(handler, "127.0.0.1", 0, process_request=process_request) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        yield f"ws://127.0.0.1:{port}"


def rest_ok(request: httpx.Request) -> httpx.Response:
    if "/0/public/Depth" in request.url.path:
        return httpx.Response(200, json={"error": [], "result": {"XXBTZUSD": {}}})
    return httpx.Response(200, json={"bids": [["1", "1"]], "asks": [["2", "1"]]})


def cfg_for(venue: str, ws_url: str) -> VenueConfig:
    base = load_config(CONFIG).venues[venue]
    return base.model_copy(update={"ws_url": ws_url, "rest_url": "https://rest.example"})


@pytest.mark.parametrize("venue", ["binance-usdm", "coinbase", "kraken"])
async def test_venue_passes_against_mock(venue: str) -> None:
    async with mock_venue() as url:
        result = await smoke.smoke_venue(
            venue, cfg_for(venue, url), 1.0, httpx.MockTransport(rest_ok)
        )
    assert result.ok, result.as_dict()
    assert result.frames["trades"] > 0
    assert result.frames["book_l2"] > 0
    assert result.first_frame_ms is not None
    assert result.rest_status == 200


async def test_smoke_all_runs_venues_together() -> None:
    async with mock_venue() as url:
        venues = {v: cfg_for(v, url) for v in ("coinbase", "kraken")}
        results = await smoke.smoke_all(venues, 0.8, httpx.MockTransport(rest_ok))
    assert [r.venue for r in results] == ["coinbase", "kraken"]
    assert all(r.ok for r in results)
    assert "coinbase" in smoke.render(results)


async def test_rest_geo_block_is_reported() -> None:
    def blocked(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(451, text="Unavailable For Legal Reasons")

    async with mock_venue() as url:
        result = await smoke.smoke_venue(
            "binance-usdm", cfg_for("binance-usdm", url), 0.5, httpx.MockTransport(blocked)
        )
    assert result.geo_blocked
    assert not result.ok
    assert "GEO-BLOCKED" in smoke.render([result])


async def test_ws_handshake_403_is_geo_block() -> None:
    async with mock_venue(reject=403) as url:
        result = await smoke.smoke_venue(
            "kraken", cfg_for("kraken", url), 0.5, httpx.MockTransport(rest_ok)
        )
    assert result.geo_blocked
    assert any("403" in e for e in result.ws_errors)


async def test_ws_handshake_401_is_auth() -> None:
    async with mock_venue(reject=401) as url:
        result = await smoke.smoke_venue(
            "coinbase", cfg_for("coinbase", url), 0.5, httpx.MockTransport(rest_ok)
        )
    assert result.auth_required
    assert "authentication required" in smoke.render([result])


async def test_auth_error_frame_detected() -> None:
    def needs_auth(_path: str, _received: list[str]) -> list[str]:
        return [orjson.dumps({"type": "error", "message": "authentication failure"}).decode()]

    async with mock_venue(needs_auth) as url:
        result = await smoke.smoke_venue(
            "coinbase", cfg_for("coinbase", url), 0.5, httpx.MockTransport(rest_ok)
        )
    assert result.auth_required
    assert not result.ws_ok


async def test_missing_channel_fails() -> None:
    def book_only(_path: str, _received: list[str]) -> list[str]:
        return [orjson.dumps({"channel": "book", "type": "update", "data": []}).decode()]

    async with mock_venue(book_only) as url:
        result = await smoke.smoke_venue(
            "kraken", cfg_for("kraken", url), 0.5, httpx.MockTransport(rest_ok)
        )
    assert not result.ws_ok
    assert result.frames["trades"] == 0


async def test_unreachable_ws_and_rest() -> None:
    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    cfg = cfg_for("kraken", "ws://127.0.0.1:9")
    result = await smoke.smoke_venue("kraken", cfg, 0.5, httpx.MockTransport(down))
    assert result.ws_errors
    assert "ConnectError" in result.rest_error
    assert not result.ok


@pytest.mark.parametrize(
    ("venue", "body", "error"),
    [
        ("kraken", {"error": ["EQuery:Unknown asset pair"]}, "kraken error"),
        ("binance-usdm", {"bids": [], "asks": []}, "no bids"),
    ],
)
async def test_rest_body_errors(venue: str, body: object, error: str) -> None:
    def reply(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async with mock_venue() as url:
        result = await smoke.smoke_venue(
            venue, cfg_for(venue, url), 0.3, httpx.MockTransport(reply)
        )
    assert error in result.rest_error


async def test_rest_non_json_and_http_error() -> None:
    def text(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>")

    def teapot(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="oops")

    async with mock_venue() as url:
        cfg = cfg_for("coinbase", url)
        a = await smoke.smoke_venue("coinbase", cfg, 0.3, httpx.MockTransport(text))
        b = await smoke.smoke_venue("coinbase", cfg, 0.3, httpx.MockTransport(teapot))
    assert a.rest_error == "response is not JSON"
    assert b.rest_status == 500
    assert not b.rest_ok


@pytest.mark.parametrize(
    ("venue", "frame", "channel"),
    [
        ("binance-usdm", BINANCE_TRADE, "trades"),
        ("binance-usdm", {"stream": "x@trade"}, "trades"),
        ("binance-usdm", BINANCE_DEPTH, "book_l2"),
        ("binance-usdm", {"result": None, "id": 1}, "control"),
        ("binance-usdm", {"code": 2, "msg": "bad"}, "error"),
        ("coinbase", {"type": "match"}, "trades"),
        ("coinbase", {"type": "l2update"}, "book_l2"),
        ("coinbase", {"type": "heartbeat"}, "heartbeats"),
        ("coinbase", {"type": "error"}, "error"),
        ("coinbase", {"type": "subscriptions"}, "control"),
        ("kraken", {"channel": "trade"}, "trades"),
        ("kraken", {"channel": "book"}, "book_l2"),
        ("kraken", {"channel": "heartbeat"}, "heartbeats"),
        ("kraken", {"method": "subscribe", "success": False}, "error"),
        ("kraken", {"channel": "status"}, "control"),
    ],
)
def test_classify(venue: str, frame: dict[str, object], channel: str) -> None:
    assert smoke.classify(venue, orjson.dumps(frame)) == channel


def test_classify_non_json_and_non_object() -> None:
    assert smoke.classify("kraken", "not json") == "error"
    assert smoke.classify("kraken", "[1,2]") == "control"


def test_plans_match_shared_capture_endpoints() -> None:
    cfg = load_config(CONFIG)
    binance = smoke.ws_plan("binance-usdm", cfg.venues["binance-usdm"])
    assert [u.split("?")[0] for u, _ in binance] == [
        "wss://fstream.binance.com/market/stream",
        "wss://fstream.binance.com/public/stream",
    ]
    assert "btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade" in binance[0][0]
    assert "btcusdt@depth@100ms" in binance[1][0]
    ((_cb_url, cb_msgs),) = smoke.ws_plan("coinbase", cfg.venues["coinbase"])
    assert orjson.loads(cb_msgs[0])["channels"] == ["matches", "level2_batch", "heartbeat"]
    ((_, kr_msgs),) = smoke.ws_plan("kraken", cfg.venues["kraken"])
    assert orjson.loads(kr_msgs[0])["params"]["depth"] == 1000
    assert smoke.rest_plan("binance-usdm", cfg.venues["binance-usdm"])[1]["limit"] == 1000
    assert smoke.rest_plan("coinbase", cfg.venues["coinbase"])[0].endswith("/products/BTC-USD/book")
    assert smoke.rest_plan("kraken", cfg.venues["kraken"])[1]["pair"] == "XBTUSD"


def test_unknown_venue_plans() -> None:
    cfg = load_config(CONFIG).venues["kraken"]
    with pytest.raises(ValueError, match="no smoke plan"):
        smoke.ws_plan("okx", cfg)
    with pytest.raises(ValueError, match="no smoke plan"):
        smoke.rest_plan("okx", cfg)


def test_kraken_rest_pair() -> None:
    assert smoke.kraken_rest_pair("BTC/USD") == "XBTUSD"
    assert smoke.kraken_rest_pair("SOL/USD") == "SOLUSD"
