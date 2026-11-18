"""Mock venues for ingestor tests: websocket feeds plus REST on one local port.

Each mock runs a synthetic market (book updates and trades for its symbols), broadcasts it to
every subscribed connection like a real venue, and can inject faults: drop all connections,
skip a sequence number, corrupt a checksum, skip a trade id, cross the book, go silent.
Message formats follow the venue docs cited in each adapter. All data is synthetic.

Run standalone (for the stress test) with:
    python tests/integration/mock_venues.py VENUE RATE_PER_S SECONDS [LEVELS]
which prints the port on its first line.
"""

from __future__ import annotations

import asyncio
import itertools
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

import orjson
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Request, Response

from tickdb.ingest.book import ASK, BID, L2Book, kraken_checksum


def _iso(ns: int) -> str:
    moment = datetime.fromtimestamp(ns / 1e9, tz=UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ns % 1_000_000_000:09d}Z"


def _px(value: float) -> str:
    return f"{value:.2f}"


def _qty(value: float) -> str:
    return f"{value:.8f}"


@dataclass
class _Sub:
    channels: set[str] = field(default_factory=set)  # "trades" | "book_l2"
    symbols: set[str] = field(default_factory=set)
    book_symbols: set[str] = field(default_factory=set)  # per-symbol book subscription


class MockVenue:
    """Base mock. Subclasses format frames for their venue."""

    kind = "base"

    def __init__(self, symbols: list[str], rate_per_s: float = 200.0, levels: int = 20) -> None:
        self.symbols = symbols
        self.rate = rate_per_s
        self.levels = levels
        self.server: Server | None = None
        self.port = 0
        self.subs: dict[ServerConnection, _Sub] = {}
        self.books: dict[str, L2Book] = {s: self._seed_book(i) for i, s in enumerate(symbols)}
        self.trade_ids: dict[str, int] = dict.fromkeys(symbols, 1000)
        self.sent_trades: dict[str, list[int]] = {s: [] for s in symbols}
        self.frames_sent = 0
        self.silent_until = 0.0
        self.paused = False
        self.connections_opened = 0
        self._tasks: list[asyncio.Task[None]] = []
        self._tick = itertools.count()

    # -------------------------------------------------------------- book helpers

    def _seed_book(self, i: int) -> L2Book:
        book = L2Book()
        mid = 100.0 + 50.0 * i
        for k in range(self.levels):
            book.apply(BID, _px(mid - 0.5 - k * 0.5), _qty(1.0 + k / 10))
            book.apply(ASK, _px(mid + 0.5 + k * 0.5), _qty(1.0 + k / 10))
        return book

    def _mutate(self, symbol: str, n: int) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        """Change one level's size on each side; return (bid changes, ask changes)."""
        book = self.books[symbol]
        bids = book.top(BID, self.levels)
        asks = book.top(ASK, self.levels)
        b_px = bids[n % len(bids)][0]
        a_px = asks[(n + 3) % len(asks)][0]
        b_q, a_q = _qty(1.0 + (n % 17) / 7), _qty(1.0 + (n % 13) / 5)
        book.apply(BID, b_px, b_q)
        book.apply(ASK, a_px, a_q)
        return [(b_px, b_q)], [(a_px, a_q)]

    # -------------------------------------------------------------- server

    async def start(self) -> MockVenue:
        self.server = await serve(
            self._handler, "127.0.0.1", 0, process_request=self._http, max_size=None
        )
        self.port = next(iter(self.server.sockets)).getsockname()[1]
        self._tasks.append(asyncio.create_task(self._producer()))
        return self

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def _http(self, conn: ServerConnection, request: Request) -> Response | None:
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None
        body = self.rest(urlparse(request.path))
        if body is None:
            return Response(404, "Not Found", Headers(), b"{}")
        headers = Headers({"Content-Type": "application/json"})
        return Response(200, "OK", headers, orjson.dumps(body))

    def rest(self, url: Any) -> Any:
        return None

    async def _handler(self, ws: ServerConnection) -> None:
        self.connections_opened += 1
        sub = _Sub()
        self.subs[ws] = sub
        self.on_open(ws, sub)
        try:
            async for message in ws:
                await self.on_message(ws, sub, orjson.loads(message))
        except Exception:
            pass
        finally:
            self.subs.pop(ws, None)

    def on_open(self, ws: ServerConnection, sub: _Sub) -> None:
        """Binance-style subscription from the URL."""

    async def on_message(self, ws: ServerConnection, sub: _Sub, msg: Any) -> None:
        """Subscription messages."""

    async def _send(self, ws: ServerConnection, frame: str) -> None:
        try:
            await ws.send(frame)
            self.frames_sent += 1
        except Exception:
            pass

    async def broadcast(self, channel: str, symbol: str, frame: str) -> None:
        if self.paused or time.monotonic() < self.silent_until:
            return
        for ws, sub in list(self.subs.items()):
            if channel in sub.channels and (
                symbol in sub.symbols if channel == "trades" else symbol in sub.book_symbols
            ):
                await self._send(ws, frame)

    async def _producer(self) -> None:
        interval = 1.0 / self.rate
        start = time.monotonic()
        for n in itertools.count():
            symbol = self.symbols[n % len(self.symbols)]
            if n % 4 == 3:
                await self.trade(symbol)
            else:
                await self.book_update(symbol, n)
            delay = start + (n + 1) * interval - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            elif n % 64 == 0:
                await asyncio.sleep(0)

    async def trade(self, symbol: str) -> None:
        self.trade_ids[symbol] += 1
        trade_id = self.trade_ids[symbol]
        if self.paused or time.monotonic() < self.silent_until:
            return
        self.sent_trades[symbol].append(trade_id)
        await self.broadcast("trades", symbol, self.trade_frame(symbol, trade_id, time.time_ns()))

    async def book_update(self, symbol: str, n: int) -> None:
        raise NotImplementedError

    def trade_frame(self, symbol: str, trade_id: int, ts: int) -> str:
        raise NotImplementedError

    # -------------------------------------------------------------- faults

    async def drop_all(self) -> None:
        for ws in list(self.subs):
            await ws.close(1011, "mock drop")

    def silence(self, seconds: float) -> None:
        self.silent_until = time.monotonic() + seconds

    def skip_trade_id(self, symbol: str) -> None:
        self.trade_ids[symbol] += 1


# ------------------------------------------------------------------ Binance USD-M


class MockBinance(MockVenue):
    kind = "binance-usdm"

    def __init__(self, symbols: list[str], rate_per_s: float = 200.0, levels: int = 20) -> None:
        super().__init__(symbols, rate_per_s, levels)
        self.last_u: dict[str, int] = {s: 5_000_000 + i * 1000 for i, s in enumerate(symbols)}

    def on_open(self, ws: ServerConnection, sub: _Sub) -> None:
        url = urlparse(ws.request.path if ws.request else "")
        streams = parse_qs(url.query).get("streams", [""])[0].split("/")
        for stream in streams:
            if "@" not in stream:
                continue
            sym, kind = stream.split("@", 1)
            symbol = sym.upper()
            if kind.startswith("aggTrade"):
                sub.channels.add("trades")
                sub.symbols.add(symbol)
            elif kind.startswith("depth"):
                sub.channels.add("book_l2")
                sub.book_symbols.add(symbol)

    def rest(self, url: Any) -> Any:
        if url.path != "/fapi/v1/depth":
            return None
        symbol = parse_qs(url.query)["symbol"][0]
        book = self.books[symbol]
        return {
            "lastUpdateId": self.last_u[symbol],
            "E": time.time_ns() // 1_000_000,
            "T": time.time_ns() // 1_000_000,
            "bids": [list(lv) for lv in book.top(BID, self.levels)],
            "asks": [list(lv) for lv in book.top(ASK, self.levels)],
        }

    async def book_update(self, symbol: str, n: int) -> None:
        bids, asks = self._mutate(symbol, n)
        prev = self.last_u[symbol]
        first, final = prev + 1, prev + 1 + n % 3
        self.last_u[symbol] = final
        ms = time.time_ns() // 1_000_000
        data = {
            "e": "depthUpdate",
            "E": ms,
            "T": ms,
            "s": symbol,
            "U": first,
            "u": final,
            "pu": prev,
            "b": [list(b) for b in bids],
            "a": [list(a) for a in asks],
        }
        frame = orjson.dumps({"stream": f"{symbol.lower()}@depth@100ms", "data": data}).decode()
        await self.broadcast("book_l2", symbol, frame)

    def trade_frame(self, symbol: str, trade_id: int, ts: int) -> str:
        ms = ts // 1_000_000
        data = {
            "e": "aggTrade",
            "E": ms,
            "a": trade_id,
            "s": symbol,
            "p": "100.10",
            "q": "0.001",
            "f": trade_id * 3,
            "l": trade_id * 3 + 2,
            "T": ms,
            "m": trade_id % 2 == 0,
        }
        return orjson.dumps({"stream": f"{symbol.lower()}@aggTrade", "data": data}).decode()

    def skip_seq(self, symbol: str) -> None:
        self.last_u[symbol] += 7  # the next event's pu no longer matches the previous u


# ------------------------------------------------------------------ Coinbase Exchange


class MockCoinbase(MockVenue):
    kind = "coinbase"
    cross_next: str | None = None

    async def on_message(self, ws: ServerConnection, sub: _Sub, msg: Any) -> None:
        kind = msg.get("type")
        products = set(msg.get("product_ids", []))
        channels = set(msg.get("channels", []))
        if kind == "subscribe":
            if "matches" in channels:
                sub.channels.add("trades")
                sub.symbols |= products
            await self._send(ws, orjson.dumps({"type": "subscriptions", "channels": []}).decode())
            if "level2_batch" in channels:
                sub.channels.add("book_l2")
                for product in sorted(products):
                    book = self.books[product]
                    snap = {
                        "type": "snapshot",
                        "product_id": product,
                        "bids": [list(lv) for lv in book.top(BID, self.levels)],
                        "asks": [list(lv) for lv in book.top(ASK, self.levels)],
                    }
                    await self._send(ws, orjson.dumps(snap).decode())
                    sub.book_symbols.add(product)
        elif kind == "unsubscribe" and "level2_batch" in channels:
            sub.book_symbols -= products

    async def book_update(self, symbol: str, n: int) -> None:
        bids, asks = self._mutate(symbol, n)
        changes = [["buy", p, q] for p, q in bids] + [["sell", p, q] for p, q in asks]
        if self.cross_next == symbol:
            self.cross_next = None
            best_ask = self.books[symbol].top(ASK, 1)[0][0]
            changes = [["buy", _px(float(best_ask) + 1.0), _qty(1.0)]]
        msg = {
            "type": "l2update",
            "product_id": symbol,
            "changes": changes,
            "time": _iso(time.time_ns()),
        }
        await self.broadcast("book_l2", symbol, orjson.dumps(msg).decode())

    def trade_frame(self, symbol: str, trade_id: int, ts: int) -> str:
        msg = {
            "type": "match",
            "trade_id": trade_id,
            "product_id": symbol,
            "side": "sell",
            "size": "0.01000000",
            "price": "100.10",
            "time": _iso(ts),
        }
        return orjson.dumps(msg).decode()

    def rest(self, url: Any) -> Any:
        if not url.path.endswith("/book"):
            return None
        symbol = url.path.split("/")[2]
        book = self.books[symbol]
        return {
            "bids": [[p, q, 1] for p, q in book.top(BID, self.levels)],
            "asks": [[p, q, 1] for p, q in book.top(ASK, self.levels)],
            "sequence": 1,
        }


# ------------------------------------------------------------------ Kraken v2


def _kraken_levels(levels: list[tuple[str, str]]) -> str:
    # Numbers as JSON number tokens that keep their trailing zeros, as Kraken sends them.
    return "[" + ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in levels) + "]"


class MockKraken(MockVenue):
    kind = "kraken"
    bad_checksum_next: str | None = None

    def _checksum(self, symbol: str) -> int:
        book = self.books[symbol]
        return kraken_checksum(book.top(BID, 10), book.top(ASK, 10))

    def _book_frame(self, kind: str, symbol: str, bids: Any, asks: Any, checksum: int) -> str:
        ts = _iso(time.time_ns())
        return (
            f'{{"channel":"book","type":"{kind}","data":[{{"symbol":"{symbol}",'
            f'"bids":{_kraken_levels(bids)},"asks":{_kraken_levels(asks)},'
            f'"checksum":{checksum},"timestamp":"{ts}"}}]}}'
        )

    async def on_message(self, ws: ServerConnection, sub: _Sub, msg: Any) -> None:
        params = msg.get("params", {})
        channel, symbols = params.get("channel"), params.get("symbol", [])
        ack = {"method": msg.get("method"), "success": True, "result": {"channel": channel}}
        await self._send(ws, orjson.dumps(ack).decode())
        if msg.get("method") == "subscribe" and channel == "trade":
            sub.channels.add("trades")
            sub.symbols |= set(symbols)
        elif msg.get("method") == "subscribe" and channel == "book":
            sub.channels.add("book_l2")
            for symbol in symbols:
                book = self.books[symbol]
                frame = self._book_frame(
                    "snapshot",
                    symbol,
                    book.top(BID, self.levels),
                    book.top(ASK, self.levels),
                    self._checksum(symbol),
                )
                await self._send(ws, frame)
                sub.book_symbols.add(symbol)
        elif msg.get("method") == "unsubscribe" and channel == "book":
            sub.book_symbols -= set(symbols)

    async def book_update(self, symbol: str, n: int) -> None:
        bids, asks = self._mutate(symbol, n)
        checksum = self._checksum(symbol)
        if self.bad_checksum_next == symbol:
            self.bad_checksum_next = None
            checksum = (checksum + 1) % 2**32
        await self.broadcast(
            "book_l2", symbol, self._book_frame("update", symbol, bids, asks, checksum)
        )

    def trade_frame(self, symbol: str, trade_id: int, ts: int) -> str:
        row = {
            "symbol": symbol,
            "side": "buy",
            "price": 100.1,
            "qty": 0.01,
            "ord_type": "market",
            "trade_id": trade_id,
            "timestamp": _iso(ts),
        }
        return orjson.dumps({"channel": "trade", "type": "update", "data": [row]}).decode()


MOCKS: dict[str, Callable[..., MockVenue]] = {
    "binance-usdm": lambda **kw: MockBinance(["BTCUSDT", "ETHUSDT", "SOLUSDT"], **kw),
    "coinbase": lambda **kw: MockCoinbase(["BTC-USD", "ETH-USD", "SOL-USD"], **kw),
    "kraken": lambda **kw: MockKraken(["BTC/USD", "ETH/USD", "SOL/USD"], **kw),
}


async def _main(venue: str, rate: float, seconds: float, levels: int) -> None:  # pragma: no cover
    mock = await MOCKS[venue](rate_per_s=rate, levels=levels).start()
    print(mock.port, flush=True)
    await asyncio.sleep(seconds)
    print(orjson.dumps({"frames_sent": mock.frames_sent}).decode(), flush=True)
    await mock.stop()


if __name__ == "__main__":  # pragma: no cover
    levels = int(sys.argv[4]) if len(sys.argv) > 4 else 20
    asyncio.run(_main(sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), levels))
