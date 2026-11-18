"""Write synthetic raw files in the shared capture's format (``ts\\tpayload`` frames and
``ts\\t!<json>\\t<body>`` control lines, one zstd stream per hour per connection).

Message shapes follow the venue docs; all values are synthetic.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import orjson
import zstandard

from tickdb.ingest.book import ASK, BID, L2Book, kraken_checksum
from tickdb.timeutil import day_bounds

RUN = "20261102T234621"
HOUR = 3_600_000_000_000


class RawDay:
    """Collect lines, then ``write()`` them into hourly files."""

    def __init__(self, root: Path, venue: str, day: date) -> None:
        self.root, self.venue, self.day = root, venue, day
        self.t0 = day_bounds(day)[0]
        self.lines: dict[tuple[int, str], list[bytes]] = defaultdict(list)

    def ts(self, seconds: float) -> int:
        return self.t0 + int(seconds * 1e9)

    def frame(self, seconds: float, conn: str, payload: Any) -> None:
        ts = self.ts(seconds)
        text = payload if isinstance(payload, str) else orjson.dumps(payload).decode()
        self.lines[((ts - self.t0) // HOUR, conn)].append(f"{ts}\t{text}".encode())

    def control(self, seconds: float, ctrl: dict[str, Any], body: Any = b"") -> None:
        ts = self.ts(seconds)
        raw = body if isinstance(body, bytes) else orjson.dumps(body)
        line = b"%d\t!%s\t%s" % (ts, orjson.dumps(ctrl), raw)
        self.lines[((ts - self.t0) // HOUR, "control")].append(line)

    def write(self) -> Path:
        base = self.root / f"venue={self.venue}" / f"date={self.day.isoformat()}"
        base.mkdir(parents=True, exist_ok=True)
        for (hour, conn), lines in self.lines.items():
            data = b"\n".join(lines) + b"\n"
            (base / f"{hour:02d}-{conn}-{RUN}.jsonl.zst").write_bytes(
                zstandard.ZstdCompressor().compress(data)
            )
        return self.root


def kraken_book_frame(kind: str, symbol: str, bids: Any, asks: Any, book: L2Book, ts: str) -> str:
    checksum = kraken_checksum(book.top(BID, 10), book.top(ASK, 10))
    levels = lambda rows: ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in rows)  # noqa: E731
    return (
        f'{{"channel":"book","type":"{kind}","data":[{{"symbol":"{symbol}",'
        f'"bids":[{levels(bids)}],"asks":[{levels(asks)}],'
        f'"checksum":{checksum},"timestamp":"{ts}"}}]}}'
    )
