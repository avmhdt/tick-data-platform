"""Helpers that run a real ingestor against a mock venue and read back its spool."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import orjson

from tests.conftest import CONFIG
from tests.integration.mock_venues import MockVenue
from tickdb.config import Settings, TickdbConfig, VenueConfig, load_config
from tickdb.ingest.base import VenueIngestor
from tickdb.ingest.metrics import Metrics
from tickdb.ingest.spool import SpoolWriter, read_spool
from tickdb.ingest.venues.registry import INGESTORS


def mock_config(
    venue: str,
    mock: MockVenue,
    tmp_path: Path,
    *,
    lifetime: str | None = None,
    silence: tuple[float, float] = (30.0, 30.0),
    depth: int | None = None,
) -> tuple[TickdbConfig, VenueConfig]:
    settings = Settings(_env_file=None, tickdb_data_dir=str(tmp_path))  # type: ignore[call-arg]
    base = load_config(CONFIG, settings)
    vcfg = base.venues[venue]
    book = vcfg.book.model_copy(
        update={"rest_checkpoint_s": None, "depth": depth or vcfg.book.depth}
    )
    vcfg = vcfg.model_copy(
        update={
            "ws_url": mock.ws_url,
            "rest_url": mock.rest_url,
            "max_conn_lifetime": lifetime,
            "book": book,
            "ws_routes": {"trades": "/market/stream", "book_l2": "/public/stream"},
        }
    )
    tcfg = base.model_copy(
        update={
            "capture": base.capture.model_copy(update={"mode": "own"}),
            "venues": {venue: vcfg},
            "reconnect": base.reconnect.model_copy(
                update={"base_s": 0.2, "cap_s": 1.0, "stable_reset_s": 300}
            ),
            "silence_timeout_s": base.silence_timeout_s.model_copy(
                update={"trades": silence[0], "book_l2": silence[1]}
            ),
        }
    )
    return tcfg, vcfg


Scenario = Callable[[VenueIngestor, MockVenue], Awaitable[None]]


async def run_with(
    venue: str,
    mock: MockVenue,
    tmp_path: Path,
    scenario: Scenario,
    **cfg_kw: Any,
) -> VenueIngestor:
    tcfg, vcfg = mock_config(venue, mock, tmp_path, **cfg_kw)
    spool = SpoolWriter(tmp_path, venue, flush_interval_s=0.2)
    ingestor = INGESTORS[venue](vcfg, tcfg, spool, Metrics(), rng=random.Random(7))
    await ingestor.run(until=scenario(ingestor, mock))
    return ingestor


def records(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "raw" / "spool").rglob("*.jsonl.zst*")):
        out.extend(read_spool(path))
    out.sort(key=lambda r: (r["ts_recv"], r["rx_seq"]))
    return out


def events(recs: list[dict[str, Any]], kind: str | None = None) -> list[dict[str, Any]]:
    found = []
    for r in recs:
        if r["kind"] != "event":
            continue
        payload = orjson.loads(r["payload"])
        payload["_ts"] = r["ts_recv"]
        payload["_conn"] = r["conn_id"]
        if kind is None or payload["type"] == kind:
            found.append(payload)
    return found


async def wait_for(predicate: Callable[[], bool], limit_s: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit_s
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)


def all_synced(ingestor: VenueIngestor) -> bool:
    conns = list(ingestor.active.values())
    return bool(conns) and all(c.ready for c in conns)
