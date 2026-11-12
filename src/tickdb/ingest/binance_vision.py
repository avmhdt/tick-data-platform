"""data.binance.vision daily USD-M futures aggTrades files (FR-05 fallback, AC-W-02 check).

``https://data.binance.vision/data/futures/um/daily/aggTrades/<SYM>/<SYM>-aggTrades-<D>.zip``
plus ``.zip.CHECKSUM`` (SHA-256). Files are stored under ``raw/external/binance-vision/`` (never
in git), downloaded at most at 8 MB/s and verified before use. Binance's REST ``aggTrades``
only searches the last two days, so older trade gaps are recovered from these files with
``source = 'vendor'`` and gap recovery ``binance_vision``.
"""

from __future__ import annotations

import csv
import hashlib
import io
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import httpx

from tickdb.smoke import USER_AGENT

BASE = "https://data.binance.vision/data/futures/um/daily/aggTrades"
RATE_BYTES = 8_000_000


def file_name(symbol: str, day: date) -> str:
    return f"{symbol}-aggTrades-{day.isoformat()}.zip"


def local_path(data_root: Path, symbol: str, day: date) -> Path:
    return data_root / "raw" / "external" / "binance-vision" / symbol / file_name(symbol, day)


def fetch(data_root: Path, symbol: str, day: date, client: httpx.Client | None = None) -> Path:
    """Download and checksum-verify one day's file (no-op if a verified copy exists)."""
    target = local_path(data_root, symbol, day)
    if target.exists() and target.with_suffix(".zip.ok").exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    http = client or httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60.0)
    url = f"{BASE}/{symbol}/{file_name(symbol, day)}"
    expected = http.get(url + ".CHECKSUM").raise_for_status().text.split()[0]
    digest = hashlib.sha256()
    tmp = target.with_name(target.name + ".tmp")
    started = time.monotonic()
    done = 0
    with http.stream("GET", url) as response, tmp.open("wb") as fh:
        response.raise_for_status()
        for chunk in response.iter_bytes(1 << 20):
            fh.write(chunk)
            digest.update(chunk)
            done += len(chunk)
            ahead = done / RATE_BYTES - (time.monotonic() - started)
            if ahead > 0:
                time.sleep(ahead)
    if digest.hexdigest() != expected:
        tmp.unlink()
        raise ValueError(f"{url}: checksum mismatch")
    tmp.replace(target)
    target.with_suffix(".zip.ok").write_text(expected)
    return target


def rows(path: Path, lo: int | None = None, hi: int | None = None) -> list[dict[str, Any]]:
    """aggTrades rows (``a, p, q, f, l, T, m`` as in the REST response) with ``lo <= a <= hi``."""
    out = []
    with zipfile.ZipFile(path) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as raw:
            for rec in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
                if not rec or not rec[0].isdigit():
                    continue  # header line in newer files
                a = int(rec[0])
                if (lo is not None and a < lo) or (hi is not None and a > hi):
                    continue
                out.append(
                    {"a": a, "p": rec[1], "q": rec[2], "f": int(rec[3]), "l": int(rec[4]),
                     "T": int(rec[5]), "m": rec[6].strip().lower() == "true"}
                )  # fmt: skip
    return out
