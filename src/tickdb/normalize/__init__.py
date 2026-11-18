"""Normalizers: raw records of the shared capture -> canonical trades and book_l2 (FR-07)."""

from __future__ import annotations

from tickdb.normalize.binance_usdm import BinanceUsdmNormalizer
from tickdb.normalize.coinbase import CoinbaseNormalizer
from tickdb.normalize.engine import Engine, VenueNormalizer
from tickdb.normalize.kraken import KrakenNormalizer

NORMALIZERS: dict[str, type[VenueNormalizer]] = {
    "binance-usdm": BinanceUsdmNormalizer,
    "coinbase": CoinbaseNormalizer,
    "kraken": KrakenNormalizer,
}

__all__ = ["NORMALIZERS", "Engine", "VenueNormalizer"]
