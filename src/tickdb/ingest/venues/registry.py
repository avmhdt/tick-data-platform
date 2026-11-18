"""Venue id -> ingestor class."""

from __future__ import annotations

from tickdb.ingest.base import VenueIngestor
from tickdb.ingest.venues.binance_usdm import BinanceUsdmIngestor
from tickdb.ingest.venues.coinbase import CoinbaseIngestor
from tickdb.ingest.venues.kraken import KrakenIngestor

INGESTORS: dict[str, type[VenueIngestor]] = {
    cls.venue: cls for cls in (BinanceUsdmIngestor, CoinbaseIngestor, KrakenIngestor)
}
