"""structlog setup: JSON lines for services, readable output for the terminal (NFR-10)."""

from __future__ import annotations

import logging

import structlog


def configure(level: str = "INFO", json: bool = True) -> None:
    """Configure structlog once per process."""
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=False,
    )
