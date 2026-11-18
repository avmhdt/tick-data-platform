"""tickdb: tick capture, storage and query platform."""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__", "connect"]


def connect(*args: object, **kwargs: object) -> object:
    """Shortcut for :func:`tickdb.client.connect` (imported lazily)."""
    from tickdb.client import connect as _connect

    return _connect(*args, **kwargs)  # type: ignore[arg-type]
