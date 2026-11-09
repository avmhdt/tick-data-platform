from __future__ import annotations

from pathlib import Path

import pytest

_ENV = (
    "CLICKHOUSE_PASSWORD",
    "GRAFANA_ADMIN_PASSWORD",
    "DATA_ROOT",
    "TICKDB_DATA_DIR",
    "TICKDB_CONFIG",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_USER",
)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Unit tests never read the developer's .env or environment."""
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
