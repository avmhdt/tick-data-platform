"""ClickHouse reachability and access rules (FR-06 skeleton, NFR-08).

Runs where CLICKHOUSE_PASSWORD is set: the CI service container, or `make test-integration`
against the local stack.
"""

from __future__ import annotations

import os

import clickhouse_connect
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("CLICKHOUSE_PASSWORD"), reason="no ClickHouse configured"
    ),
]


def _client(user: str | None = None, password: str | None = None) -> object:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=user or os.environ.get("CLICKHOUSE_USER", "tickdb"),
        password=os.environ["CLICKHOUSE_PASSWORD"] if password is None else password,
    )


def test_tickdb_user_can_query() -> None:
    client = _client()
    assert client.command("SELECT 1") == 1  # type: ignore[attr-defined]
    version = str(client.command("SELECT version()"))  # type: ignore[attr-defined]
    assert version.startswith("26.8")


def test_tickdb_database_exists() -> None:
    client = _client()
    assert client.command("EXISTS DATABASE tickdb") == 1  # type: ignore[attr-defined]


def test_default_user_is_disabled() -> None:
    with pytest.raises(Exception, match=r"(?i)auth|password|default"):
        _client(user="default", password="")


def test_server_memory_ratio_capped() -> None:
    client = _client()
    ratio = client.command(  # type: ignore[attr-defined]
        "SELECT value FROM system.server_settings "
        "WHERE name = 'max_server_memory_usage_to_ram_ratio'"
    )
    assert float(ratio) <= 0.5
