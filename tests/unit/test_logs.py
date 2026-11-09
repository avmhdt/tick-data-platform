from __future__ import annotations

import orjson
import pytest
import structlog

from tickdb import logs


def test_json_logs(capsys: pytest.CaptureFixture[str]) -> None:
    logs.configure("INFO", json=True)
    structlog.get_logger().info("frame", venue="kraken")
    structlog.get_logger().debug("hidden")
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    record = orjson.loads(lines[0])
    assert record["event"] == "frame"
    assert record["venue"] == "kraken"
    assert record["level"] == "info"


def test_console_logs(capsys: pytest.CaptureFixture[str]) -> None:
    logs.configure("DEBUG", json=False)
    structlog.get_logger().debug("visible")
    assert "visible" in capsys.readouterr().out
