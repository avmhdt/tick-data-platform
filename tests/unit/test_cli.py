from __future__ import annotations

from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from tests.conftest import CONFIG
from tickdb import __version__, cli, smoke
from tickdb.clock import ClockReading

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(cli.app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_schema_print() -> None:
    result = runner.invoke(cli.app, ["schema", "print", "book_l2"])
    assert result.exit_code == 0
    assert "batch_id" in result.stdout


def test_schema_print_unknown_is_config_error() -> None:
    assert runner.invoke(cli.app, ["schema", "print", "nope"]).exit_code == cli.EXIT_CONFIG


def test_config_check_reports_missing_secrets_without_values(tmp_path: Path) -> None:
    # pragma: allowlist nextline secret
    (tmp_path / ".env").write_text("CLICKHOUSE_PASSWORD=s3cr3t-value\nDATA_ROOT=/home/u/data\n")
    result = runner.invoke(cli.app, ["config", "check", "--config", str(CONFIG)])
    assert result.exit_code == cli.EXIT_CONFIG
    assert "GRAFANA_ADMIN_PASSWORD" in result.output
    assert "s3cr3t-value" not in result.output


def test_config_check_passes(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        # pragma: allowlist nextline secret
        "CLICKHOUSE_PASSWORD=a\nGRAFANA_ADMIN_PASSWORD=b\nDATA_ROOT=/home/u/data\n"
    )
    result = runner.invoke(cli.app, ["config", "check", "--config", str(CONFIG)])
    assert result.exit_code == 0, result.output
    assert "capture=shared" in result.output


def test_config_check_flags_mnt_data_dir(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        # pragma: allowlist nextline secret
        "CLICKHOUSE_PASSWORD=a\nGRAFANA_ADMIN_PASSWORD=b\nDATA_ROOT=/mnt/d/data\n"
    )
    result = runner.invoke(cli.app, ["config", "check", "--config", str(CONFIG)])
    assert result.exit_code == cli.EXIT_CONFIG
    assert "/mnt" in result.output


def test_missing_config_file_is_config_error(tmp_path: Path) -> None:
    result = runner.invoke(cli.app, ["config", "check", "--config", str(tmp_path / "x.yaml")])
    assert result.exit_code == cli.EXIT_CONFIG


def _fake_results(ok: bool) -> list[smoke.VenueSmoke]:
    r = smoke.VenueSmoke(venue="kraken", channels=["trades", "book_l2"])
    if ok:
        r.frames = Counter({"trades": 1, "book_l2": 2})
        r.rest_status = 200
    return [r]


@pytest.mark.parametrize(("ok", "code"), [(True, 0), (False, 1)])
def test_smoke_command(monkeypatch: pytest.MonkeyPatch, ok: bool, code: int) -> None:
    seen: dict[str, object] = {}

    async def fake(venues: dict[str, object], seconds: float) -> list[smoke.VenueSmoke]:
        seen["venues"], seen["seconds"] = list(venues), seconds
        return _fake_results(ok)

    monkeypatch.setattr(smoke, "smoke_all", fake)
    args = ["smoke", "--venue", "kraken", "--seconds", "5", "--config", str(CONFIG)]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == code
    assert seen == {"venues": ["kraken"], "seconds": 5.0}
    json_result = runner.invoke(cli.app, [*args, "--json"])
    assert '"venue": "kraken"' in json_result.stdout


def test_smoke_all_venues(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    async def fake(venues: dict[str, object], seconds: float) -> list[smoke.VenueSmoke]:
        seen.extend(venues)
        return _fake_results(True)

    monkeypatch.setattr(smoke, "smoke_all", fake)
    assert runner.invoke(cli.app, ["smoke", "--config", str(CONFIG)]).exit_code == 0
    assert seen == ["binance-usdm", "coinbase", "kraken"]


def test_smoke_unknown_venue() -> None:
    result = runner.invoke(cli.app, ["smoke", "--venue", "okx", "--config", str(CONFIG)])
    assert result.exit_code == cli.EXIT_CONFIG


@pytest.mark.parametrize(("offset", "code"), [(-12.0, 0), (75.0, 2)])
def test_clock_check(monkeypatch: pytest.MonkeyPatch, offset: float, code: int) -> None:
    monkeypatch.setattr(cli.clock, "probe", lambda server: ClockReading(server, offset, 10.0, 2))
    result = runner.invoke(cli.app, ["clock", "check", "--server", "ntp.example"])
    assert result.exit_code == code
    assert "ntp.example" in result.stdout


def test_clock_check_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(server: str) -> SimpleNamespace:
        raise OSError("no route")

    monkeypatch.setattr(cli.clock, "probe", boom)
    assert runner.invoke(cli.app, ["clock", "check"]).exit_code == 1
