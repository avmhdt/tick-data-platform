from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.conftest import CONFIG
from tickdb.config import Settings, VenueConfig, load_config, substitute


def _settings(**values: str) -> Settings:
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def test_repo_config_loads_with_defaults() -> None:
    cfg = load_config(CONFIG, _settings())
    assert set(cfg.enabled_venues()) == {"binance-usdm", "coinbase", "kraken"}
    assert cfg.capture.mode == "shared"
    assert cfg.clickhouse.port == 8123
    assert cfg.clickhouse.book_l2_ttl_days == 90
    assert cfg.retention.raw_spool_days == 30
    assert cfg.venues["kraken"].book.depth == 100
    assert cfg.venues["binance-usdm"].ws_routes["trades"] == "/market/stream"
    assert all(len(v.symbols) == 3 for v in cfg.venues.values())


def test_data_root_substituted_from_settings(tmp_path: Path) -> None:
    cfg = load_config(CONFIG, _settings(tickdb_data_dir=str(tmp_path)))
    assert cfg.data_root == str(tmp_path)


def test_tickdb_data_dir_overrides_data_root() -> None:
    s = _settings(data_root="/home/u/a", tickdb_data_dir="/home/u/b")
    assert s.data_dir == Path("/home/u/b")
    assert _settings(data_root="/home/u/a").data_dir == Path("/home/u/a")


def test_settings_read_env_file(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    # pragma: allowlist nextline secret
    env.write_text("CLICKHOUSE_PASSWORD=abc\nGRAFANA_ADMIN_PASSWORD=\nCLICKHOUSE_HTTP_PORT=18123\n")
    s = Settings(_env_file=env)  # type: ignore[call-arg]
    assert s.clickhouse_password is not None
    assert s.clickhouse_password.get_secret_value() == "abc"
    assert s.grafana_admin_password is None  # empty means unset
    assert s.clickhouse_http_port == 18123
    assert s.missing_required() == ["GRAFANA_ADMIN_PASSWORD"]
    assert "abc" not in repr(s)


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        ("/home/ze/data/tick-data-platform", False),
        ("~/data/tick-data-platform", False),
        ("/mnt/d/data", True),
        (r"D:\data", True),
        ("C:/data", True),
    ],
)
def test_data_dir_placement_rule(path: str, bad: bool) -> None:
    assert bool(_settings(data_root=path).data_dir_problems()) is bad


def test_substitute_default_and_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    assert substitute("${NOPE_X:-7}", {}) == "7"
    assert substitute("a${A}b", {"A": "1"}) == "a1b"
    monkeypatch.setenv("FROM_ENV_X", "e")
    assert substitute("${FROM_ENV_X}", {}) == "e"
    with pytest.raises(ValueError, match="NOPE_Y"):
        substitute("${NOPE_Y}", {})


def test_unknown_key_rejected(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(CONFIG.read_text(encoding="utf-8") + "\nsurprise: 1\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_config(bad, _settings())


def test_top_level_must_be_mapping(tmp_path: Path) -> None:
    bad = tmp_path / "list.yaml"
    bad.write_text("- 1\n")
    with pytest.raises(ValueError, match="mapping"):
        load_config(bad, _settings())


def _venue(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ws_url": "wss://x",
        "rest_url": "https://x",
        "symbols": ["A"],
        "channels": ["trades"],
        "metrics_port": 1,
    }
    base.update(over)
    return base


def test_venue_symbol_rules() -> None:
    VenueConfig.model_validate(_venue())
    with pytest.raises(ValidationError):
        VenueConfig.model_validate(_venue(symbols=[]))
    with pytest.raises(ValidationError):
        VenueConfig.model_validate(_venue(symbols=list("ABCDEF")))
    with pytest.raises(ValidationError):
        VenueConfig.model_validate(_venue(symbols=["A", "A"]))


def test_venue_lifetime_must_parse() -> None:
    assert VenueConfig.model_validate(_venue(max_conn_lifetime="23h")).max_conn_lifetime == "23h"
    with pytest.raises(ValidationError):
        VenueConfig.model_validate(_venue(max_conn_lifetime="a day"))


def test_rest_fraction_capped_at_half() -> None:
    from tickdb.config import RestConfig

    with pytest.raises(ValidationError):
        RestConfig(max_fraction_of_limit=0.8)
