"""Settings from ``.env`` (secrets, paths, ports) and the validated YAML config (spec section 10).

Secrets are ``SecretStr`` and are never logged or printed. ``${VAR}`` and ``${VAR:-default}``
in the YAML are filled from the settings first, then from the process environment.
"""

from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tickdb.timeutil import parse_duration_ns

DEFAULT_CONFIG = Path("config/tickdb.yaml")
_VAR = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


class Settings(BaseSettings):
    """Values from the environment and ``.env``; names match ``.env.example``."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", env_ignore_empty=True
    )

    clickhouse_password: SecretStr | None = None
    grafana_admin_password: SecretStr | None = None
    data_root: str = "~/data/tick-data-platform"
    tickdb_data_dir: str = ""
    tickdb_config: str = str(DEFAULT_CONFIG)
    tickdb_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    tickdb_log_json: bool = True
    clickhouse_host: str = "localhost"
    clickhouse_http_port: int = 8123
    clickhouse_user: str = "tickdb"
    clickhouse_readonly_user: str = "tickdb_ro"
    clickhouse_readonly_password: SecretStr | None = None
    prometheus_port: int = 9090
    grafana_port: int = 3000
    tickdb_metrics_port_base: int = 9101
    ntp_server: str = "pool.ntp.org"
    tickdb_itch_canonical_dir: str = ""
    coinbase_api_key: SecretStr | None = None
    coinbase_api_secret: SecretStr | None = None
    databento_api_key: SecretStr | None = None
    tickdb_databento_budget_usd: float = 0.0
    qlic: str = ""

    @property
    def data_dir(self) -> Path:
        """``TICKDB_DATA_DIR`` if set, else ``DATA_ROOT``, with ``~`` expanded."""
        return Path(os.path.expanduser(self.tickdb_data_dir or self.data_root))

    def data_dir_problems(self) -> list[str]:
        """Reasons the data dir breaks the placement rule (WSL ext4, never ``/mnt/*``)."""
        raw = (self.tickdb_data_dir or self.data_root).replace("\\", "/")
        problems: list[str] = []
        if raw.startswith("/mnt/") or PurePosixPath(raw).parts[:2] == ("/", "mnt"):
            problems.append(f"data dir {raw} is on a /mnt drive; use the WSL ext4 filesystem")
        if re.match(r"^[A-Za-z]:", raw):
            problems.append(f"data dir {raw} is a Windows path; data lives inside WSL")
        return problems

    def missing_required(self) -> list[str]:
        """Names of required secrets that are not set (P-06, P-07)."""
        missing = []
        if self.clickhouse_password is None:
            missing.append("CLICKHOUSE_PASSWORD")
        if self.grafana_admin_password is None:
            missing.append("GRAFANA_ADMIN_PASSWORD")
        return missing

    def substitutions(self) -> dict[str, str]:
        """Values the YAML may reference as ``${NAME}``."""
        return {
            "TICKDB_DATA_DIR": str(self.data_dir),
            "DATA_ROOT": str(self.data_dir),
            "CLICKHOUSE_HOST": self.clickhouse_host,
            "CLICKHOUSE_HTTP_PORT": str(self.clickhouse_http_port),
            "NTP_SERVER": self.ntp_server,
        }


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BookConfig(_Model):
    stream: str | None = None
    snapshot_limit: int | None = None
    rest_checkpoint_s: int | None = None
    depth: int | None = None
    checksum: bool = False


class TradesConfig(_Model):
    stream: str | None = None


class VenueConfig(_Model):
    enabled: bool = True
    ws_url: str
    ws_routes: dict[str, str] = Field(default_factory=dict)
    rest_url: str
    symbols: list[str]
    channels: list[Literal["trades", "book_l2", "heartbeats"]]
    trades: TradesConfig = TradesConfig()
    book: BookConfig = BookConfig()
    auth: Literal["none", "cdp_jwt"] = "none"
    max_conn_lifetime: str | None = None
    metrics_port: int

    @field_validator("max_conn_lifetime")
    @classmethod
    def _duration(cls, value: str | None) -> str | None:
        if value is not None:
            parse_duration_ns(value)
        return value

    @field_validator("symbols")
    @classmethod
    def _symbols(cls, value: list[str]) -> list[str]:
        if not value or len(value) > 5:
            raise ValueError("1 to 5 symbols per venue (spec section 2)")
        if len(set(value)) != len(value):
            raise ValueError("duplicate symbol")
        return value


class CaptureConfig(_Model):
    """Where live crypto data comes from (owner decision 2026-11-09, docs/decisions.md)."""

    mode: Literal["shared", "own"] = "shared"
    shared_root: str = "~/data/crypto-lob-alpha"


class ReconnectConfig(_Model):
    base_s: float = 1.0
    factor: float = 2.0
    cap_s: float = 60.0
    jitter: Literal["full", "none"] = "full"
    stable_reset_s: float = 300.0


class SpoolConfig(_Model):
    flush_interval_ms: int = 1000
    zstd_level: int = 3
    queue_max_frames: int = 100_000


class RestConfig(_Model):
    max_fraction_of_limit: float = Field(default=0.5, gt=0, le=0.5)
    timeout_s: float = 10.0
    retries: int = 3


class ParquetConfig(_Model):
    compression: str = "zstd"
    compression_level: int = 3
    row_group_rows: int = 1_000_000
    max_file_mb: int = 512


class CompactionConfig(_Model):
    daily_at_utc: str = "01:30"
    late_window_h: int = 1
    parquet: ParquetConfig = ParquetConfig()


class ClickHouseConfig(_Model):
    host: str = "localhost"
    port: int = 8123
    database: str = "tickdb"
    book_l2_ttl_days: int = 90


class OutlierConfig(_Model):
    window_s: int = 60
    mad_k: float = 10.0
    min_bps: float = 50.0
    outside_bbo_pct: float = 1.0


class DQConfig(_Model):
    outlier: OutlierConfig = OutlierConfig()
    clock_offset_max_ms: float = 50.0
    coverage_warn_pct: float = 99.0


class RetentionConfig(_Model):
    raw_spool_days: int = 30
    require_verified: bool = True
    prometheus_days: int = 45


class BenchReference(_Model):
    venue: str = "binance-usdm"
    symbol: str = "BTCUSDT"
    date: str = "auto"


class BenchConfig(_Model):
    reps: int = 10
    seed: int = 7
    reference: BenchReference = BenchReference()


class SilenceConfig(_Model):
    trades: float = 120.0
    book_l2: float = 15.0


class TickdbConfig(_Model):
    """The validated ``config/tickdb.yaml``."""

    version: Literal[1]
    data_root: str
    symbols_default: list[str]
    capture: CaptureConfig = CaptureConfig()
    venues: dict[str, VenueConfig]
    reconnect: ReconnectConfig = ReconnectConfig()
    silence_timeout_s: SilenceConfig = SilenceConfig()
    spool: SpoolConfig = SpoolConfig()
    rest: RestConfig = RestConfig()
    compaction: CompactionConfig = CompactionConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()
    dq: DQConfig = DQConfig()
    retention: RetentionConfig = RetentionConfig()
    bench: BenchConfig = BenchConfig()

    def enabled_venues(self) -> dict[str, VenueConfig]:
        return {name: venue for name, venue in self.venues.items() if venue.enabled}


def substitute(text: str, values: dict[str, str]) -> str:
    """Fill ``${VAR}`` / ``${VAR:-default}`` from ``values``, then ``os.environ``."""

    def repl(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        found = values.get(name) or os.environ.get(name)
        if found:
            return found
        if default is not None:
            return default
        raise ValueError(f"config references ${{{name}}}, which is not set")

    return _VAR.sub(repl, text)


def _substitute_tree(node: Any, values: dict[str, str]) -> Any:
    """Apply :func:`substitute` to every string in a parsed YAML tree (comments are gone)."""
    if isinstance(node, str):
        return substitute(node, values)
    if isinstance(node, list):
        return [_substitute_tree(item, values) for item in node]
    if isinstance(node, dict):
        return {key: _substitute_tree(item, values) for key, item in node.items()}
    return node


def load_config(path: str | Path | None = None, settings: Settings | None = None) -> TickdbConfig:
    """Read, substitute and validate the YAML config."""
    settings = settings or Settings()
    cfg_path = Path(path or settings.tickdb_config)
    raw: Any = _substitute_tree(
        yaml.safe_load(cfg_path.read_text(encoding="utf-8")), settings.substitutions()
    )
    if not isinstance(raw, dict):
        raise ValueError(f"{cfg_path}: expected a mapping at the top level")
    return TickdbConfig.model_validate(raw)
