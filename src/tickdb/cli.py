"""The ``tickdb`` command line (spec section 10).

Exit codes: 0 success, 1 failure, 2 verification or DQ mismatch, 3 configuration or
prerequisite missing.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from tickdb import __version__, clock, schemas, smoke
from tickdb.config import Settings, TickdbConfig, load_config

EXIT_OK, EXIT_FAIL, EXIT_MISMATCH, EXIT_CONFIG = 0, 1, 2, 3

app = typer.Typer(help="Tick data platform: capture, archive, query and monitor tick data.")
schema_app = typer.Typer(help="Canonical dataset schemas.")
clock_app = typer.Typer(help="Host clock checks.")
config_app = typer.Typer(help="Configuration checks.")
app.add_typer(schema_app, name="schema")
app.add_typer(clock_app, name="clock")
app.add_typer(config_app, name="config")

ConfigOpt = Annotated[
    Path | None, typer.Option("--config", help="YAML config; default TICKDB_CONFIG.")
]


def _load(config: Path | None) -> tuple[Settings, TickdbConfig]:
    settings = Settings()
    try:
        return settings, load_config(config, settings)
    except (OSError, ValueError, ValidationError) as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(EXIT_CONFIG) from exc


@app.command()
def version() -> None:
    """Print the tickdb version."""
    typer.echo(__version__)


@app.command("smoke")
def smoke_cmd(
    venue: Annotated[str, typer.Option(help="'all' or one venue id.")] = "all",
    seconds: Annotated[float, typer.Option(min=1, max=300, help="Websocket listen time.")] = 30,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON instead of a table.")] = False,
    config: ConfigOpt = None,
) -> None:
    """Data-access test: websocket for N seconds plus one REST snapshot per venue.

    Reports reachability, geo-blocks and auth requirements. Writes nothing to the archive.

    Example: tickdb smoke --venue all --seconds 30
    """
    _, cfg = _load(config)
    venues = cfg.enabled_venues()
    if venue != "all":
        if venue not in cfg.venues:
            typer.echo(f"unknown venue {venue!r}; known: {', '.join(cfg.venues)}", err=True)
            raise typer.Exit(EXIT_CONFIG)
        venues = {venue: cfg.venues[venue]}
    results = asyncio.run(smoke.smoke_all(venues, seconds))
    if as_json:
        typer.echo(json.dumps([r.as_dict() for r in results], indent=2))
    else:
        typer.echo(smoke.render(results))
    raise typer.Exit(EXIT_OK if all(r.ok for r in results) else EXIT_FAIL)


@schema_app.command("print")
def schema_print(
    dataset: Annotated[str, typer.Argument(help="Dataset name, e.g. trades.")],
) -> None:
    """Print a canonical dataset schema.

    Example: tickdb schema print trades
    """
    try:
        typer.echo(schemas.describe(dataset))
    except KeyError as exc:
        typer.echo(str(exc.args[0]), err=True)
        raise typer.Exit(EXIT_CONFIG) from exc


@clock_app.command("check")
def clock_check(
    server: Annotated[str | None, typer.Option(help="NTP server; default NTP_SERVER.")] = None,
    limit_ms: Annotated[float, typer.Option(help="Pass if |offset| is below this.")] = 50.0,
) -> None:
    """Measure the host clock offset against NTP (P-08: |offset| < 50 ms).

    Example: tickdb clock check
    """
    target = server or Settings().ntp_server
    try:
        reading = clock.probe(target)
    except Exception as exc:
        typer.echo(f"NTP probe of {target} failed: {exc}", err=True)
        raise typer.Exit(EXIT_FAIL) from exc
    verdict = "ok" if reading.within(limit_ms) else "OVER LIMIT"
    typer.echo(
        f"{reading.server}: offset {reading.offset_ms:+.1f} ms, delay {reading.delay_ms:.1f} ms, "
        f"stratum {reading.stratum} -> {verdict} (limit {limit_ms:g} ms)"
    )
    raise typer.Exit(EXIT_OK if reading.within(limit_ms) else EXIT_MISMATCH)


@config_app.command("check")
def config_check(config: ConfigOpt = None) -> None:
    """Validate the YAML config and .env without printing any secret.

    Example: tickdb config check
    """
    settings, cfg = _load(config)
    problems = settings.data_dir_problems()
    missing = settings.missing_required()
    typer.echo(f"config ok: {len(cfg.enabled_venues())} enabled venues, capture={cfg.capture.mode}")
    typer.echo(f"data dir: {settings.data_dir}")
    for problem in problems:
        typer.echo(f"problem: {problem}", err=True)
    for name in missing:
        typer.echo(f"missing: {name} is not set in .env", err=True)
    raise typer.Exit(EXIT_CONFIG if problems or missing else EXIT_OK)


def main() -> None:  # pragma: no cover - console entry point
    app()
