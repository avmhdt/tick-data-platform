"""The ``tickdb`` command line (spec section 10).

Exit codes: 0 success, 1 failure, 2 verification or DQ mismatch, 3 configuration or
prerequisite missing.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

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


# ---------------------------------------------------------------- shared-capture mirror

mirror_app = typer.Typer(help="Read-only copy of the shared capture's raw archive.")
app.add_typer(mirror_app, name="mirror")

SourceOpt = Annotated[Path, typer.Option(help="Shared capture data dir (read-only).")]
DestOpt = Annotated[Path | None, typer.Option(help="Data root; default TICKDB_DATA_DIR/DATA_ROOT.")]
FromOpt = Annotated[str, typer.Option("--from", help="First UTC date to mirror (YYYY-MM-DD).")]
ToOpt = Annotated[str | None, typer.Option("--to", help="Last UTC date to mirror (inclusive).")]
ExcludeOpt = Annotated[
    list[str] | None,
    typer.Option("--exclude", help="VENUE:GROUP_PREFIX to skip, e.g. binance-usdm:ticker."),
]


def _mirror_args(
    dest: Path | None, from_: str, to: str | None, exclude: list[str] | None
) -> tuple[Path, Any, Any, Any]:
    from datetime import date as _date

    from tickdb.capture import mirror

    root = dest or Settings().data_dir
    try:
        last = _date.fromisoformat(to) if to else None
        return root, _date.fromisoformat(from_), last, mirror.parse_excludes(exclude or [])
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(EXIT_CONFIG) from exc


@mirror_app.command("run")
def mirror_run(
    source: SourceOpt = Path("~/data/crypto-lob-alpha"),
    dest: DestOpt = None,
    from_: FromOpt = "2026-11-08",
    to: ToOpt = None,
    exclude: ExcludeOpt = None,
    rate_mb: Annotated[float, typer.Option(help="Read cap in MB/s.")] = 8.0,
    guard_path: Annotated[Path | None, typer.Option(help="Path on the disk to protect.")] = None,
    floor_gb: Annotated[float, typer.Option(help="Stop before free space falls below.")] = 150.0,
    grace_s: Annotated[
        float, typer.Option(help="Seconds after an hour ends before copying.")
    ] = 600,
    min_mem_gb: Annotated[float, typer.Option(help="Stop if free memory falls below.")] = 30.0,
) -> None:
    """Copy closed hours of the shared raw archive, byte for byte, with a manifest.

    Example: tickdb mirror run --from 2026-11-08 --exclude binance-usdm:ticker --guard-path /mnt/d
    """
    from tickdb.capture import mirror

    root, first, last, excludes = _mirror_args(dest, from_, to, exclude)
    src = source.expanduser()
    started = datetime.now(UTC)
    with mirror.run_lock(root / "meta" / "mirror" / ".lock") as locked:
        if not locked:
            typer.echo("another mirror run holds the lock; nothing done")
            raise typer.Exit(EXIT_OK)
        result = mirror.mirror(
            src,
            root,
            first,
            to_day=last,
            excludes=excludes,
            grace_s=grace_s,
            rate_bytes_per_s=rate_mb * 1e6,
            guard_path=guard_path,
            floor_bytes=int(floor_gb * mirror.GB),
            min_mem_bytes=int(min_mem_gb * mirror.GB),
        )
    summary = result.as_dict()
    finished = datetime.now(UTC)
    summary["started"] = started.isoformat(timespec="seconds")
    summary["seconds"] = round((finished - started).total_seconds(), 1)
    summary["utc"] = finished.isoformat(timespec="seconds")
    typer.echo(json.dumps(summary))
    if result.stopped:
        raise typer.Exit(EXIT_CONFIG)
    raise typer.Exit(EXIT_FAIL if result.errors else EXIT_OK)


@mirror_app.command("status")
def mirror_status(
    source: SourceOpt = Path("~/data/crypto-lob-alpha"),
    dest: DestOpt = None,
    from_: FromOpt = "2026-11-08",
    to: ToOpt = None,
    exclude: ExcludeOpt = None,
) -> None:
    """Per venue and day: closed source files and how many have a same-size copy.

    Exit 2 if any closed file is missing from the mirror.

    Example: tickdb mirror status --from 2026-11-08 --exclude binance-usdm:ticker
    """
    from tickdb.capture import mirror

    root, first, last, excludes = _mirror_args(dest, from_, to, exclude)
    rows = mirror.status(source.expanduser(), root, first, to_day=last, excludes=excludes)
    typer.echo(f"{'venue':<14}{'date':<12}{'files':>6}{'closed':>8}{'copied':>8}{'excl':>6}  state")
    for r in rows:
        state = "complete" if r.complete else ("open" if not r.missing else "MISSING")
        typer.echo(
            f"{r.venue:<14}{r.day.isoformat():<12}{r.source_files:>6}{r.closed_files:>8}"
            f"{r.copied_files:>8}{r.excluded:>6}  {state}"
        )
    raise typer.Exit(EXIT_MISMATCH if any(r.missing for r in rows) else EXIT_OK)


@mirror_app.command("verify")
def mirror_verify(
    from_: FromOpt = "2026-11-08",
    to: ToOpt = None,
    dest: DestOpt = None,
    rate_mb: Annotated[float, typer.Option(help="Read cap in MB/s.")] = 8.0,
) -> None:
    """Re-hash mirrored files against their manifest and decode every zstd frame.

    Exit 2 on any hash mismatch or file missing from the manifest.

    Example: tickdb mirror verify --from 2026-11-08 --to 2026-11-09
    """
    from datetime import timedelta

    from tickdb.capture import mirror

    root, first, last, _ = _mirror_args(dest, from_, to, None)
    last = last or datetime.now(UTC).date()
    limiter = mirror.RateLimiter(rate_mb * 1e6)
    bad = False
    day = first
    while day <= last:
        r = mirror.verify(root, day, limiter)
        if r.files:
            typer.echo(
                json.dumps(
                    {
                        "date": day.isoformat(),
                        "files": r.files,
                        "hash_ok": r.hash_ok,
                        "hash_bad": list(r.hash_bad),
                        "unlisted": list(r.unlisted),
                        "complete_frames": r.complete_frames,
                        "cut_last_frame": list(r.cut_frames),
                        "lines": r.lines,
                    }
                )
            )
            bad |= bool(r.hash_bad or r.unlisted)
        day += timedelta(days=1)
    raise typer.Exit(EXIT_MISMATCH if bad else EXIT_OK)


@mirror_app.command("report")
def mirror_report(
    since: Annotated[str, typer.Option(help="ISO time, UTC.")],
    until: Annotated[str | None, typer.Option(help="ISO time, UTC.")] = None,
    dest: DestOpt = None,
) -> None:
    """Summarize the mirror's run log over a window (the 24 h soak).

    Example: tickdb mirror report --since 2026-11-10T19:00:00+00:00
    """
    from tickdb.capture import mirror

    root = dest or Settings().data_dir
    report = mirror.run_log_report(
        root / "meta" / "mirror" / "run.log",
        datetime.fromisoformat(since),
        datetime.fromisoformat(until) if until else None,
    )
    typer.echo(json.dumps(report))


# ---------------------------------------------------------------- ingestors

ingest_app = typer.Typer(help="Venue ingestors (mock venues only while capture is shared).")
disk_app = typer.Typer(help="Disk usage.")
app.add_typer(ingest_app, name="ingest")
app.add_typer(disk_app, name="disk")

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _is_local(url: str) -> bool:
    from urllib.parse import urlparse

    return (urlparse(url).hostname or "") in LOCAL_HOSTS


@ingest_app.command("run")
def ingest_run(
    venue: Annotated[str, typer.Option(help="Venue id, e.g. binance-usdm.")],
    config: ConfigOpt = None,
    seconds: Annotated[float | None, typer.Option(help="Stop after N seconds.")] = None,
    metrics_port: Annotated[
        int | None, typer.Option(help="Override metrics port; 0 = off.")
    ] = None,
) -> None:
    """Long-running ingestor for one venue (the container command).

    While config capture.mode is 'shared' (owner decision: one shared crypto capture), it
    refuses any websocket or REST endpoint that is not on this host.

    Example: tickdb ingest run --venue binance-usdm
    """
    import signal

    from tickdb import logs
    from tickdb.ingest.metrics import Metrics
    from tickdb.ingest.spool import SpoolWriter, recover_partials
    from tickdb.ingest.venues.registry import INGESTORS

    settings, cfg = _load(config)
    if venue not in cfg.venues or venue not in INGESTORS:
        typer.echo(f"unknown venue {venue!r}", err=True)
        raise typer.Exit(EXIT_CONFIG)
    vcfg = cfg.venues[venue]
    if cfg.capture.mode == "shared" and not (_is_local(vcfg.ws_url) and _is_local(vcfg.rest_url)):
        typer.echo(
            "refused: capture.mode is 'shared', so live data comes from the one shared capture "
            "and this project never runs a second recorder of the venue (docs/decisions.md)",
            err=True,
        )
        raise typer.Exit(EXIT_CONFIG)
    logs.configure(settings.tickdb_log_level, settings.tickdb_log_json)
    data_dir = Path(cfg.data_root).expanduser()
    recover_partials(data_dir, venue)
    metrics = Metrics()
    port = vcfg.metrics_port if metrics_port is None else metrics_port
    if port:
        metrics.serve(port)

    def probe() -> float | None:
        try:
            return clock.probe(settings.ntp_server).offset_ms
        except Exception:
            return None

    async def main() -> None:
        spool = SpoolWriter(
            data_dir,
            venue,
            flush_interval_s=cfg.spool.flush_interval_ms / 1000,
            level=cfg.spool.zstd_level,
            queue_max=cfg.spool.queue_max_frames,
            on_depth=metrics.spool_queue_depth.labels(venue).set,
            on_bytes=metrics.spool_bytes.labels(venue).inc,
        )
        ingestor = INGESTORS[venue](vcfg, cfg, spool, metrics, clock_probe=probe, data_dir=data_dir)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):  # Windows
                loop.add_signal_handler(sig, ingestor.stop)
        await ingestor.run(until=asyncio.sleep(seconds) if seconds else None)

    asyncio.run(main())


@disk_app.command("project")
def disk_project(
    days: Annotated[str, typer.Option(help="Horizons in days, comma separated.")] = "30,90,365",
    dest: DestOpt = None,
    guard_path: Annotated[Path | None, typer.Option(help="Disk whose free space to check.")] = None,
    floor_gb: Annotated[float, typer.Option(help="Free-space floor.")] = 150.0,
    config: ConfigOpt = None,
) -> None:
    """Project disk use at 30/90/365 days from measured daily rates per store.

    Example: tickdb disk project --guard-path /mnt/d
    """
    import shutil

    from tickdb import diskproj

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    raw_days = cfg.retention.raw_spool_days
    stores = [
        diskproj.Store("raw mirror (shared)", Path("raw/external/lob-alpha"), raw_days),
        diskproj.Store("raw spool", Path("raw/spool"), raw_days),
        diskproj.Store("trades", Path("trades"), None),
        diskproj.Store("book_l2", Path("book_l2"), None),
        diskproj.Store("bbo", Path("bbo"), None),
        diskproj.Store("bars", Path("bars"), None),
    ]
    today = datetime.now(UTC).date()
    rates = [diskproj.measure(root, s, today) for s in stores]
    horizons = [int(d) for d in days.split(",")]
    free = shutil.disk_usage(guard_path).free if guard_path else None
    typer.echo(diskproj.render(rates, horizons, free, int(floor_gb * 1e9)))


if __name__ == "__main__":  # pragma: no cover
    app()
