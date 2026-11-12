"""The ``tickdb`` command line (spec section 10).

Exit codes: 0 success, 1 failure, 2 verification or DQ mismatch, 3 configuration or
prerequisite missing.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable
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


# ---------------------------------------------------------------- compaction (M3)

DateOpt = Annotated[str, typer.Option("--date", help="UTC date, YYYY-MM-DD.")]
VenueOpt = Annotated[str | None, typer.Option("--venue", help="One venue; default all enabled.")]


def _venue_list(cfg: TickdbConfig, venue: str | None) -> list[str]:
    if venue is None:
        return list(cfg.enabled_venues())
    if venue not in cfg.venues:
        typer.echo(f"unknown venue {venue!r}", err=True)
        raise typer.Exit(EXIT_CONFIG)
    return [venue]


def _raw_root(data_root: Path, raw: Path | None) -> Path:
    return raw or data_root / "raw" / "external" / "lob-alpha"


@app.command("compact")
def compact_cmd(
    date_: DateOpt,
    venue: VenueOpt = None,
    dest: DestOpt = None,
    raw: Annotated[Path | None, typer.Option(help="Raw root; default the shared mirror.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Spool -> Parquet for one UTC day (daily final partitions, manifests, gaps).

    Example: tickdb compact --date 2026-11-09 --venue kraken
    """
    from datetime import date as _date

    from tickdb.compact.compactor import compact_day

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    day = _date.fromisoformat(date_)
    for name in _venue_list(cfg, venue):
        vcfg = cfg.venues[name]
        result = compact_day(
            _raw_root(root, raw), root, name, day, tuple(vcfg.symbols),
            kraken_depth=vcfg.book.depth or 1000,
        )  # fmt: skip
        typer.echo(
            json.dumps(
                {
                    "venue": name,
                    "date": date_,
                    "seconds": result.seconds,
                    "gaps": result.gaps,
                    "rows": {k: v["rows"] for k, v in result.partitions.items()},
                    "counters": result.counters,
                }
            )
        )


@app.command("replay")
def replay_cmd(
    date_: DateOpt,
    verify: Annotated[bool, typer.Option("--verify/--no-verify", help="Compare hashes.")] = True,
    keep_scratch: Annotated[bool, typer.Option(help="Keep the rebuilt partitions.")] = False,
    venue: VenueOpt = None,
    dest: DestOpt = None,
    raw: Annotated[Path | None, typer.Option(help="Raw root; default the shared mirror.")] = None,
    config: ConfigOpt = None,
) -> None:
    """Rebuild a day from the raw spool into scratch and compare content hashes (FR-10).

    Exit 2 if any partition differs from the archive.

    Example: tickdb replay --date 2026-11-09 --verify
    """
    from datetime import date as _date

    from tickdb.compact.compactor import replay_verify

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    day = _date.fromisoformat(date_)
    ok = True
    for name in _venue_list(cfg, venue):
        vcfg = cfg.venues[name]
        report = replay_verify(
            _raw_root(root, raw), root, name, day, tuple(vcfg.symbols),
            keep_scratch=keep_scratch, kraken_depth=vcfg.book.depth or 1000,
        )  # fmt: skip
        from tickdb.retention import record_verify

        record_verify(root, name, day, report)
        typer.echo(json.dumps(report))
        ok &= report["ok"]
    raise typer.Exit(EXIT_OK if ok or not verify else EXIT_MISMATCH)


@app.command("load")
def load_cmd(
    date_: DateOpt,
    venue: VenueOpt = None,
    dest: DestOpt = None,
    config: ConfigOpt = None,
) -> None:
    """Parquet -> ClickHouse for one UTC day via REPLACE PARTITION, with a post-load check.

    Exit 2 if a ClickHouse fingerprint differs from the Parquet files.

    Example: tickdb load --date 2026-11-12
    """
    from datetime import date as _date

    from tickdb.load import clickhouse as ch

    settings, cfg = _load(config)
    if settings.clickhouse_password is None:
        typer.echo("CLICKHOUSE_PASSWORD is not set in .env (P-06)", err=True)
        raise typer.Exit(EXIT_CONFIG)
    root = dest or Path(cfg.data_root).expanduser()
    client = ch.connect_settings(settings)
    ch.apply_schema(client)
    ok = True
    for name in _venue_list(cfg, venue):
        report = ch.load_venue_day(
            client, root, name, _date.fromisoformat(date_), tuple(cfg.venues[name].symbols)
        )
        rows = {t: v["rows"] for t, v in report.tables.items()}
        typer.echo(json.dumps({"venue": name, "date": date_, "ok": report.ok, "rows": rows}))
        ok &= report.ok
    raise typer.Exit(EXIT_OK if ok else EXIT_MISMATCH)


dq_app = typer.Typer(help="Data-quality reports.")
app.add_typer(dq_app, name="dq")


@dq_app.command("report")
def dq_report(
    date_: DateOpt,
    out: Annotated[Path, typer.Option(help="Report folder.")] = Path("reports/dq"),
    dest: DestOpt = None,
    config: ConfigOpt = None,
) -> None:
    """Daily gap report: reports/dq/YYYY-MM-DD.{md,csv,json} (aggregates only).

    Example: tickdb dq report --date 2026-11-12
    """
    from datetime import date as _date

    from tickdb.dq import report

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    summary = report.write_day(root, _date.fromisoformat(date_), out)
    typer.echo(json.dumps(summary))


@dq_app.command("summary")
def dq_summary(
    from_: FromOpt,
    to: Annotated[str, typer.Option("--to", help="Last UTC date (inclusive).")],
    out: Annotated[Path, typer.Option(help="Report folder.")] = Path("reports/dq"),
) -> None:
    """Rolling summary reports/dq/summary.md from the daily reports.

    Example: tickdb dq summary --from 2026-11-12 --to 2026-12-11
    """
    from datetime import date as _date

    from tickdb.dq import report

    path = report.write_summary(out, _date.fromisoformat(from_), _date.fromisoformat(to))
    typer.echo(str(path))


@app.command("backfill")
def backfill_cmd(
    date_: DateOpt,
    venue: VenueOpt = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Only list what is missing.")] = False,
    dest: DestOpt = None,
    config: ConfigOpt = None,
) -> None:
    """REST backfill of the open trade gaps of a compacted day (FR-05); then re-run compact.

    Example: tickdb backfill --date 2026-11-12 --venue kraken
    """
    from datetime import date as _date

    from tickdb.ingest import backfill as bf

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    day = _date.fromisoformat(date_)
    for name in _venue_list(cfg, venue):
        needs = bf.plan(root, name, day)
        report = {
            "venue": name,
            "gaps": len(needs),
            "missing_ids": sum(n.hi - n.lo + 1 for n in needs),
        }
        if dry_run or not needs:
            typer.echo(json.dumps(report))
            continue
        received, requests, spool = asyncio.run(_backfill(bf, name, cfg, needs, root, day))
        report |= {"received": received, "requests": requests, "spool": spool}
        typer.echo(json.dumps(report))


async def _backfill(
    bf: Any, name: str, cfg: TickdbConfig, needs: list[Any], root: Path, day: Any
) -> tuple[int, int, str]:
    from datetime import timedelta

    filler = bf.Backfiller(name, cfg.venues[name].rest_url)
    received = 0
    for need in needs:
        got = await filler.fetch(need)
        if got < need.hi - need.lo + 1 and name == "binance-usdm":
            got = max(got, filler.vision(need, root, [day - timedelta(days=1), day]))
        received += got
    await filler.http.aclose()
    for error in filler.errors:
        typer.echo(f"backfill note: {error}", err=True)
    return received, filler.requests, str(filler.write(root, day))


retention_app = typer.Typer(help="Retention (raw kept 7 days after verification, D-10).")
scheduler_app = typer.Typer(help="Daily job.")
app.add_typer(retention_app, name="retention")
app.add_typer(scheduler_app, name="scheduler")


@retention_app.command("apply")
def retention_apply(
    dry_run: Annotated[bool, typer.Option("--dry-run", help="List only.")] = False,
    dest: DestOpt = None,
    config: ConfigOpt = None,
) -> None:
    """Delete raw venue-days past retention whose replay check passed (never others).

    Example: tickdb retention apply --dry-run
    """
    from tickdb import retention

    _, cfg = _load(config)
    root = dest or Path(cfg.data_root).expanduser()
    plan = retention.apply(root, cfg.retention.raw_spool_days, dry_run=dry_run)
    report = {
        "dry_run": dry_run,
        "delete": [str(p.relative_to(root)) for p in plan.delete],
        "bytes": plan.bytes,
        "kept_unverified": plan.keep_unverified,
    }
    typer.echo(json.dumps(report))


@scheduler_app.command("run")
def scheduler_run(
    once: Annotated[str, typer.Option(help="Job to run once: daily.")] = "daily",
    date_: Annotated[str | None, typer.Option("--date", help="Day; default yesterday UTC.")] = None,
    reports: Annotated[Path, typer.Option(help="Gap report folder.")] = Path("reports/dq"),
    config: ConfigOpt = None,
) -> None:
    """Run the daily job for D-1 (compact, backfill, load, replay, report, retention).

    Example: tickdb scheduler run --once daily
    """
    from datetime import date as _date
    from datetime import timedelta

    from tickdb import retention, scheduler
    from tickdb.compact.compactor import compact_day, replay_verify
    from tickdb.dq import report
    from tickdb.ingest import backfill as bf

    if once != "daily":
        typer.echo("only the daily job runs here; the hourly step is the mirror's cron", err=True)
        raise typer.Exit(EXIT_CONFIG)
    settings, cfg = _load(config)
    root = Path(cfg.data_root).expanduser()
    raw = root / "raw" / "external" / "lob-alpha"
    day = _date.fromisoformat(date_) if date_ else datetime.now(UTC).date() - timedelta(days=1)
    log = Path("results") / "job_log.csv"
    results = {}
    with scheduler.job_lock(root / "meta" / "scheduler" / ".lock") as locked:
        if not locked:
            typer.echo("another daily job holds the lock")
            raise typer.Exit(EXIT_OK)
        for name in cfg.enabled_venues():
            vcfg = cfg.venues[name]
            symbols = tuple(vcfg.symbols)
            depth = vcfg.book.depth or 1000
            fetched = {"rows": 0}

            def compact(
                name: str = name, symbols: tuple[str, ...] = symbols, depth: int = depth
            ) -> scheduler.StepResult:
                r = compact_day(raw, root, name, day, symbols, kraken_depth=depth)
                return scheduler.StepResult(
                    "compact", bool(r.partitions), f"{r.seconds}s gaps={r.gaps}"
                )

            def backfill(
                name: str = name, fetched: dict[str, int] = fetched
            ) -> scheduler.StepResult:
                needs = bf.plan(root, name, day)
                if not needs:
                    return scheduler.StepResult("backfill", True, "no open trade gaps")
                got, requests, _ = asyncio.run(_backfill(bf, name, cfg, needs, root, day))
                fetched["rows"] = got
                return scheduler.StepResult("backfill", True, f"received={got} requests={requests}")

            def recompact(
                name: str = name,
                symbols: tuple[str, ...] = symbols,
                depth: int = depth,
                fetched: dict[str, int] = fetched,
            ) -> scheduler.StepResult:
                if not fetched["rows"]:
                    return scheduler.StepResult("recompact", True, "nothing fetched", skipped=True)
                r = compact_day(raw, root, name, day, symbols, kraken_depth=depth)
                return scheduler.StepResult("recompact", True, f"{r.seconds}s gaps={r.gaps}")

            def load(name: str = name, symbols: tuple[str, ...] = symbols) -> scheduler.StepResult:
                if settings.clickhouse_password is None:
                    return scheduler.StepResult(
                        "load", False, "ClickHouse not configured", skipped=True
                    )
                from tickdb.load import clickhouse as ch

                client = ch.connect_settings(settings)
                ch.apply_schema(client)
                rep = ch.load_venue_day(client, root, name, day, symbols)
                return scheduler.StepResult(
                    "load", rep.ok, json.dumps({t: v["rows"] for t, v in rep.tables.items()})
                )

            def replay(
                name: str = name, symbols: tuple[str, ...] = symbols, depth: int = depth
            ) -> scheduler.StepResult:
                rep = replay_verify(raw, root, name, day, symbols, kraken_depth=depth)
                retention.record_verify(root, name, day, rep)
                return scheduler.StepResult(
                    "replay", bool(rep["ok"]), f"partitions={len(rep['partitions'])}"
                )

            actions: dict[str, Callable[..., scheduler.StepResult]] = {
                "compact": compact,
                "backfill": backfill,
                "recompact": recompact,
                "load": load,
                "replay": replay,
            }
            results[name] = scheduler.daily_steps(name, day, actions, log)
        report.write_day(root, day, reports)
        report.write_summary(reports, _date(2026, 11, 12), day)
        retention.apply(root, cfg.retention.raw_spool_days, dry_run=False)
    typer.echo(scheduler.status_line(results))
