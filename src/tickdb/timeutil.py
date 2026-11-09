"""Integer-nanosecond time helpers. Stored data never uses float seconds (NFR-06)."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

NS_PER_US = 1_000
NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000
NS_PER_DAY = 86_400 * NS_PER_S

TimeLike = str | date | datetime | int

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_DURATION = re.compile(r"^\s*(\d+)\s*(ns|us|ms|s|m|h|d)\s*$")
_UNIT_NS = {
    "ns": 1,
    "us": NS_PER_US,
    "ms": NS_PER_MS,
    "s": NS_PER_S,
    "m": 60 * NS_PER_S,
    "h": 3_600 * NS_PER_S,
    "d": NS_PER_DAY,
}


def _datetime_ns(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError(f"naive datetime {moment.isoformat()}: pass a UTC-aware datetime")
    delta = moment.astimezone(UTC) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * NS_PER_S + delta.microseconds * NS_PER_US


def to_ns(value: TimeLike) -> int:
    """Convert a TimeLike to int ns since the epoch, UTC.

    ``int`` is taken as ns already. ``date`` means its UTC midnight. Strings are ISO-8601;
    a string without an offset is read as UTC, and ``Z`` is accepted.
    """
    if isinstance(value, bool):
        raise TypeError("bool is not a time")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        return _datetime_ns(value)
    if isinstance(value, date):
        return _datetime_ns(datetime(value.year, value.month, value.day, tzinfo=UTC))
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10:
            return to_ns(date.fromisoformat(text))
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return _datetime_ns(moment)
    raise TypeError(f"unsupported time value: {value!r}")


def ns_to_datetime(ns: int) -> datetime:
    """UTC datetime for ``ns`` (microsecond precision; the ns remainder is dropped)."""
    return _EPOCH + timedelta(microseconds=ns // NS_PER_US)


def utc_date(ns: int) -> date:
    """UTC calendar date containing ``ns``."""
    return ns_to_datetime(ns).date()


def day_bounds(day: date | str) -> tuple[int, int]:
    """Half-open ns bounds ``[start, end)`` of a UTC day."""
    start = to_ns(day if isinstance(day, date) else date.fromisoformat(day))
    return start, start + NS_PER_DAY


def ms_to_ns(ms: int) -> int:
    """Exact scaling of a venue millisecond timestamp."""
    return ms * NS_PER_MS


def us_to_ns(us: int) -> int:
    """Exact scaling of a venue microsecond timestamp."""
    return us * NS_PER_US


def iso_to_ns(text: str) -> int:
    """Parse an RFC 3339 timestamp with up to 9 fractional digits, exactly, to int ns.

    ``datetime.fromisoformat`` keeps only microseconds, so the fraction is handled here.
    """
    match = re.match(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})?$",
        text.strip(),
    )
    if match is None:
        raise ValueError(f"not an RFC 3339 timestamp: {text!r}")
    head, frac, offset = match.groups()
    moment = datetime.fromisoformat(head + (offset or "Z").replace("Z", "+00:00"))
    whole = _datetime_ns(moment)
    return whole + int((frac or "").ljust(9, "0"))


def parse_duration_ns(text: str) -> int:
    """Parse ``"500ms"``, ``"5s"``, ``"1m"``, ``"23h"`` and similar into int ns."""
    match = _DURATION.match(text)
    if match is None:
        raise ValueError(f"bad duration {text!r}; use e.g. 500ms, 5s, 1m, 23h")
    return int(match.group(1)) * _UNIT_NS[match.group(2)]
