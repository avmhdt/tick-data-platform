from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from tickdb import timeutil as tu


def test_int_passes_through() -> None:
    assert tu.to_ns(1_730_246_400_123_456_789) == 1_730_246_400_123_456_789


def test_date_is_utc_midnight() -> None:
    assert tu.to_ns(date(2024, 10, 30)) == 1_730_246_400 * tu.NS_PER_S


def test_datetime_with_offset() -> None:
    plus3 = timezone(timedelta(hours=3))
    moment = datetime(2024, 10, 30, 3, 0, 0, 5, tzinfo=plus3)
    assert tu.to_ns(moment) == 1_730_246_400 * tu.NS_PER_S + 5_000


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        tu.to_ns(datetime(2024, 1, 1))


def test_bool_rejected() -> None:
    with pytest.raises(TypeError):
        tu.to_ns(True)


def test_unsupported_type() -> None:
    with pytest.raises(TypeError):
        tu.to_ns(1.5)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2024-10-30", 1_730_246_400 * tu.NS_PER_S),
        ("2024-10-30T00:00:01Z", 1_730_246_401 * tu.NS_PER_S),
        ("2024-10-30T00:00:01", 1_730_246_401 * tu.NS_PER_S),
        ("2024-10-30T01:00:00+01:00", 1_730_246_400 * tu.NS_PER_S),
    ],
)
def test_strings(text: str, expected: int) -> None:
    assert tu.to_ns(text) == expected


def test_iso_to_ns_keeps_nanoseconds() -> None:
    assert tu.iso_to_ns("2024-10-30T00:00:00.123456789Z") == 1_730_246_400_123_456_789
    assert tu.iso_to_ns("2024-10-30T00:00:00.5Z") == 1_730_246_400_500_000_000
    assert tu.iso_to_ns("2024-10-30T00:00:00Z") == 1_730_246_400_000_000_000


def test_iso_to_ns_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="RFC 3339"):
        tu.iso_to_ns("yesterday")


def test_exact_unit_scaling() -> None:
    assert tu.ms_to_ns(1_730_246_400_123) == 1_730_246_400_123_000_000
    assert tu.us_to_ns(1_730_246_400_123_456) == 1_730_246_400_123_456_000


def test_day_bounds_and_utc_date() -> None:
    start, end = tu.day_bounds("2024-10-30")
    assert end - start == tu.NS_PER_DAY
    assert tu.utc_date(start) == date(2024, 10, 30)
    assert tu.utc_date(end - 1) == date(2024, 10, 30)
    assert tu.utc_date(end) == date(2024, 10, 31)
    assert tu.day_bounds(date(2024, 10, 30)) == (start, end)


def test_ns_to_datetime() -> None:
    assert tu.ns_to_datetime(1_730_246_400_000_001_999) == datetime(
        2024, 10, 30, 0, 0, 0, 1, tzinfo=UTC
    )


@pytest.mark.parametrize(
    ("text", "ns"),
    [
        ("500ms", 500_000_000),
        ("5s", 5 * tu.NS_PER_S),
        ("1m", 60 * tu.NS_PER_S),
        ("23h", 82_800 * tu.NS_PER_S),
    ],
)
def test_durations(text: str, ns: int) -> None:
    assert tu.parse_duration_ns(text) == ns


def test_bad_duration() -> None:
    with pytest.raises(ValueError, match="bad duration"):
        tu.parse_duration_ns("5 minutes")
