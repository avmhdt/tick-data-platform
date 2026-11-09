from __future__ import annotations

import pyarrow as pa
import pytest

from tickdb import schemas


@pytest.mark.parametrize("name", sorted(schemas.SCHEMAS))
def test_every_schema_is_arrow(name: str) -> None:
    schema = schemas.get(name)
    assert isinstance(schema, pa.Schema)
    assert len(set(schema.names)) == len(schema.names)


@pytest.mark.parametrize("name", sorted(schemas.CANONICAL))
def test_canonical_fields_come_first_in_spec_order(name: str) -> None:
    canonical = schemas.CANONICAL[name]
    assert tuple(schemas.get(name).names[: len(canonical)]) == canonical


def test_timestamps_are_int64_ns() -> None:
    for name, schema in schemas.SCHEMAS.items():
        for field in schema:
            if field.name.startswith("ts") or field.name in ("start_recv", "end_recv"):
                assert field.type == pa.int64(), (name, field.name)


def test_dedupe_keys_exist_in_schema() -> None:
    for name, keys in schemas.DEDUPE_KEYS.items():
        assert set(keys) <= set(schemas.get(name).names)


def test_trades_side_and_source() -> None:
    trades = schemas.TRADES
    assert trades.field("side").type == pa.int8()
    assert trades.field("ts_fetch").nullable
    assert schemas.TRADE_SOURCES[0] == "ws"


def test_bbo_sides_nullable_for_empty_book() -> None:
    for col in ("bid_px", "bid_qty", "ask_px", "ask_qty"):
        assert schemas.BBO.field(col).nullable


def test_unknown_dataset_lists_known() -> None:
    with pytest.raises(KeyError, match="known: "):
        schemas.get("nope")


def test_describe_marks_added_fields() -> None:
    text = schemas.describe("trades")
    assert text.splitlines()[0].startswith("trades")
    assert "source" in text
    assert any("added" in line and "ts_fetch" in line for line in text.splitlines())
    assert any("canonical" in line and "trade_id" in line for line in text.splitlines())


def test_empty_tables_build_from_every_schema() -> None:
    for schema in schemas.SCHEMAS.values():
        assert schema.empty_table().num_rows == 0
