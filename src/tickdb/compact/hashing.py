"""Content hash of a dataset partition (FR-10).

SHA-256 over the Arrow IPC stream of the table in archive order, re-chunked into record
batches of exactly ``CHUNK_ROWS`` rows with the dataset's fixed schema, so the hash does not
depend on how the rows were batched when they were produced. Idempotency is claimed on this
hash, not on Parquet file bytes (R-12).
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

CHUNK_ROWS = 65_536


class _HashSink:
    def __init__(self) -> None:
        self.digest = hashlib.sha256()
        self.closed = False

    def write(self, data: bytes | memoryview) -> int:
        self.digest.update(data)
        return len(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class StreamHasher:
    """Feed record batches in archive order; ``hexdigest()`` when done."""

    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema
        self._sink = _HashSink()
        self._writer = ipc.new_stream(pa.PythonFile(self._sink, mode="w"), schema)
        self._pending: list[pa.RecordBatch] = []
        self._pending_rows = 0
        self.rows = 0
        self._done: str | None = None

    def update(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        self._pending.append(batch.cast(self.schema) if batch.schema != self.schema else batch)
        self._pending_rows += batch.num_rows
        self.rows += batch.num_rows
        while self._pending_rows >= CHUNK_ROWS:
            self._emit(CHUNK_ROWS)

    def _emit(self, n: int) -> None:
        table = pa.Table.from_batches(self._pending, schema=self.schema)
        head, tail = table.slice(0, n), table.slice(n)
        self._writer.write_batch(head.combine_chunks().to_batches()[0])
        self._pending = tail.to_batches() if tail.num_rows else []
        self._pending_rows = tail.num_rows

    def hexdigest(self) -> str:
        if self._done is None:
            if self._pending_rows:
                self._emit(self._pending_rows)
            self._writer.close()
            self._done = self._sink.digest.hexdigest()
        return self._done


def content_hash(table: pa.Table, sort_by: Sequence[str] = ()) -> str:
    """SHA-256 hex of ``table`` (stably sorted by ``sort_by`` first, if given)."""
    if sort_by:
        indices = pc.sort_indices(table, sort_keys=[(k, "ascending") for k in sort_by])
        table = table.take(indices)
    hasher = StreamHasher(table.schema)
    for batch in table.to_batches():
        hasher.update(batch)
    return hasher.hexdigest()
