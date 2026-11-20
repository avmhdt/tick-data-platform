"""tickdb: tick capture, storage and query platform."""

from __future__ import annotations

import os

__version__ = "0.1.0"

# The host is shared: every tickdb process stays within its thread budget (8 by default,
# TICKDB_MAX_THREADS to change it). Thread pools read these at first import, so they are set
# here, before polars, pyarrow (OMP_NUM_THREADS), numpy or numba load. A lower value already in
# the environment wins.
MAX_THREADS = os.environ.get("TICKDB_MAX_THREADS", "8")
THREAD_VARS = (
    "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
    "NUMBA_NUM_THREADS", "POLARS_MAX_THREADS", "RAYON_NUM_THREADS",
)  # fmt: skip
for _var in THREAD_VARS:
    _have = os.environ.get(_var, "")
    if not _have.isdigit() or int(_have) > int(MAX_THREADS):
        os.environ[_var] = MAX_THREADS

__all__ = ["MAX_THREADS", "__version__", "connect"]


def connect(*args: object, **kwargs: object) -> object:
    """Shortcut for :func:`tickdb.client.connect` (imported lazily)."""
    from tickdb.client import connect as _connect

    return _connect(*args, **kwargs)  # type: ignore[arg-type]
