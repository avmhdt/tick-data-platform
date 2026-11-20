"""Every tickdb process stays within the shared host's thread budget."""

from __future__ import annotations

import json
import os
import subprocess
import sys

CODE = """
import json, os
import tickdb, polars, duckdb
from tickdb.dq import daily  # noqa: F401  (uses duckdb with the cap)
con = duckdb.connect(config={"threads": int(tickdb.MAX_THREADS)})
print(json.dumps({
    "env": {v: os.environ[v] for v in tickdb.THREAD_VARS},
    "polars": polars.thread_pool_size(),
    "duckdb": con.execute("SELECT current_setting('threads')").fetchone()[0],
}))
"""


def run(extra: dict[str, str]) -> dict[str, object]:
    env = {k: v for k, v in os.environ.items() if not k.endswith("_THREADS")}
    env.pop("TICKDB_MAX_THREADS", None)
    env.update(extra)
    out = subprocess.run(
        [sys.executable, "-c", CODE], env=env, capture_output=True, text=True, check=True
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_defaults_cap_every_pool_at_eight() -> None:
    got = run({"OMP_NUM_THREADS": "64"})
    assert set(got["env"].values()) == {"8"}  # type: ignore[union-attr]
    assert got["polars"] <= 8  # type: ignore[operator]
    assert int(got["duckdb"]) <= 8  # type: ignore[call-overload]


def test_a_lower_value_wins() -> None:
    got = run({"POLARS_MAX_THREADS": "2", "TICKDB_MAX_THREADS": "4"})
    env = got["env"]
    assert env["POLARS_MAX_THREADS"] == "2"  # type: ignore[index]
    assert env["OMP_NUM_THREADS"] == "4"  # type: ignore[index]
    assert got["polars"] == 2
