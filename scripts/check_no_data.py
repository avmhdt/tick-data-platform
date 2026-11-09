"""Pre-commit and CI guard: no data files and nothing over 2 MB outside tests/fixtures/."""

from __future__ import annotations

import sys
from pathlib import Path

BLOCKED_SUFFIXES = (".parquet", ".jsonl.zst", ".zst", ".feather", ".h5", ".itch", ".pcap")
MAX_BYTES = 2 * 1024 * 1024


def problems(paths: list[str]) -> list[str]:
    found = []
    for name in paths:
        path = Path(name)
        posix = path.as_posix()
        if posix.endswith(BLOCKED_SUFFIXES):
            found.append(f"{posix}: data file type is not allowed in git")
        elif posix.startswith("data/") or posix == "data":
            found.append(f"{posix}: data/ is never committed")
        elif (
            not posix.startswith("tests/fixtures/")
            and path.is_file()
            and path.stat().st_size > MAX_BYTES
        ):
            found.append(f"{posix}: larger than 2 MB outside tests/fixtures/")
    return found


def main(argv: list[str]) -> int:
    found = problems(argv)
    for line in found:
        print(line, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
