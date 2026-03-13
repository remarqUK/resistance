from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=0)
    args = parser.parse_args()

    path = Path(args.path)
    lines = path.read_text(encoding="utf-8").splitlines()
    start = max(args.start, 1)
    end = args.end or len(lines)
    for lineno in range(start, min(end, len(lines)) + 1):
        print(f"{lineno:4}: {lines[lineno - 1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
