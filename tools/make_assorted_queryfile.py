#!/usr/bin/env python3
"""
Reads any queryfile (pipe-delimited query|target) and writes an assorted version
where entries are grouped by their chunking predicate (the last WHERE col = ...
before the pipe), shuffled within each group, then round-robin interleaved across
groups so each wave of concurrent apps gets a healthy mix of query types.

Usage:
    python make_assorted_queryfile.py [--input queryfile.txt] [--output queryfile-assorted.txt] [--seed 42]
"""

import argparse
import re
import random
from collections import defaultdict

def extract_group_key(line: str) -> str:
    """Return (last-WHERE-column, target-table) so queries that filter on the same
    column but write to different targets are treated as separate pools."""
    parts = line.split("|")
    query = parts[0]
    target = parts[-1].strip() if len(parts) > 1 else ""
    matches = list(re.finditer(r'\bWHERE\b\s+(?:\w+\.)?(\w+)\s*=', query, re.IGNORECASE))
    col = matches[-1].group(1).upper() if matches else "__full_table__"
    return f"{col}|{target}"


def load_lines(path: str) -> list[str]:
    with open(path) as f:
        return [ln.rstrip("\n") for ln in f if ln.strip()]


def build_pools(lines: list[str]) -> dict[str, list[str]]:
    pools: dict[str, list[str]] = defaultdict(list)
    for line in lines:
        pools[extract_group_key(line)].append(line)
    return dict(pools)


def interleave(pools: dict[str, list[str]], seed: int) -> list[str]:
    rng = random.Random(seed)
    queues = [list(rng.sample(entries, len(entries))) for entries in pools.values()]
    result = []
    while True:
        active = [q for q in queues if q]
        if not active:
            break
        for q in active:
            result.append(q.pop(0))
    return result


def wave_summary(lines: list[str], pools: dict[str, list[str]], wave_size: int = 5) -> None:
    key_by_line = {line: extract_group_key(line) for line in lines}
    print(f"\nPool breakdown ({len(pools)} groups):")
    for key, entries in pools.items():
        print(f"  {key}: {len(entries)} entries")
    print(f"\nWave preview (wave size = {wave_size}):")
    for wave_num, start in enumerate(range(0, len(lines), wave_size), 1):
        wave = lines[start:start + wave_size]
        counts = defaultdict(int)
        for ln in wave:
            counts[key_by_line[ln]] += 1
        summary = ", ".join(f"{k}×{v}" for k, v in sorted(counts.items()))
        print(f"  Wave {wave_num:2d}: {summary}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="queryfile.txt", help="Source queryfile")
    parser.add_argument("--output", default="queryfile-assorted.txt", help="Output file")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for shuffle")
    args = parser.parse_args()

    lines = load_lines(args.input)
    pools = build_pools(lines)
    mixed = interleave(pools, seed=args.seed)

    with open(args.output, "w") as f:
        f.write("\n".join(mixed) + "\n")

    print(f"Read {len(lines)} lines from {args.input}")
    print(f"Written {len(mixed)} lines to {args.output}")
    wave_summary(mixed, pools)


if __name__ == "__main__":
    main()
