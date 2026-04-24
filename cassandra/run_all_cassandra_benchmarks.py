"""Run all Cassandra benchmarks (INSERT/READ/UPDATE/DELETE) in one go.

This script is intentionally thin: it forwards only --scales and --both-index-modes
to the individual benchmark scripts and otherwise relies on their defaults.

Examples:
  # Use each benchmark's default scales
  python cassandra/run_all_cassandra_benchmarks.py --both-index-modes

  # Run all benchmarks for a single scale
  python cassandra/run_all_cassandra_benchmarks.py --scales 1000000 --both-index-modes
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _run_one(script_path: Path, forwarded_args: list[str]) -> None:
    cmd = [sys.executable, str(script_path), *forwarded_args]
    print(f"\n=== Uruchomiono: {script_path.name} ===")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Runs all Cassandra benchmark suites sequentially: "
            "INSERT, READ, UPDATE, DELETE. "
            "Only forwards --scales and --both-index-modes; everything else uses defaults."
        )
    )

    parser.add_argument(
        "--scales",
        default=None,
        help=(
            "Comma-separated scales (e.g. 500000,1000000,10000000). "
            "If omitted, each benchmark script uses its own defaults."
        ),
    )

    parser.add_argument(
        "--both-index-modes",
        "--both-index-mode",
        dest="both_index_modes",
        action="store_true",
        help="Run each benchmark in both modes (no_indexes + with_indexes) into a single CSV.",
    )

    args = parser.parse_args()

    forwarded_args: list[str] = []
    if args.scales:
        forwarded_args += ["--scales", args.scales]
    if args.both_index_modes:
        forwarded_args.append("--both-index-modes")

    cassandra_dir = Path(__file__).resolve().parent

    scripts = [
        cassandra_dir / "benchmark_cassandra_insert_scenarios.py",
        cassandra_dir / "benchmark_cassandra_read_scenarios.py",
        cassandra_dir / "benchmark_cassandra_update_scenarios.py",
        cassandra_dir / "benchmark_cassandra_delete_scenarios.py",
    ]

    missing = [p for p in scripts if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Brakuje skryptow: {', '.join(str(p) for p in missing)}")

    try:
        for script in scripts:
            _run_one(script, forwarded_args)
    except KeyboardInterrupt:
        print("\nInterrupted by user (Ctrl+C).")
        return 130

    print("\nBenchmarki skonczone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
