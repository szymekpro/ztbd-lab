"""Run all MariaDB benchmarks (INSERT/READ/UPDATE/DELETE) in one go.

This script is intentionally thin: it forwards only --scales and --both-index-modes
to the individual benchmark scripts and otherwise relies on their defaults.

Examples:
  # Use each benchmark's default scales
  python mariadb/run_all_mariadb_benchmarks.py --both-index-modes

  # Run all benchmarks for a single scale
  python mariadb/run_all_mariadb_benchmarks.py --scales 1000000 --both-index-modes
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _run_one(script_path: Path, forwarded_args: list[str], *, cwd: Path) -> None:
    cmd = [sys.executable, str(script_path), *forwarded_args]
    print(f"\n=== Uruchomiono: {script_path.name} ===")
    print(" ".join(cmd))
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    subprocess.run(cmd, check=True, cwd=str(cwd), env=env)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Runs all MariaDB benchmark suites sequentially: "
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

    parser.add_argument(
        "--seed-value",
        "--seed",
        dest="seed_value",
        type=int,
        default=None,
        help="Seed for benchmark data/sampling (forwarded to child scripts as --seed-value).",
    )

    args = parser.parse_args()

    forwarded_args: list[str] = []
    if args.scales:
        forwarded_args += ["--scales", args.scales]
    if args.both_index_modes:
        forwarded_args.append("--both-index-modes")
    if args.seed_value is not None:
        forwarded_args += ["--seed-value", str(args.seed_value)]

    mariadb_dir = Path(__file__).resolve().parent
    repo_root = mariadb_dir.parent

    scripts = [
        mariadb_dir / "benchmark_mariadb_insert_scenarios.py",
        mariadb_dir / "benchmark_mariadb_read_scenarios.py",
        mariadb_dir / "benchmark_mariadb_update_scenarios.py",
        mariadb_dir / "benchmark_mariadb_delete_scenarios.py",
    ]

    missing = [p for p in scripts if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Brakuje skryptow: {', '.join(str(p) for p in missing)}")

    try:
        for script in scripts:
            _run_one(script, forwarded_args, cwd=repo_root)
    except KeyboardInterrupt:
        print("\nInterrupted by user (Ctrl+C).")
        return 130

    print("\nBenchmarki skonczone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
