"""Run ALL database benchmarks (Cassandra/MariaDB/PostgreSQL/MongoDB) from repo root.

This script is intentionally thin. It forwards only:
- --scales
- --both-index-modes
- --seed-value

to the per-database run_all wrappers.

Examples:
  ./env/Scripts/python.exe run_all_benchmarks.py --both-index-modes --seed 123
  ./env/Scripts/python.exe run_all_benchmarks.py --scales 500000,1000000 --both-index-mode --seed-value 7
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _run_one(script_path: Path, forwarded_args: list[str], *, cwd: Path) -> None:
    cmd = [sys.executable, str(script_path), *forwarded_args]
    print(f"\n=== Uruchomiono: {script_path.as_posix()} ===")
    print(" ".join(cmd))

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")

    subprocess.run(cmd, check=True, cwd=str(cwd), env=env)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Runs ALL benchmark suites sequentially across databases: Cassandra, MariaDB, PostgreSQL, MongoDB. "
            "Only forwards --scales, --both-index-modes and --seed-value; everything else uses defaults."
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
        help="Seed for benchmark data/sampling (forwarded as --seed-value).",
    )

    args = parser.parse_args()

    forwarded_args: list[str] = []
    if args.scales:
        forwarded_args += ["--scales", args.scales]
    if args.both_index_modes:
        forwarded_args.append("--both-index-modes")
    if args.seed_value is not None:
        forwarded_args += ["--seed-value", str(args.seed_value)]

    repo_root = Path(__file__).resolve().parent

    # Ensure default results directories exist (benchmarks write into these by default).
    for results_dir in [
        repo_root / "cassandra" / "results",
        repo_root / "mariadb" / "results",
        repo_root / "postgres" / "results",
        repo_root / "mongodb" / "results",
    ]:
        results_dir.mkdir(parents=True, exist_ok=True)

    scripts = [
        repo_root / "mariadb" / "run_all_mariadb_benchmarks.py",
        repo_root / "postgres" / "run_all_psql_benchmarks.py",
        repo_root / "mongodb" / "run_all_mongodb_benchmarks.py",
        repo_root / "cassandra" / "run_all_cassandra_benchmarks.py",
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
