"""Run all MongoDB benchmarks (INSERT/READ/UPDATE/DELETE) sequentially.

This mirrors postgres/run_all_psql_benchmarks.py.
It forwards only:
- --scales
- --both-index-modes

to individual benchmark scripts.

Examples:
  python mongodb/run_all_mongodb_benchmarks.py --both-index-modes
  python mongodb/run_all_mongodb_benchmarks.py --scales 1000000 --both-index-modes
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _run_one(module_name: str, forwarded_args: list[str], *, cwd: Path) -> None:
    cmd = [sys.executable, "-m", module_name, *forwarded_args]
    print(f"\n=== Uruchomiono: {module_name} ===")
    print(" ".join(cmd))
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    subprocess.run(cmd, check=True, cwd=str(cwd), env=env)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Runs all MongoDB benchmark suites sequentially: INSERT, READ, UPDATE, DELETE. "
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

    mongo_dir = Path(__file__).resolve().parent
    repo_root = mongo_dir.parent

    init_file = mongo_dir / "__init__.py"
    if not init_file.exists():
        raise FileNotFoundError(
            "Brakuje mongodb/__init__.py (katalog mongodb musi byc pakietem, by dzialalo python -m mongodb.*)."
        )

    modules = [
        "mongodb.benchmark_mongodb_insert_scenarios",
        "mongodb.benchmark_mongodb_read_scenarios",
        "mongodb.benchmark_mongodb_update_scenarios",
        "mongodb.benchmark_mongodb_delete_scenarios",
    ]

    try:
        for module in modules:
            _run_one(module, forwarded_args, cwd=repo_root)
    except KeyboardInterrupt:
        print("\nInterrupted by user (Ctrl+C).")
        return 130

    print("\nBenchmarki skonczone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
