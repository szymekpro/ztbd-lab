"""
MariaDB/MySQL INSERT benchmark suite mirroring benchmark_psql_insert_scenarios.py
- Uses `PyMySQL` (import pymysql) for DB connections (matches repo requirements.txt).
- Adapts PostgreSQL-specific SQL (RETURNING, generate_series, md5(clock_timestamp()))
  to MariaDB-compatible equivalents (LAST_INSERT_ID(), numbers sequences).

Usage: similar CLI flags as the PostgreSQL script. Default DB port set to 3306.

Notes:
- The bulk-insert implementation builds a derived numbers table with UNION ALL.
- Scenario intensity is configurable via CLI to increase real DB workload.
"""
import argparse
import os
import random
import string
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from statistics import mean
from typing import Optional

try:
    import pymysql
    from pymysql.connections import Connection as PyMySQLConnection
except Exception:  # pragma: no cover - helpful error if dependency missing at runtime
    pymysql = None
    PyMySQLConnection = object


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def connect_db(cfg: DbConfig) -> PyMySQLConnection:
    if pymysql is None:  # pragma: no cover
        raise RuntimeError("PyMySQL is required for MariaDB benchmarking. Install with: pip install PyMySQL")

    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.dbname,
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
        local_infile=True,
    )


def ensure_existing_schema(conn: PyMySQLConnection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ('artists', 'albums', 'tracks', 'track_artists', 'track_albums')
        """
    )
    found = int(cur.fetchone()[0])
    cur.close()
    if found < 5:
        raise RuntimeError("Required spotify schema/tables not found in current database. Run a MariaDB init script first.")


def _new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def prepare_scale_data_with_seed_script(
    cfg: DbConfig, target_rows: int, seed_value: Optional[int], pool_size: int
) -> None:
    import seed_mariadb_faker_data as seed_script

    seed_cfg = seed_script.DbConfig(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    with seed_script.connect_db(seed_cfg) as seed_conn:
        seed_script.seed_all(
            seed_conn,
            dbname=seed_cfg.dbname,
            n_genres=30,
            n_artists=max(50, target_rows // 20000),
            n_albums=max(80, target_rows // 10000),
            n_tracks=target_rows,
            seed=seed_value,
            truncate=True,
            pool_size=pool_size,
        )


def scenario_single_insert(conn: PyMySQLConnection, inserts_count: int = 100) -> int:
    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        for _ in range(inserts_count):
            cur.execute(
                """
                INSERT INTO tracks (
                    spotify_track_id,
                    name,
                    explicit,
                    duration_min,
                    disc_number,
                    track_number,
                    isrc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    _new_spotify_id("single"),
                    "Single Insert Track",
                    0,
                    3.141,
                    1,
                    1,
                    _new_isrc(),
                ),
            )
        conn.commit()
    finally:
        cur.close()
    return inserts_count


def scenario_complex_insert(conn: PyMySQLConnection, items_count: int = 5) -> int:
    cur = conn.cursor()
    inserted = 0
    try:
        cur.execute("START TRANSACTION")
        cur.execute(
            "INSERT INTO artists (name, raw_genres_text) VALUES (%s, %s)",
            (f"Complex Artist {uuid.uuid4().hex[:8]}", "bench,complex"),
        )
        cur.execute("SELECT LAST_INSERT_ID()")
        artist_id = int(cur.fetchone()[0])
        inserted += 1

        cur.execute(
            "INSERT INTO albums (spotify_album_id, name, album_type, release_date, total_tracks) VALUES (%s, %s, %s, CURDATE(), %s)",
            (_new_spotify_id("album"), f"Complex Album {uuid.uuid4().hex[:8]}", "album", items_count),
        )
        cur.execute("SELECT LAST_INSERT_ID()")
        album_id = int(cur.fetchone()[0])
        inserted += 1

        for idx in range(items_count):
            cur.execute(
                "INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    _new_spotify_id("cmp"),
                    f"Complex Track {idx + 1}",
                    0,
                    round(random.uniform(2.5, 6.5), 3),
                    1,
                    idx + 1,
                    _new_isrc(),
                ),
            )
            cur.execute("SELECT LAST_INSERT_ID()")
            track_id = int(cur.fetchone()[0])
            inserted += 1

            cur.execute(
                "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, TRUE)",
                (track_id, album_id),
            )
            inserted += 1

            cur.execute(
                "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (%s, %s, 1)",
                (track_id, artist_id),
            )
            inserted += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    return inserted


def scenario_bulk_insert(conn: PyMySQLConnection, bulk_size: int = 10000) -> int:
    # Use an inlined derived numbers table built with UNION ALL. This avoids WITH RECURSIVE
    # and parameterization that can cause syntax issues on some MariaDB versions/drivers.
    if bulk_size <= 0:
        return 0

    # Build a derived table like: (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL ... ) seq
    parts = ["SELECT 1 AS n"]
    # Keep string building in Python; for very large bulk_size this will be large but
    # the default is 10000 which is acceptable for a one-off benchmark insert.
    for i in range(2, bulk_size + 1):
        parts.append(f"UNION ALL SELECT {i}")

    seq_sql = "(" + "\n".join(parts) + ") seq"

    sql = f"""
    INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
    SELECT
      LEFT(MD5(CONCAT(UUID(), seq.n)), 22),
      CONCAT('Bulk Track ', seq.n),
      (RAND() > 0.5),
      ROUND((1.0 + RAND() * 8.0), 3),
      1,
      1 + (seq.n % 20),
      NULL
    FROM {seq_sql};
    """

    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    return bulk_size


def scenario_heavy_payload_insert(
    conn: PyMySQLConnection, payload_kb: int = 75, rows_count: int = 20
) -> int:
    payload_size = payload_kb * 1024
    heavy_text = "".join(random.choices(string.ascii_letters + string.digits + " ", k=payload_size))
    fitted_text = heavy_text[:500]

    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        for _ in range(rows_count):
            cur.execute(
                "INSERT INTO artists (name, raw_genres_text) VALUES (%s, %s)",
                (f"Heavy Payload Artist {uuid.uuid4().hex[:8]}", fitted_text),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return rows_count


def _concurrent_worker(cfg: DbConfig, ops_per_worker: int) -> int:
    with connect_db(cfg) as worker_conn:
        cur = worker_conn.cursor()
        try:
            cur.execute("START TRANSACTION")
            for _ in range(ops_per_worker):
                cur.execute(
                    "INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        _new_spotify_id("concurrent"),
                        "Concurrent Insert Track",
                        0,
                        4.200,
                        1,
                        1,
                        _new_isrc(),
                    ),
                )
            worker_conn.commit()
        finally:
            cur.close()
    return ops_per_worker


def scenario_concurrent_inserts(cfg: DbConfig, workers: int = 100, ops_per_worker: int = 5) -> int:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        inserted = list(pool.map(lambda _x: _concurrent_worker(cfg, ops_per_worker), range(workers)))
    return sum(inserted)


def scenario_upsert(conn: PyMySQLConnection, ops_count: int = 200, key_pool_size: int = 50) -> int:
    key_pool_size = max(1, min(key_pool_size, ops_count))
    key_pool = [_new_spotify_id("upsert") for _ in range(key_pool_size)]

    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        for idx in range(ops_count):
            spotify_track_id = key_pool[idx % key_pool_size]
            cur.execute(
                """
                INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  name = VALUES(name),
                  explicit = VALUES(explicit),
                  duration_min = VALUES(duration_min),
                  updated_at = NOW()
                """,
                (
                    spotify_track_id,
                    f"Upsert Track {idx}",
                    1 if (idx % 2) else 0,
                    2.5 + ((idx % 10) * 0.1),
                    1,
                    1 + (idx % 20),
                    _new_isrc(),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return ops_count


def timed_run(fn, *args, **kwargs) -> tuple[float, int]:
    start = time.perf_counter()
    ops = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, ops


def parse_scales(raw: str) -> list[int]:
    scales: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        value = int(text)
        if value <= 0:
            raise ValueError("Scale must be positive")
        scales.append(value)
    if not scales:
        raise ValueError("At least one scale is required")
    return scales


def run_benchmark(
    cfg: DbConfig,
    scales: list[int],
    runs_per_scenario: int,
    bulk_size: int,
    heavy_payload_kb: int,
    concurrent_workers: int,
    skip_prepare: bool,
    prepare_mode: str,
    seed_value: Optional[int],
    pool_size: int,
    single_ops_per_run: int,
    complex_items_count: int,
    heavy_rows_per_run: int,
    concurrent_ops_per_worker: int,
    upsert_ops_per_run: int,
    upsert_key_pool_size: int,
) -> list[dict]:
    results: list[dict] = []

    with connect_db(cfg) as conn:
        ensure_existing_schema(conn)

        for scale in scales:
            if not skip_prepare and prepare_mode == "seed-script":
                prepare_scale_data_with_seed_script(
                    cfg=cfg, target_rows=scale, seed_value=seed_value, pool_size=pool_size
                )

            scenarios = [
                ("single_insert", lambda: scenario_single_insert(conn, inserts_count=single_ops_per_run)),
                ("complex_insert", lambda: scenario_complex_insert(conn, items_count=complex_items_count)),
                ("bulk_insert", lambda: scenario_bulk_insert(conn, bulk_size=bulk_size)),
                (
                    "heavy_payload_insert",
                    lambda: scenario_heavy_payload_insert(
                        conn, payload_kb=heavy_payload_kb, rows_count=heavy_rows_per_run
                    ),
                ),
                (
                    "concurrent_inserts",
                    lambda: scenario_concurrent_inserts(
                        cfg, workers=concurrent_workers, ops_per_worker=concurrent_ops_per_worker
                    ),
                ),
                (
                    "upsert_insert_or_update",
                    lambda: scenario_upsert(
                        conn, ops_count=upsert_ops_per_run, key_pool_size=upsert_key_pool_size
                    ),
                ),
            ]

            for scenario_name, scenario_fn in scenarios:
                for run_idx in range(1, runs_per_scenario + 1):
                    elapsed, ops = timed_run(scenario_fn)
                    results.append(
                        {
                            "scale": scale,
                            "scenario": scenario_name,
                            "run": run_idx,
                            "seconds": elapsed,
                            "operations": ops,
                            "ops_per_sec": (ops / elapsed) if elapsed > 0 else None,
                        }
                    )

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str], list[dict]] = {}
    for row in results:
        key = (row["scale"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== Benchmark Summary (avg from runs) ===")
    print("scale | scenario | avg_seconds | avg_ops_per_sec")
    print("-" * 70)

    for (scale, scenario), rows in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        avg_seconds = mean(r["seconds"] for r in rows)
        valid_ops = [r["ops_per_sec"] for r in rows if r["ops_per_sec"] is not None]
        avg_ops = mean(valid_ops) if valid_ops else 0.0
        print(f"{scale} | {scenario} | {avg_seconds:.6f} | {avg_ops:.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    fieldnames = ["scale", "scenario", "run", "seconds", "operations", "ops_per_sec"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "MariaDB INSERT benchmark suite (6 scenariuszy) "
            "na istniejacym schemacie spotify: single, complex, bulk, heavy payload, concurrent, upsert."
        )
    )

    parser.add_argument("--scales", default="500000,1000000,10000000")
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--bulk-size", type=int, default=10000)
    parser.add_argument("--heavy-payload-kb", type=int, default=75)
    parser.add_argument("--concurrent-workers", type=int, default=100)
    parser.add_argument(
        "--prepare-mode",
        choices=["seed-script", "fast"],
        default="seed-script",
        help=(
            "seed-script: przygotowuje skale przez seed_mariadb_faker_data.py; "
            "fast: alias zachowany dla zgodnosci CLI"
        ),
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument("--single-ops-per-run", type=int, default=100)
    parser.add_argument("--complex-items-count", type=int, default=20)
    parser.add_argument("--heavy-rows-per-run", type=int, default=20)
    parser.add_argument("--concurrent-ops-per-worker", type=int, default=5)
    parser.add_argument("--upsert-ops-per-run", type=int, default=200)
    parser.add_argument("--upsert-key-pool-size", type=int, default=50)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie przygotowuje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="mariadb_insert_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3307")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))

    args = parser.parse_args()

    if args.runs_per_scenario <= 0:
        raise ValueError("runs-per-scenario must be > 0")
    if args.bulk_size <= 0:
        raise ValueError("bulk-size must be > 0")
    if args.heavy_payload_kb < 50 or args.heavy_payload_kb > 100:
        raise ValueError("heavy-payload-kb should be in range 50-100")
    if args.concurrent_workers <= 0:
        raise ValueError("concurrent-workers must be > 0")
    if args.pool_size <= 0:
        raise ValueError("pool-size must be > 0")
    if args.single_ops_per_run <= 0:
        raise ValueError("single-ops-per-run must be > 0")
    if args.complex_items_count <= 0:
        raise ValueError("complex-items-count must be > 0")
    if args.heavy_rows_per_run <= 0:
        raise ValueError("heavy-rows-per-run must be > 0")
    if args.concurrent_ops_per_worker <= 0:
        raise ValueError("concurrent-ops-per-worker must be > 0")
    if args.upsert_ops_per_run <= 0:
        raise ValueError("upsert-ops-per-run must be > 0")
    if args.upsert_key_pool_size <= 0:
        raise ValueError("upsert-key-pool-size must be > 0")

    print(
        "Info: Existing spotify schema has no 50-100KB text/json column; "
        "heavy payload scenario writes max payload available in artists.raw_genres_text (500 chars)."
    )

    scales = parse_scales(args.scales)

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    results = run_benchmark(
        cfg=cfg,
        scales=scales,
        runs_per_scenario=args.runs_per_scenario,
        bulk_size=args.bulk_size,
        heavy_payload_kb=args.heavy_payload_kb,
        concurrent_workers=args.concurrent_workers,
        skip_prepare=args.skip_prepare,
        prepare_mode=args.prepare_mode,
        seed_value=args.seed_value,
        pool_size=args.pool_size,
        single_ops_per_run=args.single_ops_per_run,
        complex_items_count=args.complex_items_count,
        heavy_rows_per_run=args.heavy_rows_per_run,
        concurrent_ops_per_worker=args.concurrent_ops_per_worker,
        upsert_ops_per_run=args.upsert_ops_per_run,
        upsert_key_pool_size=args.upsert_key_pool_size,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nSaved detailed results to: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

