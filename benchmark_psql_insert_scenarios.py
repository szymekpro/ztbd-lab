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

import psycopg


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def connect_db(cfg: DbConfig) -> psycopg.Connection:
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.execute("SET search_path TO spotify, public;")
    return conn


def ensure_existing_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'spotify'
              AND table_name IN ('artists', 'albums', 'tracks', 'track_artists', 'track_albums')
            """
        )
        found = int(cur.fetchone()[0])
        if found < 5:
            raise RuntimeError(
                "Required spotify schema/tables not found. Run init.postgres.sql first."
            )
    conn.commit()


def _count_rows(cur: psycopg.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT count(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def prepare_scale_data_with_seed_script(
    cfg: DbConfig,
    target_rows: int,
    seed_value: Optional[int],
    pool_size: int,
) -> None:
    import seed_psql_faker_data as seed_script

    seed_cfg = seed_script.DbConfig(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    # Build each scale from scratch for reproducible comparisons.
    with seed_script.connect_db(seed_cfg) as seed_conn:
        seed_script.seed_all(
            seed_conn,
            n_genres=30,
            n_artists=max(50, target_rows // 20000),
            n_albums=max(80, target_rows // 10000),
            n_tracks=target_rows,
            seed=seed_value,
            truncate=True,
            pool_size=pool_size,
        )


def _new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def scenario_single_insert(conn: psycopg.Connection) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
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
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    _new_spotify_id("single"),
                    "Single Insert Track",
                    False,
                    3.141,
                    1,
                    1,
                    _new_isrc(),
                ),
            )
    return 1


def scenario_complex_insert(conn: psycopg.Connection, items_count: int = 5) -> int:
    album_id: int
    artist_id: int
    inserted = 0

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artists (name, raw_genres_text)
                VALUES (%s, %s)
                RETURNING artist_id
                """,
                (f"Complex Artist {uuid.uuid4().hex[:8]}", "bench,complex"),
            )
            artist_id = int(cur.fetchone()[0])
            inserted += 1

            cur.execute(
                """
                INSERT INTO albums (spotify_album_id, name, album_type, release_date, total_tracks)
                VALUES (%s, %s, %s, current_date, %s)
                RETURNING album_id
                """,
                (_new_spotify_id("album"), f"Complex Album {uuid.uuid4().hex[:8]}", "album", items_count),
            )
            album_id = int(cur.fetchone()[0])
            inserted += 1

            for idx in range(items_count):
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
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING track_id
                    """,
                    (
                        _new_spotify_id("cmp"),
                        f"Complex Track {idx + 1}",
                        False,
                        round(random.uniform(2.5, 6.5), 3),
                        1,
                        idx + 1,
                        _new_isrc(),
                    ),
                )
                track_id = int(cur.fetchone()[0])
                inserted += 1

                cur.execute(
                    """
                    INSERT INTO track_albums (track_id, album_id, is_primary)
                    VALUES (%s, %s, TRUE)
                    """,
                    (track_id, album_id),
                )
                inserted += 1

                cur.execute(
                    """
                    INSERT INTO track_artists (track_id, artist_id, artist_order)
                    VALUES (%s, %s, 1)
                    """,
                    (track_id, artist_id),
                )
                inserted += 1

    return inserted

def scenario_bulk_insert(conn: psycopg.Connection, bulk_size: int = 10_000) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
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
                )
                SELECT
                    left(md5(clock_timestamp()::text || gs::text || random()::text), 22),
                    'Bulk Track ' || gs::text,
                    (random() > 0.5),
                    round((1.0 + random() * 8.0)::numeric, 3),
                    1,
                    1 + (gs %% 20),
                    NULL
                FROM generate_series(1, %s) gs
                """,
                (bulk_size,),
            )

    return bulk_size


def scenario_heavy_payload_insert(conn: psycopg.Connection, payload_kb: int = 75) -> int:
    payload_size = payload_kb * 1024
    heavy_text = "".join(random.choices(string.ascii_letters + string.digits + " ", k=payload_size))
    fitted_text = heavy_text[:500]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artists (name, raw_genres_text)
                VALUES (%s, %s)
                """,
                (
                    f"Heavy Payload Artist {uuid.uuid4().hex[:8]}",
                    fitted_text,
                ),
            )

    return 1


def _concurrent_worker(cfg: DbConfig) -> int:
    with connect_db(cfg) as worker_conn:
        with worker_conn.transaction():
            with worker_conn.cursor() as cur:
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
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _new_spotify_id("concurrent"),
                        "Concurrent Insert Track",
                        False,
                        4.200,
                        1,
                        1,
                        _new_isrc(),
                    ),
                )
    return 1


def scenario_concurrent_inserts(cfg: DbConfig, workers: int = 100) -> int:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        inserted = list(pool.map(lambda _x: _concurrent_worker(cfg), range(workers)))
    return sum(inserted)


def scenario_upsert(conn: psycopg.Connection) -> int:
    spotify_track_id = _new_spotify_id("upsert")

    with conn.transaction():
        with conn.cursor() as cur:
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
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spotify_track_id) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    explicit = EXCLUDED.explicit,
                    duration_min = EXCLUDED.duration_min,
                    updated_at = now()
                """,
                (
                    spotify_track_id,
                    "Upsert Track Created",
                    False,
                    2.500,
                    1,
                    1,
                    _new_isrc(),
                ),
            )

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
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spotify_track_id) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    explicit = EXCLUDED.explicit,
                    duration_min = EXCLUDED.duration_min,
                    updated_at = now()
                """,
                (
                    spotify_track_id,
                    "Upsert Track Updated",
                    True,
                    3.750,
                    1,
                    1,
                    _new_isrc(),
                ),
            )

    return 2


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
) -> list[dict]:
    results: list[dict] = []

    with connect_db(cfg) as conn:
        ensure_existing_schema(conn)

        for scale in scales:
            if not skip_prepare:
                prepare_scale_data_with_seed_script(
                    cfg=cfg,
                    target_rows=scale,
                    seed_value=seed_value,
                    pool_size=pool_size,
                )

            scenarios = [
                ("single_insert", lambda: scenario_single_insert(conn)),
                ("complex_insert", lambda: scenario_complex_insert(conn, items_count=5)),
                ("bulk_insert", lambda: scenario_bulk_insert(conn, bulk_size=bulk_size)),
                (
                    "heavy_payload_insert",
                    lambda: scenario_heavy_payload_insert(conn, payload_kb=heavy_payload_kb),
                ),
                (
                    "concurrent_inserts",
                    lambda: scenario_concurrent_inserts(cfg, workers=concurrent_workers),
                ),
                ("upsert_insert_or_update", lambda: scenario_upsert(conn)),
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
            "PostgreSQL INSERT benchmark suite (6 scenariuszy) "
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
            "seed-script: przygotowuje skale przez seed_psql_faker_data.py; "
            "fast: szybkie dopelnienie tracks bez pelnego seedowania"
        ),
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="psql_insert_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "5434")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify_db"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

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
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nSaved detailed results to: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
