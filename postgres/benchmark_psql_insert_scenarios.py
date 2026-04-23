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


# ---------------------------------------------------------------------------
# INDEX MANAGEMENT (for with/without indexes comparison)
# ---------------------------------------------------------------------------

MANAGED_INDEXES = [
    {
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date)",
        "drop":   "DROP INDEX IF EXISTS idx_albums_release_date",
        "new": False,
    },
    {
        "name": "idx_track_albums_album_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_albums_album_id ON track_albums(album_id)",
        "drop":   "DROP INDEX IF EXISTS idx_track_albums_album_id",
        "new": True,
    },
    {
        "name": "idx_track_artists_artist_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id)",
        "drop":   "DROP INDEX IF EXISTS idx_track_artists_artist_id",
        "new": True,
    },
]


def apply_indexes(conn: psycopg.Connection, with_indexes: bool) -> None:
    action = "Tworzenie" if with_indexes else "Usuwanie"
    print(f"\n[INDEX] {action} indeksów...")
    with conn.cursor() as cur:
        for idx in MANAGED_INDEXES:
            sql = idx["create"] if with_indexes else idx["drop"]
            label = "(nowy)" if idx.get("new") else "(schemat)"
            print(f"  {'CREATE' if with_indexes else 'DROP':6s}  {idx['name']} {label}")
            cur.execute(sql)
    conn.commit()
    print("[INDEX] Gotowe.\n")


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


def _scaled_count(scale: int, fraction: float, *, min_count: int, max_count: int) -> int:
    if scale <= 0:
        return min_count
    return max(min_count, min(int(scale * fraction), max_count))


def prepare_scale_data_with_seed_script(
    cfg: DbConfig,
    target_rows: int,
    seed_value: Optional[int],
    pool_size: int,
) -> None:
    import sys
    from pathlib import Path

    # Make import work regardless of cwd (root/ vs postgres/)
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

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
            include_audio_features=False,
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


def scenario_complex_insert_scaled(conn: psycopg.Connection, scale: int) -> int:
    items = _scaled_count(scale, 0.000005, min_count=5, max_count=200)
    return scenario_complex_insert(conn, items_count=items)

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


def scenario_bulk_insert_scaled(conn: psycopg.Connection, scale: int, base_bulk_size: int) -> int:
    # Scale bulk size relative to 1,000,000 baseline so plots show scale effects.
    scaled = int(base_bulk_size * (scale / 1_000_000))
    scaled = max(1_000, min(scaled, 200_000))
    return scenario_bulk_insert(conn, bulk_size=scaled)


def scenario_heavy_payload_insert(conn: psycopg.Connection, payload_kb: int = 75) -> int:
    payload_size = min(payload_kb * 1024, 500)
    artist_batch_size = 250
    genres_per_artist = 3

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH inserted_artists AS (
                    INSERT INTO artists (name, raw_genres_text)
                    SELECT
                        'Heavy Payload Artist ' || gs::text || ' ' || substr(md5(clock_timestamp()::text || gs::text), 1, 8),
                        left(repeat(md5(random()::text || gs::text), 20), %s)
                    FROM generate_series(1, %s) gs
                    RETURNING artist_id
                )
                INSERT INTO artist_genres (artist_id, genre_id)
                SELECT ia.artist_id, g.genre_id
                FROM inserted_artists ia
                JOIN LATERAL (
                    SELECT genre_id
                    FROM genres
                    ORDER BY random()
                    LIMIT %s
                ) g ON TRUE
                """,
                (payload_size, artist_batch_size, genres_per_artist),
            )

    return artist_batch_size + (artist_batch_size * genres_per_artist)


def _concurrent_insert_worker(cfg: DbConfig, chunk_sizes: list[int], worker_tag: str) -> int:
    if not chunk_sizes:
        return 0

    # Reuse a single connection per worker (avoid connection storms on Windows).
    with connect_db(cfg) as worker_conn:
        inserted = 0
        with worker_conn.transaction():
            with worker_conn.cursor() as cur:
                for idx, n in enumerate(chunk_sizes):
                    if n <= 0:
                        continue
                    # One statement inserts `n` rows; fixed chunk size => roundtrips scale with dataset.
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
                            left(md5(clock_timestamp()::text || random()::text || %s || %s || gs::text), 22),
                            'Concurrent Bulk Track',
                            false,
                            4.200,
                            1,
                            1 + (gs %% 20),
                            NULL
                        FROM generate_series(1, %s) gs
                        """,
                        (worker_tag, str(idx), n),
                    )
                    inserted += n
        return inserted


def scenario_concurrent_inserts_scaled(
    cfg: DbConfig,
    scale: int,
    workers: int,
    chunk_size: int,
) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    total = _scaled_count(scale, 0.002, min_count=workers, max_count=200_000)
    chunks = [chunk_size] * (total // chunk_size)
    rem = total % chunk_size
    if rem:
        chunks.append(rem)

    buckets: list[list[int]] = [[] for _ in range(workers)]
    for i, n in enumerate(chunks):
        buckets[i % workers].append(n)

    tag = uuid.uuid4().hex[:8]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        inserted = list(
            pool.map(
                lambda args: _concurrent_insert_worker(cfg, args[0], args[1]),
                [(buckets[i], f"w{tag}_{i}") for i in range(workers)],
            )
        )
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
    concurrent_chunk_size: int,
    skip_prepare: bool,
    seed_value: Optional[int],
    pool_size: int,
    index_modes: list[bool],
) -> list[dict]:
    results: list[dict] = []

    if not index_modes:
        raise ValueError("index_modes must not be empty")

    for scale in scales:
        for with_indexes in index_modes:
            index_label = "with_indexes" if with_indexes else "no_indexes"

            # To fairly compare with/without indexes, rebuild the dataset per mode.
            if not skip_prepare:
                prepare_scale_data_with_seed_script(
                    cfg=cfg,
                    target_rows=scale,
                    seed_value=seed_value,
                    pool_size=pool_size,
                )

            with connect_db(cfg) as conn:
                ensure_existing_schema(conn)
                apply_indexes(conn, with_indexes)

                scenarios = [
                    ("single_insert", lambda: scenario_single_insert(conn)),
                    ("complex_insert", lambda: scenario_complex_insert_scaled(conn, scale=scale)),
                    ("bulk_insert", lambda: scenario_bulk_insert_scaled(conn, scale=scale, base_bulk_size=bulk_size)),
                    (
                        "heavy_payload_insert",
                        lambda: scenario_heavy_payload_insert(conn, payload_kb=heavy_payload_kb),
                    ),
                    (
                        "concurrent_inserts",
                        lambda: scenario_concurrent_inserts_scaled(
                            cfg,
                            scale=scale,
                            workers=concurrent_workers,
                            chunk_size=concurrent_chunk_size,
                        ),
                    ),
                    ("upsert_insert_or_update", lambda: scenario_upsert(conn)),
                ]

                for scenario_name, scenario_fn in scenarios:
                    for run_idx in range(1, runs_per_scenario + 1):
                        elapsed, ops = timed_run(scenario_fn)
                        results.append(
                            {
                                "scale": scale,
                                "index_mode": index_label,
                                "scenario": scenario_name,
                                "run": run_idx,
                                "seconds": elapsed,
                                "operations": ops,
                                "ops_per_sec": (ops / elapsed) if elapsed > 0 else None,
                            }
                        )

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row["scale"], row.get("index_mode", "with_indexes"), row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== Benchmark Summary (avg from runs) ===")
    print("scale | index_mode | scenario | avg_seconds | avg_ops_per_sec")
    print("-" * 70)

    for (scale, index_mode, scenario), rows in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        avg_seconds = mean(r["seconds"] for r in rows)
        valid_ops = [r["ops_per_sec"] for r in rows if r["ops_per_sec"] is not None]
        avg_ops = mean(valid_ops) if valid_ops else 0.0
        print(f"{scale} | {index_mode} | {scenario} | {avg_seconds:.6f} | {avg_ops:.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["scale", "index_mode", "scenario", "run", "seconds", "operations", "ops_per_sec"]
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
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Usuwa zarządzane indeksy przed testem (tryb 'bez indeksów').",
    )
    parser.add_argument(
        "--both-index-modes",
        action="store_true",
        help="Uruchamia benchmark w dwóch trybach (bez indeksów i z indeksami) i zapisuje do jednego CSV.",
    )
    parser.add_argument("--bulk-size", type=int, default=10000)
    parser.add_argument("--heavy-payload-kb", type=int, default=75)
    parser.add_argument("--concurrent-workers", type=int, default=100)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile rekordów wstawia jeden worker w pojedynczym INSERT (stały chunk size).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="postgres/results/psql_insert_benchmark_results.csv")

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
    if args.heavy_payload_kb <= 0:
        raise ValueError("heavy-payload-kb must be > 0")
    if args.concurrent_workers <= 0:
        raise ValueError("concurrent-workers must be > 0")
    if args.concurrent_chunk_size <= 0:
        raise ValueError("concurrent-chunk-size must be > 0")
    if args.pool_size <= 0:
        raise ValueError("pool-size must be > 0")

    if args.both_index_modes:
        index_modes = [False, True]
        mode_label = "BEZ indeksów + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksów"

    print(f"\n>>> PostgreSQL INSERT Benchmark – tryb: {mode_label} <<<")

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
        concurrent_chunk_size=args.concurrent_chunk_size,
        skip_prepare=args.skip_prepare,
        seed_value=args.seed_value,
        pool_size=args.pool_size,
        index_modes=index_modes,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nSaved detailed results to: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
