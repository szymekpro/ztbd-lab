import argparse
import os
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
from typing import Optional

from benchmark_mariadb_common import (
    DbConfig,
    apply_indexes,
    connect_db,
    ensure_existing_schema,
    parse_scales,
    prepare_scale_data_with_seed_script,
    scaled_count,
    timed_run,
)


MANAGED_INDEXES = [
    {
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date)",
        "drop": "DROP INDEX IF EXISTS idx_albums_release_date ON albums",
        "new": False,
    },
]


def _new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def _sample_ids(conn) -> tuple[list[int], list[int], list[int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT artist_id FROM artists LIMIT 200")
        artist_ids = [int(r[0]) for r in cur.fetchall()]

        cur.execute("SELECT album_id FROM albums LIMIT 200")
        album_ids = [int(r[0]) for r in cur.fetchall()]

        cur.execute("SELECT genre_id FROM genres LIMIT 200")
        genre_ids = [int(r[0]) for r in cur.fetchall()]

    if not artist_ids:
        artist_ids = [1]
    if not album_ids:
        album_ids = [1]
    if not genre_ids:
        genre_ids = [1]

    return artist_ids, album_ids, genre_ids


def scenario_single_insert(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tracks (
                spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (_new_spotify_id("single"), "Single Insert Track", False, 3.141, 1, 1, _new_isrc()),
        )
    conn.commit()
    return 1


def scenario_complex_insert(conn, artist_ids: list[int], album_ids: list[int], items_count: int = 5) -> int:
    inserted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artists (name, raw_genres_text, updated_at) VALUES (%s, %s, NOW())",
                (f"Complex Artist {uuid.uuid4().hex[:8]}", "bench,complex"),
            )
            inserted += 1

            cur.execute(
                """
                INSERT INTO albums (spotify_album_id, name, album_type, release_date, total_tracks, updated_at)
                VALUES (%s, %s, %s, CURDATE(), %s, NOW())
                """,
                (_new_spotify_id("album"), f"Complex Album {uuid.uuid4().hex[:8]}", "album", items_count),
            )
            inserted += 1

            artist_ref = random.choice(artist_ids)
            album_ref = random.choice(album_ids)

            for idx in range(items_count):
                cur.execute(
                    """
                    INSERT INTO tracks (
                        spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
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
                track_id = int(cur.lastrowid)
                inserted += 1

                cur.execute(
                    "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, TRUE)",
                    (track_id, album_ref),
                )
                inserted += 1

                cur.execute(
                    "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (%s, %s, 1)",
                    (track_id, artist_ref),
                )
                inserted += 1

        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise


def scenario_complex_insert_scaled(conn, scale: int, artist_ids: list[int], album_ids: list[int]) -> int:
    items = scaled_count(scale, 0.000005, min_count=5, max_count=200)
    return scenario_complex_insert(conn, artist_ids, album_ids, items_count=items)


def scenario_bulk_insert(conn, bulk_size: int = 10_000) -> int:
    rows = []
    for i in range(bulk_size):
        rows.append(
            (
                _new_spotify_id("bulk"),
                f"Bulk Track {i + 1}",
                bool(i % 2),
                round(1.0 + random.random() * 8.0, 3),
                1,
                1 + (i % 20),
                None,
            )
        )

    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO tracks (
                    spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return bulk_size


def scenario_bulk_insert_scaled(conn, scale: int, base_bulk_size: int) -> int:
    scaled = int(base_bulk_size * (scale / 1_000_000))
    scaled = max(1_000, min(scaled, 200_000))
    return scenario_bulk_insert(conn, bulk_size=scaled)


def scenario_heavy_payload_insert(conn, genre_ids: list[int], payload_kb: int = 75) -> int:
    payload_size = min(payload_kb * 1024, 500)
    artist_batch_size = 250
    genres_per_artist = 3
    payload = ("x" * payload_size)[:500]

    inserted = 0
    try:
        with conn.cursor() as cur:
            for i in range(artist_batch_size):
                cur.execute(
                    "INSERT INTO artists (name, raw_genres_text, updated_at) VALUES (%s, %s, NOW())",
                    (f"Heavy Payload Artist {i + 1}", payload),
                )
                artist_id = int(cur.lastrowid)
                inserted += 1

                for g in range(genres_per_artist):
                    genre_id = genre_ids[(i + g) % len(genre_ids)]
                    cur.execute(
                        "INSERT IGNORE INTO artist_genres (artist_id, genre_id) VALUES (%s, %s)",
                        (artist_id, genre_id),
                    )
                    inserted += 1
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise


def _concurrent_insert_worker(cfg: DbConfig, chunk_sizes: list[int], worker_tag: str) -> int:
    if not chunk_sizes:
        return 0

    inserted = 0
    with connect_db(cfg) as conn:
        try:
            with conn.cursor() as cur:
                for chunk_idx, n in enumerate(chunk_sizes):
                    rows = []
                    for i in range(n):
                        rows.append(
                            (
                                _new_spotify_id(f"c{worker_tag}"),
                                f"Concurrent Bulk Track {chunk_idx}_{i}",
                                False,
                                4.200,
                                1,
                                1 + (i % 20),
                                None,
                            )
                        )
                    cur.executemany(
                        """
                        INSERT INTO tracks (
                            spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        rows,
                    )
                    inserted += n
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return inserted


def scenario_concurrent_inserts_scaled(cfg: DbConfig, scale: int, workers: int, chunk_size: int) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    total = scaled_count(scale, 0.002, min_count=workers, max_count=200_000)
    chunks = [chunk_size] * (total // chunk_size)
    rem = total % chunk_size
    if rem:
        chunks.append(rem)

    buckets: list[list[int]] = [[] for _ in range(workers)]
    for i, n in enumerate(chunks):
        buckets[i % workers].append(n)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        inserted = list(
            pool.map(
                lambda args: _concurrent_insert_worker(cfg, args[0], args[1]),
                [(buckets[i], f"w{i}") for i in range(workers)],
            )
        )

    return sum(inserted)


def scenario_upsert(conn) -> int:
    spotify_track_id = _new_spotify_id("upsert")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tracks (
                    spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    explicit = VALUES(explicit),
                    duration_min = VALUES(duration_min),
                    updated_at = NOW()
                """,
                (spotify_track_id, "Upsert Track Created", False, 2.500, 1, 1, _new_isrc()),
            )

            cur.execute(
                """
                INSERT INTO tracks (
                    spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    explicit = VALUES(explicit),
                    duration_min = VALUES(duration_min),
                    updated_at = NOW()
                """,
                (spotify_track_id, "Upsert Track Updated", True, 3.750, 1, 1, _new_isrc()),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return 2


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

            if not skip_prepare:
                prepare_scale_data_with_seed_script(
                    cfg=cfg,
                    target_rows=scale,
                    seed_value=seed_value,
                    pool_size=pool_size,
                )

            with connect_db(cfg) as conn:
                ensure_existing_schema(conn)
                apply_indexes(conn, MANAGED_INDEXES, with_indexes)

                artist_ids, album_ids, genre_ids = _sample_ids(conn)

                scenarios = [
                    ("single_insert", lambda: scenario_single_insert(conn)),
                    (
                        "complex_insert",
                        lambda: scenario_complex_insert_scaled(conn, scale=scale, artist_ids=artist_ids, album_ids=album_ids),
                    ),
                    ("bulk_insert", lambda: scenario_bulk_insert_scaled(conn, scale=scale, base_bulk_size=bulk_size)),
                    (
                        "heavy_payload_insert",
                        lambda: scenario_heavy_payload_insert(conn, genre_ids=genre_ids, payload_kb=heavy_payload_kb),
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
            "MariaDB INSERT benchmark suite (6 scenariuszy) "
            "na istniejacym schemacie spotify: single, complex, bulk, heavy payload, concurrent, upsert."
        )
    )

    parser.add_argument("--scales", default="500000,1000000,10000000")
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Usuwa zarzadzane indeksy przed testem (tryb 'bez indeksow').",
    )
    parser.add_argument(
        "--both-index-modes",
        action="store_true",
        help="Uruchamia benchmark w dwoch trybach (bez indeksow i z indeksami) i zapisuje do jednego CSV.",
    )
    parser.add_argument("--bulk-size", type=int, default=10000)
    parser.add_argument("--heavy-payload-kb", type=int, default=75)
    parser.add_argument("--concurrent-workers", type=int, default=100)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile rekordow wstawia jeden worker w pojedynczym INSERT (staly chunk size).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="mariadb/results/mariadb_insert_benchmark_results.csv")

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
        mode_label = "BEZ indeksow + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksow"

    print(f"\n>>> MariaDB INSERT Benchmark - tryb: {mode_label} <<<")

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
