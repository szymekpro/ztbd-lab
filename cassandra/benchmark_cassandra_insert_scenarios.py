import argparse
import os
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from statistics import mean
from typing import Optional

from cassandra.concurrent import execute_concurrent_with_args

from benchmark_cassandra_common import (
    DbConfig,
    apply_indexes,
    close_db,
    connect_db,
    ensure_existing_schema,
    new_bigint_id,
    new_isrc,
    new_spotify_id,
    parse_scales,
    prepare_scale_data_with_seed_script,
    random_text,
    scaled_count,
    timed_run,
    wait_for_secondary_indexes,
)


MANAGED_INDEXES = [
    {
        "name": "idx_tracks_spotify_track_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_tracks_spotify_track_id ON tracks (spotify_track_id)",
        "drop": "DROP INDEX IF EXISTS idx_tracks_spotify_track_id",
        "new": False,
    },
    {
        "name": "idx_tracks_isrc",
        "create": "CREATE INDEX IF NOT EXISTS idx_tracks_isrc ON tracks (isrc)",
        "drop": "DROP INDEX IF EXISTS idx_tracks_isrc",
        "new": False,
    },
    {
        "name": "idx_track_albums_album_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_albums_album_id ON track_albums (album_id)",
        "drop": "DROP INDEX IF EXISTS idx_track_albums_album_id",
        "new": True,
    },
    {
        "name": "idx_track_artists_artist_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists (artist_id)",
        "drop": "DROP INDEX IF EXISTS idx_track_artists_artist_id",
        "new": True,
    },
]


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _sample_ids(session) -> tuple[list[int], list[int], list[int]]:
    artist_ids = [int(r.artist_id) for r in session.execute("SELECT artist_id FROM artists LIMIT 200")]
    album_ids = [int(r.album_id) for r in session.execute("SELECT album_id FROM albums LIMIT 200")]
    genre_ids = [int(r.genre_id) for r in session.execute("SELECT genre_id FROM genres LIMIT 200")]

    if not artist_ids:
        artist_ids = [1]
    if not album_ids:
        album_ids = [1]
    if not genre_ids:
        genre_ids = [1]

    return artist_ids, album_ids, genre_ids


def scenario_single_insert(session) -> int:
    now = _now()
    session.execute(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit,
            duration_min, disc_number, track_number, isrc,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            new_bigint_id(),
            new_spotify_id("single"),
            "Single Insert Track",
            False,
            Decimal("3.141"),
            1,
            1,
            new_isrc(),
            now,
            now,
        ),
    )
    return 1


def scenario_complex_insert(session, artist_ids: list[int], album_ids: list[int], items_count: int = 5) -> int:
    inserted = 0
    now = _now()

    artist_id = new_bigint_id()
    album_id = new_bigint_id()

    session.execute(
        "INSERT INTO artists (artist_id, name, raw_genres_text, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
        (artist_id, f"Complex Artist {new_spotify_id('a')[:8]}", "bench,complex", now, now),
    )
    inserted += 1

    session.execute(
        """
        INSERT INTO albums (album_id, spotify_album_id, name, album_type, release_date, total_tracks, created_at, updated_at)
        VALUES (%s, %s, %s, %s, toDate(now()), %s, %s, %s)
        """,
        (album_id, new_spotify_id("album"), f"Complex Album {new_spotify_id('b')[:8]}", "album", items_count, now, now),
    )
    inserted += 1

    artist_ref = random.choice(artist_ids) if artist_ids else artist_id
    album_ref = random.choice(album_ids) if album_ids else album_id

    for idx in range(items_count):
        track_id = new_bigint_id()
        session.execute(
            """
            INSERT INTO tracks (
                track_id, spotify_track_id, name, explicit,
                duration_min, disc_number, track_number, isrc,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                track_id,
                new_spotify_id("cmp"),
                f"Complex Track {idx + 1}",
                False,
                Decimal(str(round(random.uniform(2.5, 6.5), 3))),
                1,
                idx + 1,
                new_isrc(),
                now,
                now,
            ),
        )
        inserted += 1

        session.execute(
            "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, %s)",
            (track_id, album_ref, True),
        )
        inserted += 1

        session.execute(
            "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (%s, %s, %s)",
            (track_id, artist_ref, 1),
        )
        inserted += 1

    return inserted


def scenario_complex_insert_scaled(session, scale: int, artist_ids: list[int], album_ids: list[int]) -> int:
    items = scaled_count(scale, 0.000005, min_count=5, max_count=200)
    return scenario_complex_insert(session, artist_ids, album_ids, items_count=items)


def scenario_bulk_insert(session, bulk_size: int = 10_000) -> int:
    now = _now()
    stmt = session.prepare(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit,
            duration_min, disc_number, track_number, isrc,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows = []
    for i in range(bulk_size):
        rows.append(
            (
                new_bigint_id(),
                new_spotify_id("bulk"),
                f"Bulk Track {i + 1}",
                bool(i % 2),
                Decimal(str(round(1.0 + random.random() * 8.0, 3))),
                1,
                1 + (i % 20),
                None,
                now,
                now,
            )
        )

    execute_concurrent_with_args(
        session,
        stmt,
        rows,
        concurrency=min(300, max(50, bulk_size // 50)),
        raise_on_first_error=True,
    )
    return bulk_size


def scenario_bulk_insert_scaled(session, scale: int, base_bulk_size: int) -> int:
    scaled = int(base_bulk_size * (scale / 1_000_000))
    scaled = max(1_000, min(scaled, 200_000))
    return scenario_bulk_insert(session, bulk_size=scaled)


def scenario_heavy_payload_insert(session, genre_ids: list[int], payload_kb: int = 75) -> int:
    payload_size = min(payload_kb * 1024, 500)
    artist_batch_size = 250
    genres_per_artist = 3

    now = _now()
    payload_text = random_text(payload_size)
    inserted = 0

    for i in range(artist_batch_size):
        artist_id = new_bigint_id()
        session.execute(
            "INSERT INTO artists (artist_id, name, raw_genres_text, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            (artist_id, f"Heavy Payload Artist {i + 1}", payload_text, now, now),
        )
        inserted += 1

        for g in range(genres_per_artist):
            genre_id = genre_ids[(i + g) % len(genre_ids)]
            session.execute(
                "INSERT INTO artist_genres (artist_id, genre_id) VALUES (%s, %s)",
                (artist_id, genre_id),
            )
            inserted += 1

    return inserted


def _concurrent_insert_worker(cfg: DbConfig, chunk_sizes: list[int], worker_tag: str) -> int:
    if not chunk_sizes:
        return 0

    cluster = None
    session = None
    try:
        cluster, session = connect_db(cfg)
        stmt = session.prepare(
            """
            INSERT INTO tracks (
                track_id, spotify_track_id, name, explicit,
                duration_min, disc_number, track_number, isrc,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        inserted = 0
        for chunk_idx, n in enumerate(chunk_sizes):
            now = _now()
            rows = []
            for i in range(n):
                rows.append(
                    (
                        new_bigint_id(),
                        new_spotify_id(f"c{worker_tag}"),
                        f"Concurrent Bulk Track {chunk_idx}_{i}",
                        False,
                        Decimal("4.200"),
                        1,
                        1 + (i % 20),
                        None,
                        now,
                        now,
                    )
                )

            execute_concurrent_with_args(
                session,
                stmt,
                rows,
                concurrency=min(250, max(25, n // 5)),
                raise_on_first_error=True,
            )
            inserted += n

        return inserted
    finally:
        close_db(cluster, session)


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


def scenario_upsert(session) -> int:
    now = _now()
    track_id = new_bigint_id()

    session.execute(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit,
            duration_min, disc_number, track_number, isrc,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            track_id,
            new_spotify_id("upsert"),
            "Upsert Track Created",
            False,
            Decimal("2.500"),
            1,
            1,
            new_isrc(),
            now,
            now,
        ),
    )

    session.execute(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit,
            duration_min, disc_number, track_number, isrc,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            track_id,
            new_spotify_id("upsert"),
            "Upsert Track Updated",
            True,
            Decimal("3.750"),
            1,
            1,
            new_isrc(),
            now,
            _now(),
        ),
    )

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
                    include_audio_features=False,
                )

            cluster = None
            session = None
            try:
                cluster, session = connect_db(cfg)
                ensure_existing_schema(session, cfg.keyspace)
                apply_indexes(session, MANAGED_INDEXES, with_indexes)

                artist_ids, album_ids, genre_ids = _sample_ids(session)

                if with_indexes:
                    wait_for_secondary_indexes(session, MANAGED_INDEXES, max_total_seconds=600.0, step_seconds=30.0)

                scenarios = [
                    ("single_insert", lambda: scenario_single_insert(session)),
                    (
                        "complex_insert",
                        lambda: scenario_complex_insert_scaled(session, scale=scale, artist_ids=artist_ids, album_ids=album_ids),
                    ),
                    (
                        "bulk_insert",
                        lambda: scenario_bulk_insert_scaled(session, scale=scale, base_bulk_size=bulk_size),
                    ),
                    (
                        "heavy_payload_insert",
                        lambda: scenario_heavy_payload_insert(session, genre_ids=genre_ids, payload_kb=heavy_payload_kb),
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
                    ("upsert_insert_or_update", lambda: scenario_upsert(session)),
                ]

                for scenario_name, scenario_fn in scenarios:
                    for run_idx in range(1, runs_per_scenario + 1):
                        try:
                            elapsed, ops = timed_run(scenario_fn)
                        except Exception as exc:
                            print(
                                f"[WARN] insert scenario failed: scale={scale}, index_mode={index_label}, "
                                f"scenario={scenario_name}, run={run_idx} — {type(exc).__name__}: {exc}"
                            )
                            continue
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
            finally:
                close_db(cluster, session)

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
            "Cassandra INSERT benchmark suite (6 scenariuszy) "
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
    parser.add_argument("--output", default="cassandra/results/cassandra_insert_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "9043")))
    parser.add_argument("--db-keyspace", default=os.getenv("DB_KEYSPACE", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))

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

    print(f"\n>>> Cassandra INSERT Benchmark - tryb: {mode_label} <<<")

    scales = parse_scales(args.scales)

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user or None,
        password=args.db_password or None,
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
