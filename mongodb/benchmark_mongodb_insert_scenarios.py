"""MongoDB INSERT benchmark suite – port of the PostgreSQL suite.

Scenarios (same names as in postgres/benchmark_psql_insert_scenarios.py):
  1. single_insert
  2. complex_insert
  3. bulk_insert
  4. heavy_payload_insert
  5. concurrent_inserts
  6. upsert_insert_or_update

Output CSV schema matches plot_results.py expectations.
Default output filename matches the repo convention (mongodb/results/mongodb_*_benchmark_results.csv),
so plot_results.py can load it automatically when run from the repo root.
"""


from __future__ import annotations

import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import argparse
import os
import random
import string
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from statistics import mean
from typing import Optional, Any

try:
    from mongodb.benchmark_mongodb_common import (
        DbConfig,
        apply_indexes,
        connect_db,
        default_mongo_config_from_env,
        ensure_existing_schema,
        parse_scales,
        prepare_scale_data_with_seed_script,
        _scaled_count,
    )
except ModuleNotFoundError:
    from benchmark_mongodb_common import (  # type: ignore
        DbConfig,
        apply_indexes,
        connect_db,
        default_mongo_config_from_env,
        ensure_existing_schema,
        parse_scales,
        prepare_scale_data_with_seed_script,
        _scaled_count,
    )


_BASE62 = string.digits + string.ascii_letters


def _new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def _new_int_id(base: int) -> int:
    # Avoid collisions with seeded (1..scale) ids by shifting above scale.
    # Mongo does not enforce uniqueness on these ids, but duplicates would skew joins.
    return base + (uuid.uuid4().int % 1_000_000_000)


def scenario_single_insert(db: Any, *, scale: int) -> int:
    now = datetime.now(tz=timezone.utc)
    doc = {
        "track_id": _new_int_id(scale + 10_000_000),
        "spotify_track_id": _new_spotify_id("single"),
        "name": "Single Insert Track",
        "explicit": False,
        "duration_min": 3.141,
        "disc_number": 1,
        "track_number": 1,
        "isrc": _new_isrc(),
        "created_at": now,
        "updated_at": now,
    }
    db.tracks.insert_one(doc)
    return 1


def scenario_complex_insert(db: Any, *, items_count: int, scale: int) -> int:
    now = datetime.now(tz=timezone.utc)

    artist_id = _new_int_id(scale + 20_000_000)
    album_id = _new_int_id(scale + 30_000_000)

    inserted = 0

    db.artists.insert_one(
        {
            "artist_id": artist_id,
            "name": f"Complex Artist {uuid.uuid4().hex[:8]}",
            "raw_genres_text": "bench,complex",
            "created_at": now,
            "updated_at": now,
        }
    )
    inserted += 1

    db.albums.insert_one(
        {
            "album_id": album_id,
            "spotify_album_id": _new_spotify_id("album"),
            "name": f"Complex Album {uuid.uuid4().hex[:8]}",
            "album_type": "album",
            "release_date": now,
            "total_tracks": int(items_count),
            "created_at": now,
            "updated_at": now,
        }
    )
    inserted += 1

    for idx in range(items_count):
        track_id = _new_int_id(scale + 40_000_000 + idx)
        db.tracks.insert_one(
            {
                "track_id": track_id,
                "spotify_track_id": _new_spotify_id("cmp"),
                "name": f"Complex Track {idx + 1}",
                "explicit": False,
                "duration_min": round(random.uniform(2.5, 6.5), 3),
                "disc_number": 1,
                "track_number": idx + 1,
                "isrc": _new_isrc(),
                "created_at": now,
                "updated_at": now,
            }
        )
        inserted += 1

        db.track_albums.insert_one({"track_id": track_id, "album_id": album_id, "is_primary": True})
        inserted += 1

        db.track_artists.insert_one({"track_id": track_id, "artist_id": artist_id, "artist_order": 1})
        inserted += 1

    return inserted


def scenario_complex_insert_scaled(db: Any, *, scale: int) -> int:
    items = _scaled_count(scale, 0.000005, min_count=5, max_count=200)
    return scenario_complex_insert(db, items_count=items, scale=scale)


def scenario_bulk_insert(db: Any, *, bulk_size: int, scale: int) -> int:
    now = datetime.now(tz=timezone.utc)
    tag = uuid.uuid4().hex[:5]
    prefix = f"bk{tag}"  # 7 chars

    docs = []
    base = scale + 50_000_000
    for i in range(bulk_size):
        docs.append(
            {
                "track_id": base + i,
                "spotify_track_id": f"{prefix}{i:015d}"[:22].ljust(22, "0"),
                "name": f"Bulk Track {i}",
                "explicit": (i % 2 == 0),
                "duration_min": round(1.0 + (i % 8) + 0.001 * (i % 1000), 3),
                "disc_number": 1,
                "track_number": 1 + (i % 20),
                "created_at": now,
                "updated_at": now,
            }
        )

    if docs:
        db.tracks.insert_many(docs, ordered=False)
    return bulk_size


def scenario_bulk_insert_scaled(db: Any, *, scale: int, base_bulk_size: int) -> int:
    scaled = int(base_bulk_size * (scale / 1_000_000))
    scaled = max(1_000, min(scaled, 200_000))
    return scenario_bulk_insert(db, bulk_size=scaled, scale=scale)


def scenario_heavy_payload_insert(db: Any, *, payload_kb: int, scale: int) -> int:
    payload_size = max(payload_kb * 1024, 50 * 1024)
    artist_batch_size = 250
    genres_per_artist = 3

    now = datetime.now(tz=timezone.utc)

    genre_ids = [g.get("genre_id") for g in db.genres.find({}, {"genre_id": 1, "_id": 0}) if g.get("genre_id")]
    if not genre_ids:
        genre_ids = [1]

    artist_docs = []
    artist_genre_docs = []
    base_artist_id = scale + 60_000_000

    for i in range(artist_batch_size):
        artist_id = base_artist_id + i
        artist_docs.append(
            {
                "artist_id": artist_id,
                "name": f"Heavy Payload Artist {i} {uuid.uuid4().hex[:8]}",
                "raw_genres_text": (uuid.uuid4().hex * 200)[:payload_size],
                "created_at": now,
                "updated_at": now,
            }
        )

        # Pick distinct genre_ids to avoid unique (artist_id, genre_id) conflicts.
        chosen = random.sample(genre_ids, k=min(genres_per_artist, len(genre_ids)))
        for gid in chosen:
            artist_genre_docs.append({"artist_id": artist_id, "genre_id": gid})

    if artist_docs:
        db.artists.insert_many(artist_docs, ordered=False)
    if artist_genre_docs:
        try:
            db.artist_genres.insert_many(artist_genre_docs, ordered=False)
        except Exception:
            # Some duplicates might still happen if genre_ids are tiny; ignore.
            pass

    return artist_batch_size + (artist_batch_size * genres_per_artist)


def _concurrent_insert_worker(cfg: DbConfig, *, row_count: int, worker_tag: str, scale: int) -> int:
    if row_count <= 0:
        return 0

    client, db = connect_db(cfg, max_pool_size=300)
    try:
        now = datetime.now(tz=timezone.utc)
        inserted = 0
        base = scale + 70_000_000 + (uuid.uuid4().int % 10_000_000)
        for i in range(row_count):
            spotify_track_id = f"{worker_tag}{i:013d}"[:22].ljust(22, "0")
            db.tracks.insert_one(
                {
                    "track_id": base + i,
                    "spotify_track_id": spotify_track_id,
                    "name": f"Concurrent Track {worker_tag} {i}",
                    "explicit": False,
                    "duration_min": 4.200,
                    "disc_number": 1,
                    "track_number": 1 + (i % 20),
                    "created_at": now,
                    "updated_at": now,
                }
            )
            inserted += 1
        return inserted
    finally:
        client.close()


def scenario_concurrent_inserts_scaled(
    cfg: DbConfig,
    *,
    scale: int,
    workers: int,
    chunk_size: int,
) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    total = _scaled_count(scale, 0.002, min_count=workers, max_count=200_000)
    base = total // workers
    rem = total % workers
    per_worker_counts = [base + (1 if i < rem else 0) for i in range(workers)]

    tag = uuid.uuid4().hex[:6]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        inserted = list(
            pool.map(
                lambda args: _concurrent_insert_worker(cfg, row_count=args[0], worker_tag=args[1], scale=scale),
                [(per_worker_counts[i], f"c{tag}{i:02d}") for i in range(workers)],
            )
        )
    return int(sum(inserted))


def scenario_upsert(db: Any, *, scale: int) -> int:
    now = datetime.now(tz=timezone.utc)
    spotify_track_id = _new_spotify_id("upsert")
    base_track_id = _new_int_id(scale + 80_000_000)

    db.tracks.update_one(
        {"spotify_track_id": spotify_track_id},
        {
            "$setOnInsert": {
                "track_id": base_track_id,
                "created_at": now,
            },
            "$set": {
                "name": "Upsert Track Created",
                "explicit": False,
                "duration_min": 2.500,
                "disc_number": 1,
                "track_number": 1,
                "isrc": _new_isrc(),
                "updated_at": now,
            },
        },
        upsert=True,
    )

    db.tracks.update_one(
        {"spotify_track_id": spotify_track_id},
        {
            "$set": {
                "name": "Upsert Track Updated",
                "explicit": True,
                "duration_min": 3.750,
                "disc_number": 1,
                "track_number": 1,
                "isrc": _new_isrc(),
                "updated_at": now,
            }
        },
        upsert=True,
    )

    return 2


def timed_run(fn, *args, **kwargs) -> tuple[float, int]:
    start = time.perf_counter()
    ops = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, int(ops)


def run_benchmark(
    cfg: DbConfig,
    *,
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

            client, db = connect_db(cfg)
            try:
                ensure_existing_schema(db)
                apply_indexes(db, with_indexes)

                scenarios = [
                    ("single_insert", lambda: scenario_single_insert(db, scale=scale)),
                    ("complex_insert", lambda: scenario_complex_insert_scaled(db, scale=scale)),
                    ("bulk_insert", lambda: scenario_bulk_insert_scaled(db, scale=scale, base_bulk_size=bulk_size)),
                    ("heavy_payload_insert", lambda: scenario_heavy_payload_insert(db, payload_kb=heavy_payload_kb, scale=scale)),
                    (
                        "concurrent_inserts",
                        lambda: scenario_concurrent_inserts_scaled(
                            cfg,
                            scale=scale,
                            workers=concurrent_workers,
                            chunk_size=concurrent_chunk_size,
                        ),
                    ),
                    ("upsert_insert_or_update", lambda: scenario_upsert(db, scale=scale)),
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
            finally:
                client.close()

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row["scale"], row.get("index_mode", "with_indexes"), row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== MongoDB INSERT Benchmark Summary (avg from runs) ===")
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
            "MongoDB INSERT benchmark suite (6 scenariuszy) – single, complex, bulk, heavy payload, concurrent, upsert. "
            "Nazwy scenariuszy i format CSV są kompatybilne z plot_results.py."
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
    parser.add_argument("--concurrent-workers", type=int, default=50)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile rekordów wstawia jeden worker (liczba insert_one per worker).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="mongodb/results/mongodb_insert_benchmark_results.csv")

    # Mongo config (defaults from docker-compose)
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "27018")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))
    parser.add_argument("--db-auth-source", default=os.getenv("DB_AUTH_SOURCE", "spotify"))

    args = parser.parse_args()

    if args.runs_per_scenario <= 0:
        raise ValueError("runs-per-scenario must be > 0")
    if args.concurrent_workers <= 0:
        raise ValueError("concurrent-workers must be > 0")
    if args.concurrent_chunk_size <= 0:
        raise ValueError("concurrent-chunk-size must be > 0")
    if args.pool_size <= 0:
        raise ValueError("pool-size must be > 0")

    scales = parse_scales(args.scales)

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        auth_source=args.db_auth_source,
    )

    if args.both_index_modes:
        index_modes = [False, True]
        mode_label = "BEZ indeksów + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksów"

    print(f"\n>>> MongoDB INSERT Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg,
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
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
