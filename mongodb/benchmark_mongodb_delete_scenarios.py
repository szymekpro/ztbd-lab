"""MongoDB DELETE benchmark suite – port of the PostgreSQL suite.

Scenarios (same names as in postgres/benchmark_psql_delete_scenarios.py):
  1. point_delete
  2. cascade_delete
  3. relationship_delete
  4. range_delete
  5. concurrent_delete
  6. soft_delete

Important differences vs PostgreSQL:
- MongoDB does not support server-side ON DELETE CASCADE across collections.
  Scenario `cascade_delete` is therefore *mocked* as a client-driven multi-collection
  delete (albums + album_artists + track_albums).

Output CSV schema matches visualization/plot_results.py expectations.
Default output filename matches PostgreSQL so plot_results.py works unchanged
when called with --results-dir ../mongodb/results.
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
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from statistics import mean
from typing import Optional, Any

try:
    from mongodb.benchmark_mongodb_common import (
        DbConfig,
        apply_indexes,
        connect_db,
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
        ensure_existing_schema,
        parse_scales,
        prepare_scale_data_with_seed_script,
        _scaled_count,
    )


_WARNED_CASCADE = False


def _base36(n: int) -> str:
    if n < 0:
        raise ValueError("n must be >= 0")
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if n == 0:
        return "0"
    out = []
    while n:
        n, r = divmod(n, 36)
        out.append(alphabet[r])
    return "".join(reversed(out))


def _isrc_from_tag(tag: str, i: int) -> str:
    # Keep exactly 12 chars: 2 + 6 + 4.
    return f"PL{tag.upper()}{_base36(i).rjust(4, '0')[:4]}"


def _new_int_id(base: int) -> int:
    return base + (uuid.uuid4().int % 1_000_000_000)


def _new_spotify_id(prefix: str = "") -> str:
    seed = prefix + uuid.uuid4().hex
    return seed[:22].ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


# ---------------------------------------------------------------------------
# SAMPLE ID FETCHING
# ---------------------------------------------------------------------------


def fetch_sample_ids(db: Any, *, scale: int) -> dict:
    samples: dict = {}

    chart = db.charts.find_one({}, {"chart_id": 1, "_id": 0})
    samples["chart_id"] = int(chart.get("chart_id")) if chart and chart.get("chart_id") is not None else 1

    max_album_id = max(1, max(80, scale // 10000))
    samples["album_ids"] = [random.randint(1, max_album_id) for _ in range(100)]

    max_artist_id = max(1, max(50, scale // 20000))
    samples["artist_ids"] = [random.randint(1, max_artist_id) for _ in range(20)]

    print("[SETUP] Załadowano sample IDs:")
    print(f"  chart_id:   {samples['chart_id']}")
    print(f"  album_ids:  {len(samples['album_ids'])}")
    print(f"  artist_ids: {len(samples['artist_ids'])}")
    return samples


# ---------------------------------------------------------------------------
# SETUP HELPERS (create temporary data to delete)
# ---------------------------------------------------------------------------


def _setup_market(db: Any, *, scale: int) -> int:
    code = uuid.uuid4().hex[:2].upper()
    market_id = _new_int_id(scale + 10_000_000)
    db.markets.insert_one({"market_id": market_id, "country_code": code, "name": f"TempMarket_{uuid.uuid4().hex[:6]}"})
    return market_id


def _setup_album_with_relations(db: Any, *, artist_id: int, scale: int) -> tuple[int, int]:
    now = datetime.now(tz=timezone.utc)
    album_id = _new_int_id(scale + 20_000_000)
    track_id = _new_int_id(scale + 30_000_000)

    db.albums.insert_one(
        {
            "album_id": album_id,
            "spotify_album_id": _new_spotify_id("del_alb"),
            "name": f"TempAlbum_{uuid.uuid4().hex[:8]}",
            "album_type": "single",
            "release_date": now,
            "total_tracks": 1,
            "created_at": now,
            "updated_at": now,
        }
    )
    db.album_artists.insert_one({"album_id": album_id, "artist_id": artist_id, "artist_order": 1})

    db.tracks.insert_one(
        {
            "track_id": track_id,
            "spotify_track_id": _new_spotify_id("del_trk"),
            "name": f"TempTrack_{uuid.uuid4().hex[:8]}",
            "explicit": False,
            "duration_min": 3.0,
            "disc_number": 1,
            "track_number": 1,
            "isrc": _new_isrc(),
            "created_at": now,
            "updated_at": now,
        }
    )
    db.track_albums.insert_one({"track_id": track_id, "album_id": album_id, "is_primary": True})

    return album_id, track_id


def _setup_track_artist_relations_bulk(db: Any, *, artist_id: int, count: int, scale: int) -> tuple[str, list[int]]:
    now = datetime.now(tz=timezone.utc)
    tag = uuid.uuid4().hex[:6]
    prefix = f"rb{tag}"  # marker for cleanup

    base_track_id = scale + 40_000_000 + (uuid.uuid4().int % 1_000_000)
    track_docs = []
    rel_docs = []
    track_ids: list[int] = []

    for i in range(count):
        track_id = base_track_id + i
        track_ids.append(track_id)
        track_docs.append(
            {
                "track_id": track_id,
                "spotify_track_id": f"{prefix}{i:014d}",
                "name": f"RelBulk_{tag}_{i}",
                "explicit": False,
                "duration_min": 2.5,
                "disc_number": 1,
                "track_number": 1,
                "isrc": _isrc_from_tag(tag, i),
                "created_at": now,
                "updated_at": now,
            }
        )
        rel_docs.append({"track_id": track_id, "artist_id": artist_id, "artist_order": 1, "bench_tag": tag})

    if track_docs:
        db.tracks.insert_many(track_docs, ordered=False)
    if rel_docs:
        db.track_artists.insert_many(rel_docs, ordered=False)

    return tag, track_ids


def _setup_old_chart_entries(db: Any, *, chart_id: int, count: int, scale: int) -> int:
    if count <= 0:
        return 0

    # Seeded charts use only last ~7 days, so choosing 4 years ago should target only our inserts.
    old_start = datetime.now(tz=timezone.utc) - timedelta(days=365 * 4)

    max_track_id = max(1, min(int(scale), 5000))
    track_ids = list(range(1, max_track_id + 1))

    docs = []
    base_entry_id = _new_int_id(scale + 50_000_000)
    for i in range(count):
        docs.append(
            {
                "chart_entry_id": base_entry_id + i,
                "chart_id": int(chart_id),
                "track_id": int(track_ids[i % len(track_ids)]),
                "chart_date": old_start - timedelta(days=(i % 3650)),
                "position": int((i % 200) + 1),
                "streams": int(100000 + (i % 4900000)),
                "bench_tag": "old_delete",
            }
        )

    if docs:
        db.chart_entries.insert_many(docs, ordered=False)
    return len(docs)


def _setup_tracks_for_concurrent_delete(db: Any, *, count: int, scale: int) -> tuple[str, list[int]]:
    if count <= 0:
        return "", []

    now = datetime.now(tz=timezone.utc)
    tag = uuid.uuid4().hex[:6]
    prefix = f"cc{tag}"
    base_track_id = scale + 60_000_000 + (uuid.uuid4().int % 1_000_000)

    docs = []
    ids: list[int] = []
    for i in range(count):
        track_id = base_track_id + i
        ids.append(track_id)
        docs.append(
            {
                "track_id": track_id,
                "spotify_track_id": f"{prefix}{i:014d}",
                "name": f"ConcBulk_{tag}_{i}",
                "explicit": False,
                "duration_min": 3.0,
                "disc_number": 1,
                "track_number": 1,
                "isrc": _isrc_from_tag(tag, i),
                "created_at": now,
                "updated_at": now,
                "bench_tag": tag,
            }
        )

    db.tracks.insert_many(docs, ordered=False)
    return tag, ids


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------


def scenario_point_delete(db: Any, *, scale: int, _samples: dict) -> tuple[float, int]:
    market_id = _setup_market(db, scale=scale)

    start = time.perf_counter()
    res = db.markets.delete_one({"market_id": market_id})
    elapsed = time.perf_counter() - start
    return elapsed, int(res.deleted_count)


def scenario_cascade_delete(db: Any, *, scale: int, samples: dict) -> tuple[float, int]:
    global _WARNED_CASCADE
    if not _WARNED_CASCADE:
        print(
            "[MOCK] cascade_delete: MongoDB nie ma ON DELETE CASCADE między kolekcjami – "
            "symuluję to jako 3 delete'y (album + album_artists + track_albums)."
        )
        _WARNED_CASCADE = True

    artist_id = int(random.choice(samples["artist_ids"]))
    album_id, _track_id = _setup_album_with_relations(db, artist_id=artist_id, scale=scale)

    start = time.perf_counter()
    r1 = db.track_albums.delete_many({"album_id": album_id})
    r2 = db.album_artists.delete_many({"album_id": album_id})
    r3 = db.albums.delete_one({"album_id": album_id})
    elapsed = time.perf_counter() - start

    affected = int(r1.deleted_count) + int(r2.deleted_count) + int(r3.deleted_count)
    return elapsed, affected


def scenario_relationship_delete(db: Any, *, scale: int, samples: dict) -> tuple[float, int]:
    artist_id = int(random.choice(samples["artist_ids"]))
    rel_count = _scaled_count(scale, 0.001, min_count=500, max_count=50000)
    tag, track_ids = _setup_track_artist_relations_bulk(db, artist_id=artist_id, count=rel_count, scale=scale)

    start = time.perf_counter()
    res = db.track_artists.delete_many({"artist_id": artist_id, "bench_tag": tag})
    elapsed = time.perf_counter() - start

    # Cleanup tracks outside timed window
    db.tracks.delete_many({"track_id": {"$in": track_ids}})

    return elapsed, int(res.deleted_count)


def scenario_range_delete(db: Any, *, scale: int, samples: dict) -> tuple[float, int]:
    chart_id = int(samples["chart_id"])
    entries_count = _scaled_count(scale, 0.005, min_count=2000, max_count=200000)
    _setup_old_chart_entries(db, chart_id=chart_id, count=entries_count, scale=scale)

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=365 * 3)

    start = time.perf_counter()
    res = db.chart_entries.delete_many({"chart_id": chart_id, "chart_date": {"$lt": cutoff}})
    elapsed = time.perf_counter() - start

    return elapsed, int(res.deleted_count)


def _concurrent_delete_worker(cfg: DbConfig, *, tag: str, chunks: list[list[int]]) -> int:
    if not chunks:
        return 0

    client, db = connect_db(cfg, max_pool_size=300)
    try:
        deleted = 0
        for ids in chunks:
            if not ids:
                continue
            res = db.tracks.delete_many({"bench_tag": tag, "track_id": {"$in": ids}})
            deleted += int(res.deleted_count)
        return deleted
    finally:
        client.close()


def scenario_concurrent_delete(
    cfg: DbConfig,
    db: Any,
    *,
    scale: int,
    workers: int = 50,
    chunk_size: int = 500,
) -> tuple[float, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    delete_count = _scaled_count(scale, 0.0005, min_count=workers, max_count=50000)
    tag, track_ids = _setup_tracks_for_concurrent_delete(db, count=delete_count, scale=scale)
    if not track_ids:
        return 0.0, 0

    chunks_per_worker = max(1, (len(track_ids) + chunk_size - 1) // chunk_size // workers + 1)

    rng = random.Random(uuid.uuid4().int & 0xFFFFFFFF)
    buckets: list[list[list[int]]] = []
    for _w in range(workers):
        worker_chunks: list[list[int]] = []
        for _c in range(chunks_per_worker):
            sample = rng.sample(track_ids, k=min(chunk_size, len(track_ids)))
            worker_chunks.append(sample)
        buckets.append(worker_chunks)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda worker_chunks: _concurrent_delete_worker(cfg, tag=tag, chunks=worker_chunks), buckets))
    elapsed = time.perf_counter() - start

    # Cleanup leftovers outside timed window
    db.tracks.delete_many({"bench_tag": tag})

    return elapsed, int(sum(results))


def scenario_soft_delete(db: Any, *, samples: dict) -> tuple[float, int]:
    album_id = int(random.choice(samples["album_ids"]))

    start = time.perf_counter()
    res = db.albums.update_one({"album_id": album_id}, {"$set": {"updated_at": datetime.now(tz=timezone.utc)}})
    elapsed = time.perf_counter() - start
    return elapsed, int(res.modified_count)


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------


def run_benchmark(
    cfg: DbConfig,
    *,
    scales: Optional[list[int]],
    runs_per_scenario: int,
    index_modes: list[bool],
    concurrent_workers: int,
    concurrent_chunk_size: int,
    skip_prepare: bool,
    reseed_per_index_mode: bool,
    seed_value: Optional[int],
    pool_size: int,
) -> list[dict]:
    results: list[dict] = []

    scales_to_run = scales if scales else [None]

    if not index_modes:
        raise ValueError("index_modes must not be empty")

    for scale in scales_to_run:
        if scale is not None and not skip_prepare and not reseed_per_index_mode:
            print(f"\n[PREP] Seeding scale={scale:,} via seed_mongodb_faker_data.py ...")
            prepare_scale_data_with_seed_script(
                cfg=cfg,
                target_rows=scale,
                seed_value=seed_value,
                pool_size=pool_size,
            )

        if reseed_per_index_mode and scale is not None:
            for with_indexes in index_modes:
                if not skip_prepare:
                    print(f"\n[PREP] (per index mode) Seeding scale={scale:,} ...")
                    prepare_scale_data_with_seed_script(
                        cfg=cfg,
                        target_rows=scale,
                        seed_value=seed_value,
                        pool_size=pool_size,
                    )

                client, db = connect_db(cfg)
                try:
                    ensure_existing_schema(db)
                    effective_scale = int(scale)
                    samples = fetch_sample_ids(db, scale=effective_scale)

                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(db, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(db, scale=effective_scale, _samples=samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(db, scale=effective_scale, samples=samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(db, scale=effective_scale, samples=samples)),
                        ("range_delete", lambda: scenario_range_delete(db, scale=effective_scale, samples=samples)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                db,
                                scale=effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(db, samples=samples)),
                    ]

                    for scenario_name, scenario_fn in scenario_defs:
                        for run_idx in range(1, runs_per_scenario + 1):
                            elapsed, ops = scenario_fn()
                            results.append(
                                {
                                    "scale": effective_scale,
                                    "index_mode": index_label,
                                    "scenario": scenario_name,
                                    "run": run_idx,
                                    "seconds": elapsed,
                                    "rows_affected": int(ops),
                                    "ops_per_sec": (ops / elapsed) if elapsed > 0 and ops > 0 else None,
                                }
                            )
                finally:
                    client.close()
        else:
            client, db = connect_db(cfg)
            try:
                ensure_existing_schema(db)
                effective_scale = int(scale) if scale is not None else int(db.tracks.estimated_document_count())

                samples = fetch_sample_ids(db, scale=effective_scale)

                for with_indexes in index_modes:
                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(db, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(db, scale=effective_scale, _samples=samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(db, scale=effective_scale, samples=samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(db, scale=effective_scale, samples=samples)),
                        ("range_delete", lambda: scenario_range_delete(db, scale=effective_scale, samples=samples)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                db,
                                scale=effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(db, samples=samples)),
                    ]

                    for scenario_name, scenario_fn in scenario_defs:
                        for run_idx in range(1, runs_per_scenario + 1):
                            elapsed, ops = scenario_fn()
                            results.append(
                                {
                                    "scale": effective_scale,
                                    "index_mode": index_label,
                                    "scenario": scenario_name,
                                    "run": run_idx,
                                    "seconds": elapsed,
                                    "rows_affected": int(ops),
                                    "ops_per_sec": (ops / elapsed) if elapsed > 0 and ops > 0 else None,
                                }
                            )
            finally:
                client.close()

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (int(row.get("scale", 0) or 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== MongoDB DELETE Benchmark Summary (avg z prób) ===")
    print(f"{'scale':>10} {'index_mode':<15} {'scenario':<22} {'avg_sec':>10} {'avg_rows':>10} {'avg_ops/s':>12}")
    print("-" * 85)

    for (scale, index_mode, scenario), rows in sorted(groups.items()):
        avg_sec = mean(r["seconds"] for r in rows)
        avg_rows = mean(r["rows_affected"] for r in rows)
        valid_ops = [r["ops_per_sec"] for r in rows if r["ops_per_sec"] is not None]
        avg_ops = mean(valid_ops) if valid_ops else 0.0
        print(f"{scale:>10} {index_mode:<15} {scenario:<22} {avg_sec:>10.6f} {avg_rows:>10.1f} {avg_ops:>12.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["scale", "index_mode", "scenario", "run", "seconds", "rows_affected", "ops_per_sec"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MongoDB DELETE benchmark – 6 scenariuszy usuwania z porównaniem przed/po indeksach."
    )

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
    parser.add_argument(
        "--scales",
        default=None,
        help=(
            "Lista skal (np. 500000,1000000,10000000). "
            "Jeśli podane, skrypt automatycznie seeduje bazę dla każdej skali "
            "używając seed_mongodb_faker_data.py (truncate + seed_all)."
        ),
    )
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--concurrent-workers", type=int, default=50)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile track_id kasuje jeden worker w pojedynczym delete_many (stały chunk size).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie seeduje danych dla skal (zakłada, że baza jest już przygotowana).",
    )
    parser.add_argument(
        "--reseed-per-index-mode",
        action="store_true",
        help=(
            "Jeśli używasz --both-index-modes: reseeduj (truncate+seed) bazę przed każdym trybem indeksów. "
            "Wolniejsze, ale bardziej uczciwe porównanie."
        ),
    )
    parser.add_argument("--output", default="mongodb/results/mongodb_delete_benchmark_results.csv")

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

    scales = parse_scales(args.scales) if args.scales else None

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

    print(f"\n>>> MongoDB DELETE Benchmark – tryb: {mode_label} <<<")

    if scales and not args.skip_prepare:
        print(f"Info: uruchamiam benchmark dla skal: {', '.join(f'{s:,}' for s in scales)}")

    results = run_benchmark(
        cfg,
        scales=scales,
        runs_per_scenario=args.runs_per_scenario,
        index_modes=index_modes,
        concurrent_workers=args.concurrent_workers,
        concurrent_chunk_size=args.concurrent_chunk_size,
        skip_prepare=args.skip_prepare,
        reseed_per_index_mode=args.reseed_per_index_mode,
        seed_value=args.seed_value,
        pool_size=args.pool_size,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
