"""
Cassandra DELETE benchmark suite - 6 scenariuszy usuwania.
"""

import argparse
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
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
    scaled_count,
)


MANAGED_INDEXES = [
    {
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums (release_date)",
        "drop": "DROP INDEX IF EXISTS idx_albums_release_date",
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


def _count_rows(session, table_name: str) -> int:
    # Cassandra does not support cheap COUNT(*) at scale.
    # For compatibility with the runner we estimate from a bounded scan.
    rows = list(session.execute(f"SELECT * FROM {table_name} LIMIT 100000"))
    return len(rows)


def fetch_sample_ids(session) -> dict:
    samples: dict = {}

    chart_ids = [int(r.chart_id) for r in session.execute("SELECT chart_id FROM charts LIMIT 20")]
    album_ids = [int(r.album_id) for r in session.execute("SELECT album_id FROM albums LIMIT 200")]
    artist_ids = [int(r.artist_id) for r in session.execute("SELECT artist_id FROM artists LIMIT 200")]
    track_ids = [int(r.track_id) for r in session.execute("SELECT track_id FROM tracks LIMIT 5000")]

    samples["chart_id"] = chart_ids[0] if chart_ids else 1
    samples["album_ids"] = album_ids if album_ids else [1]
    samples["artist_ids"] = artist_ids if artist_ids else [1]
    samples["track_ids"] = track_ids if track_ids else [1]

    print("[SETUP] Zaladowano sample IDs:")
    print(f"  chart_id:    {samples['chart_id']}")
    print(f"  album_ids:   {len(samples['album_ids'])}")
    print(f"  artist_ids:  {len(samples['artist_ids'])}")
    return samples


def _setup_market(session) -> int:
    market_id = new_bigint_id()
    code = new_spotify_id("M")[:2].upper()
    session.execute(
        "INSERT INTO markets (market_id, country_code, name) VALUES (%s, %s, %s)",
        (market_id, code, f"TempMarket_{market_id % 100000}"),
    )
    return market_id


def _setup_album_with_relations(session, artist_id: int) -> tuple[int, int]:
    now = _now()
    album_id = new_bigint_id()
    track_id = new_bigint_id()

    session.execute(
        """
        INSERT INTO albums (album_id, spotify_album_id, name, album_type, release_date, total_tracks, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (album_id, new_spotify_id("dalb"), f"TempAlbum_{album_id % 100000}", "single", date.today(), 1, now, now),
    )
    session.execute(
        "INSERT INTO album_artists (album_id, artist_id, artist_order) VALUES (%s, %s, %s)",
        (album_id, artist_id, 1),
    )
    session.execute(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit, duration_min,
            disc_number, track_number, isrc, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (track_id, new_spotify_id("dtrk"), f"TempTrack_{track_id % 100000}", False, Decimal("3.0"), 1, 1, new_isrc(), now, now),
    )
    session.execute(
        "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, %s)",
        (track_id, album_id, True),
    )
    return album_id, track_id


def _setup_track_artist_relations_bulk(session, artist_id: int, count: int) -> list[int]:
    if count <= 0:
        return []

    now = _now()
    track_ids: list[int] = []

    stmt_track = session.prepare(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit, duration_min,
            disc_number, track_number, isrc, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows = []
    for i in range(count):
        track_id = new_bigint_id()
        track_ids.append(track_id)
        rows.append(
            (
                track_id,
                new_spotify_id("rb"),
                f"RelBulk_{i}",
                False,
                Decimal("2.5"),
                1,
                1,
                new_isrc(),
                now,
                now,
            )
        )

    execute_concurrent_with_args(session, stmt_track, rows, concurrency=200, raise_on_first_error=True)

    stmt_rel = session.prepare("INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (?, ?, ?)")
    rel_rows = [(track_id, artist_id, 1) for track_id in track_ids]
    execute_concurrent_with_args(session, stmt_rel, rel_rows, concurrency=200, raise_on_first_error=True)

    return track_ids


def _setup_old_chart_entries(session, chart_id: int, track_ids: list[int], count: int) -> int:
    if count <= 0:
        return 0

    if not track_ids:
        track_ids = [1]

    old_date = date.today() - timedelta(days=365 * 4)
    stmt = session.prepare(
        """
        INSERT INTO chart_entries (chart_id, chart_date, track_id, chart_entry_id, position, streams)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )

    rows = []
    track_pool = max(1, len(track_ids))
    for i in range(count):
        track_id = track_ids[i % track_pool]
        chart_date = old_date - timedelta(days=i // track_pool)
        rows.append(
            (
                chart_id,
                chart_date,
                track_id,
                new_bigint_id(),
                (i % 200) + 1,
                100000 + (i % 4_900_000),
            )
        )

    execute_concurrent_with_args(session, stmt, rows, concurrency=200, raise_on_first_error=True)
    return len(rows)


def _setup_tracks_for_concurrent_delete(session, count: int) -> list[int]:
    if count <= 0:
        return []

    now = _now()
    stmt = session.prepare(
        """
        INSERT INTO tracks (
            track_id, spotify_track_id, name, explicit, duration_min,
            disc_number, track_number, isrc, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows = []
    track_ids = []
    for i in range(count):
        track_id = new_bigint_id()
        track_ids.append(track_id)
        rows.append(
            (
                track_id,
                new_spotify_id("cc"),
                f"ConcBulk_{i}",
                False,
                Decimal("3.0"),
                1,
                1,
                new_isrc(),
                now,
                now,
            )
        )

    execute_concurrent_with_args(session, stmt, rows, concurrency=200, raise_on_first_error=True)
    return track_ids


def scenario_point_delete(session, _samples: dict) -> tuple[float, int]:
    market_id = _setup_market(session)

    start = time.perf_counter()
    session.execute("DELETE FROM markets WHERE market_id = %s", (market_id,))
    elapsed = time.perf_counter() - start
    return elapsed, 1


def scenario_cascade_delete(session, samples: dict) -> tuple[float, int]:
    artist_id = random.choice(samples["artist_ids"])
    album_id, track_id = _setup_album_with_relations(session, artist_id)

    start = time.perf_counter()
    affected = 0

    rel_rows = list(
        session.execute(
            "SELECT track_id, album_id FROM track_albums WHERE album_id = %s ALLOW FILTERING",
            (album_id,),
        )
    )
    for row in rel_rows:
        session.execute(
            "DELETE FROM track_albums WHERE track_id = %s AND album_id = %s",
            (row.track_id, row.album_id),
        )
        affected += 1

    session.execute("DELETE FROM album_artists WHERE album_id = %s", (album_id,))
    affected += 1

    session.execute("DELETE FROM albums WHERE album_id = %s", (album_id,))
    affected += 1

    elapsed = time.perf_counter() - start

    session.execute("DELETE FROM tracks WHERE track_id = %s", (track_id,))
    return elapsed, affected


def scenario_relationship_delete(session, samples: dict, scale: int) -> tuple[float, int]:
    artist_id = random.choice(samples["artist_ids"])
    rel_count = scaled_count(scale, 0.001, min_count=500, max_count=50_000)
    created_track_ids = _setup_track_artist_relations_bulk(session, artist_id, rel_count)
    created_set = set(created_track_ids)

    start = time.perf_counter()
    affected = 0

    rel_rows = list(
        session.execute(
            "SELECT track_id, artist_id FROM track_artists WHERE artist_id = %s LIMIT %s ALLOW FILTERING",
            (artist_id, rel_count * 2),
        )
    )

    for row in rel_rows:
        track_id = int(row.track_id)
        if track_id not in created_set:
            continue
        session.execute(
            "DELETE FROM track_artists WHERE track_id = %s AND artist_id = %s",
            (track_id, artist_id),
        )
        affected += 1

    elapsed = time.perf_counter() - start

    for track_id in created_track_ids:
        session.execute("DELETE FROM tracks WHERE track_id = %s", (track_id,))

    return elapsed, affected


def scenario_range_delete(session, samples: dict, scale: int) -> tuple[float, int]:
    chart_id = samples["chart_id"]
    entries_count = scaled_count(scale, 0.005, min_count=2_000, max_count=200_000)
    _setup_old_chart_entries(session, chart_id, samples["track_ids"], entries_count)

    cutoff = date.today() - timedelta(days=365 * 3)

    start = time.perf_counter()
    # CQL supports range deletes on clustering columns – equivalent to
    # PostgreSQL's single-statement DELETE WHERE chart_date < cutoff.
    session.execute(
        "DELETE FROM chart_entries WHERE chart_id = %s AND chart_date < %s",
        (chart_id, cutoff),
    )
    elapsed = time.perf_counter() - start

    # Approximate affected rows by counting what was set up (all entries are older than cutoff).
    return elapsed, entries_count


def _concurrent_delete_worker(cfg: DbConfig, ids: list[int]) -> int:
    if not ids:
        return 0

    cluster = None
    session = None
    try:
        cluster, session = connect_db(cfg)
        stmt = session.prepare("DELETE FROM tracks WHERE track_id = ?")
        execute_concurrent_with_args(
            session,
            stmt,
            [(track_id,) for track_id in ids],
            concurrency=min(300, max(50, len(ids) // 10)),
            raise_on_first_error=True,
        )
        return len(ids)
    finally:
        close_db(cluster, session)


def scenario_concurrent_delete(
    cfg: DbConfig,
    session,
    scale: int,
    workers: int = 50,
    chunk_size: int = 500,
) -> tuple[float, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    delete_count = scaled_count(scale, 0.0005, min_count=workers, max_count=50_000)
    track_ids = _setup_tracks_for_concurrent_delete(session, count=delete_count)

    chunks = [track_ids[i : i + chunk_size] for i in range(0, len(track_ids), chunk_size)]

    buckets: list[list[int]] = [[] for _ in range(max(1, workers))]
    for i, chunk in enumerate(chunks):
        buckets[i % len(buckets)].extend(chunk)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda worker_ids: _concurrent_delete_worker(cfg, worker_ids), buckets))
    elapsed = time.perf_counter() - start
    return elapsed, sum(results)


def scenario_soft_delete(session, samples: dict) -> tuple[float, int]:
    album_id = random.choice(samples["album_ids"])

    start = time.perf_counter()
    session.execute(
        "UPDATE albums SET updated_at = %s WHERE album_id = %s",
        (_now(), album_id),
    )
    elapsed = time.perf_counter() - start
    return elapsed, 1


def run_benchmark(
    cfg: DbConfig,
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
            print(f"\n[PREP] Seeding scale={scale:,} via seed_cassandra_faker_data.py ...")
            prepare_scale_data_with_seed_script(
                cfg=cfg,
                target_rows=scale,
                seed_value=seed_value,
                pool_size=pool_size,
                include_audio_features=False,
            )

        if reseed_per_index_mode and scale is not None:
            for with_indexes in index_modes:
                if not skip_prepare:
                    print(f"\n[PREP] (per index mode) Seeding scale={scale:,} via seed_cassandra_faker_data.py ...")
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
                    effective_scale = scale
                    samples = fetch_sample_ids(session)

                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(session, MANAGED_INDEXES, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(session, samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(session, samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(session, samples, effective_scale)),
                        ("range_delete", lambda: scenario_range_delete(session, samples, effective_scale)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                session,
                                effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(session, samples)),
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
                                    "rows_affected": ops,
                                    "ops_per_sec": (ops / elapsed) if elapsed > 0 and ops > 0 else None,
                                }
                            )
                finally:
                    close_db(cluster, session)
        else:
            cluster = None
            session = None
            try:
                cluster, session = connect_db(cfg)
                ensure_existing_schema(session, cfg.keyspace)
                effective_scale = scale if scale is not None else _count_rows(session, "tracks")
                samples = fetch_sample_ids(session)

                for with_indexes in index_modes:
                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(session, MANAGED_INDEXES, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(session, samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(session, samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(session, samples, effective_scale)),
                        ("range_delete", lambda: scenario_range_delete(session, samples, effective_scale)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                session,
                                effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(session, samples)),
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
                                    "rows_affected": ops,
                                    "ops_per_sec": (ops / elapsed) if elapsed > 0 and ops > 0 else None,
                                }
                            )
            finally:
                close_db(cluster, session)

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (int(row.get("scale", 0) or 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== DELETE Benchmark Summary (avg z prob) ===")
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
        description="Cassandra DELETE benchmark - 6 scenariuszy usuwania z porownaniem przed/po indeksach."
    )

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
    parser.add_argument(
        "--scales",
        default=None,
        help=(
            "Lista skal (np. 500000,1000000,10000000). "
            "Jesli podane, skrypt automatycznie seeduje baze dla kazdej skali "
            "uzywajac seed_cassandra_faker_data.py (TRUNCATE + seed_all)."
        ),
    )
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--concurrent-workers", type=int, default=50)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile track_id kasuje jeden worker w pojedynczym DELETE (staly chunk size).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie seeduje danych dla skal (zaklada, ze baza jest juz przygotowana).",
    )
    parser.add_argument(
        "--reseed-per-index-mode",
        action="store_true",
        help=(
            "Jesli uzywasz --both-index-modes: reseeduj (TRUNCATE+seed) baze przed kazdym trybem indeksow. "
            "Wolniejsze, ale bardziej uczciwe porownanie."
        ),
    )
    parser.add_argument("--output", default="cassandra/results/cassandra_delete_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "9043")))
    parser.add_argument("--db-keyspace", default=os.getenv("DB_KEYSPACE", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))

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
        keyspace=args.db_keyspace,
        user=args.db_user or None,
        password=args.db_password or None,
    )

    if args.both_index_modes:
        index_modes = [False, True]
        mode_label = "BEZ indeksow + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksow"

    print(f"\n>>> Cassandra DELETE Benchmark - tryb: {mode_label} <<<")

    if scales and not args.skip_prepare:
        print(f"Info: uruchamiam benchmark dla skal: {', '.join(f'{s:,}' for s in scales)}")

    results = run_benchmark(
        cfg=cfg,
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
