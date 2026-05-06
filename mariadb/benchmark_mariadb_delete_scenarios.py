"""
MariaDB DELETE benchmark suite - 6 scenariuszy usuwania.
"""

import argparse
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
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
)


MANAGED_INDEXES = [
    {
        "name": "idx_chart_entries_chart_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date ON chart_entries(chart_id, chart_date)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_chart_date ON chart_entries",
        "new": False,
    },
    {
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date)",
        "drop": "DROP INDEX IF EXISTS idx_albums_release_date ON albums",
        "new": False,
    },
    {
        "name": "idx_track_albums_album_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_albums_album_id ON track_albums(album_id)",
        "drop": "DROP INDEX IF EXISTS idx_track_albums_album_id ON track_albums",
        "new": True,
    },
    {
        "name": "idx_track_artists_artist_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id)",
        "drop": "DROP INDEX IF EXISTS idx_track_artists_artist_id ON track_artists",
        "new": True,
    },
]


def _new_spotify_id(prefix: str = "") -> str:
    seed = prefix + uuid.uuid4().hex
    return seed[:22].ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def _count_rows(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return int(cur.fetchone()[0])


def fetch_sample_ids(conn) -> dict:
    samples: dict = {}

    with conn.cursor() as cur:
        cur.execute("SELECT chart_id FROM charts ORDER BY chart_id LIMIT 1")
        row = cur.fetchone()
        samples["chart_id"] = int(row[0]) if row else 1

        cur.execute("SELECT album_id FROM albums ORDER BY album_id LIMIT 100")
        rows = cur.fetchall()
        samples["album_ids"] = [int(r[0]) for r in rows] if rows else [1]

        cur.execute("SELECT artist_id FROM artists ORDER BY artist_id LIMIT 100")
        rows = cur.fetchall()
        samples["artist_ids"] = [int(r[0]) for r in rows] if rows else [1]

    print("[SETUP] Załadowano sample IDs:")
    print(f"  chart_id:    {samples['chart_id']}")
    print(f"  album_ids:   {len(samples['album_ids'])}")
    print(f"  artist_ids:  {len(samples['artist_ids'])}")

    return samples


def _setup_market(conn) -> int:
    code = uuid.uuid4().hex[:2].upper()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO markets (country_code, name) VALUES (%s, %s)",
            (code, f"TempMarket_{uuid.uuid4().hex[:6]}"),
        )
        market_id = int(cur.lastrowid)
    conn.commit()
    return market_id


def _setup_album_with_relations(conn, artist_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO albums (spotify_album_id, name, album_type, release_date, total_tracks, updated_at)
            VALUES (%s, %s, 'single', CURDATE(), 1, NOW())
            """,
            (_new_spotify_id("del_alb"), f"TempAlbum_{uuid.uuid4().hex[:8]}"),
        )
        album_id = int(cur.lastrowid)

        cur.execute(
            "INSERT INTO album_artists (album_id, artist_id, artist_order) VALUES (%s, %s, 1)",
            (album_id, artist_id),
        )

        cur.execute(
            """
            INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc, updated_at)
            VALUES (%s, %s, false, 3.0, 1, 1, %s, NOW())
            """,
            (_new_spotify_id("del_trk"), f"TempTrack_{uuid.uuid4().hex[:8]}", _new_isrc()),
        )
        track_id = int(cur.lastrowid)
        cur.execute(
            "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, true)",
            (track_id, album_id),
        )
    conn.commit()
    return album_id


def _setup_track_artist_relations_bulk(conn, artist_id: int, count: int) -> str:
    tag = uuid.uuid4().hex[:5]
    prefix = f"rb{tag}"

    rows = []
    for i in range(count):
        spotify_track_id = f"{prefix}{i:015d}"
        isrc = f"PL{tag.upper()}{i:05d}"[:12]
        rows.append((spotify_track_id, f"RelBulk_{tag}_{i}", False, 2.5, 1, 1, isrc))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

        cur.execute(
            """
            INSERT INTO track_artists (track_id, artist_id, artist_order)
            SELECT t.track_id, %s, 1
            FROM tracks t
            WHERE t.spotify_track_id LIKE %s
            """,
            (artist_id, f"{prefix}%"),
        )

    conn.commit()
    return prefix


def _setup_old_chart_entries(conn, chart_id: int, count: int) -> int:
    if count <= 0:
        return 0

    old_date = date.today() - timedelta(days=365 * 4)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT IGNORE INTO chart_entries (chart_id, track_id, chart_date, position, streams)
            SELECT
              %s,
              t.track_id,
              DATE_SUB(%s, INTERVAL (ROW_NUMBER() OVER (ORDER BY t.track_id) %% 3650) DAY),
              ((ROW_NUMBER() OVER (ORDER BY t.track_id) - 1) %% 200) + 1,
              100000 + FLOOR(RAND() * 4900000)
            FROM tracks t
            ORDER BY t.track_id
            LIMIT %s
            """,
            (chart_id, old_date, count),
        )
        inserted = int(cur.rowcount or 0)
    conn.commit()
    return inserted


def _setup_tracks_for_concurrent_delete(conn, count: int) -> list[int]:
    tag = uuid.uuid4().hex[:5]
    prefix = f"cc{tag}"

    rows = []
    for i in range(count):
        spotify_track_id = f"{prefix}{i:015d}"
        isrc = f"PL{tag.upper()}{i:05d}"[:12]
        rows.append((spotify_track_id, f"ConcBulk_{tag}_{i}", False, 3.0, 1, 1, isrc))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

        cur.execute("SELECT track_id FROM tracks WHERE spotify_track_id LIKE %s ORDER BY track_id", (f"{prefix}%",))
        track_ids = [int(r[0]) for r in cur.fetchall()]

    conn.commit()
    return track_ids


def scenario_point_delete(conn, _samples: dict) -> tuple[float, int]:
    market_id = _setup_market(conn)

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM markets WHERE market_id = %s", (market_id,))
        affected = int(cur.rowcount)
    conn.commit()

    elapsed = time.perf_counter() - start
    return elapsed, affected


def scenario_cascade_delete(conn, samples: dict) -> tuple[float, int]:
    artist_id = random.choice(samples["artist_ids"])
    album_id = _setup_album_with_relations(conn, artist_id)

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM albums WHERE album_id = %s", (album_id,))
        affected = int(cur.rowcount)
    conn.commit()

    elapsed = time.perf_counter() - start
    return elapsed, affected


def scenario_relationship_delete(conn, samples: dict, scale: int) -> tuple[float, int]:
    artist_id = random.choice(samples["artist_ids"])
    rel_count = scaled_count(scale, 0.001, min_count=500, max_count=50_000)
    prefix = _setup_track_artist_relations_bulk(conn, artist_id, rel_count)

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE ta
            FROM track_artists ta
            JOIN tracks t ON ta.track_id = t.track_id
            WHERE ta.artist_id = %s
              AND t.spotify_track_id LIKE %s
            """,
            (artist_id, f"{prefix}%"),
        )
        affected = int(cur.rowcount)
    conn.commit()

    elapsed = time.perf_counter() - start

    with conn.cursor() as cur:
        cur.execute("DELETE FROM tracks WHERE spotify_track_id LIKE %s", (f"{prefix}%",))
    conn.commit()

    return elapsed, affected


def scenario_range_delete(conn, samples: dict, scale: int) -> tuple[float, int]:
    chart_id = samples["chart_id"]
    entries_count = scaled_count(scale, 0.005, min_count=2_000, max_count=200_000)
    _setup_old_chart_entries(conn, chart_id, count=entries_count)

    cutoff = date.today() - timedelta(days=365 * 3)

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM chart_entries WHERE chart_id = %s AND chart_date < %s",
            (chart_id, cutoff),
        )
        affected = int(cur.rowcount)
    conn.commit()

    elapsed = time.perf_counter() - start
    return elapsed, affected


def _concurrent_delete_worker(cfg: DbConfig, chunks: list[list[int]]) -> int:
    if not chunks:
        return 0

    with connect_db(cfg) as conn:
        deleted = 0
        with conn.cursor() as cur:
            for ids in chunks:
                if not ids:
                    continue
                fmt = ",".join(["%s"] * len(ids))
                cur.execute(f"DELETE FROM tracks WHERE track_id IN ({fmt})", ids)
                deleted += int(cur.rowcount or 0)
        conn.commit()
    return deleted


def scenario_concurrent_delete(
    cfg: DbConfig,
    conn,
    scale: int,
    workers: int = 50,
    chunk_size: int = 500,
) -> tuple[float, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    delete_count = scaled_count(scale, 0.0005, min_count=workers, max_count=50_000)
    track_ids = _setup_tracks_for_concurrent_delete(conn, count=delete_count)

    chunks = [track_ids[i : i + chunk_size] for i in range(0, len(track_ids), chunk_size)]

    buckets: list[list[list[int]]] = [[] for _ in range(max(1, workers))]
    for i, chunk in enumerate(chunks):
        buckets[i % len(buckets)].append(chunk)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda worker_chunks: _concurrent_delete_worker(cfg, worker_chunks), buckets))
    elapsed = time.perf_counter() - start
    return elapsed, sum(results)


def scenario_soft_delete(conn, samples: dict) -> tuple[float, int]:
    album_id = random.choice(samples["album_ids"])

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("UPDATE albums SET updated_at = NOW() WHERE album_id = %s", (album_id,))
        affected = int(cur.rowcount)
    conn.commit()

    elapsed = time.perf_counter() - start
    return elapsed, affected


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
            print(f"\n[PREP] Seeding scale={scale:,} via seed_mariadb_faker_data.py ...")
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
                    print(f"\n[PREP] (per index mode) Seeding scale={scale:,} via seed_mariadb_faker_data.py ...")
                    prepare_scale_data_with_seed_script(
                        cfg=cfg,
                        target_rows=scale,
                        seed_value=seed_value,
                        pool_size=pool_size,
                        include_audio_features=False,
                    )

                with connect_db(cfg) as conn:
                    ensure_existing_schema(conn)
                    effective_scale = scale
                    samples = fetch_sample_ids(conn)

                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(conn, MANAGED_INDEXES, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(conn, samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(conn, samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(conn, samples, effective_scale)),
                        ("range_delete", lambda: scenario_range_delete(conn, samples, effective_scale)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                conn,
                                effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(conn, samples)),
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
        else:
            with connect_db(cfg) as conn:
                ensure_existing_schema(conn)
                effective_scale = scale if scale is not None else _count_rows(conn, "tracks")

                samples = fetch_sample_ids(conn)

                for with_indexes in index_modes:
                    index_label = "with_indexes" if with_indexes else "no_indexes"
                    apply_indexes(conn, MANAGED_INDEXES, with_indexes)

                    scenario_defs = [
                        ("point_delete", lambda: scenario_point_delete(conn, samples)),
                        ("cascade_delete", lambda: scenario_cascade_delete(conn, samples)),
                        ("relationship_delete", lambda: scenario_relationship_delete(conn, samples, effective_scale)),
                        ("range_delete", lambda: scenario_range_delete(conn, samples, effective_scale)),
                        (
                            "concurrent_delete",
                            lambda: scenario_concurrent_delete(
                                cfg,
                                conn,
                                effective_scale,
                                workers=concurrent_workers,
                                chunk_size=concurrent_chunk_size,
                            ),
                        ),
                        ("soft_delete", lambda: scenario_soft_delete(conn, samples)),
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
        description="MariaDB DELETE benchmark - 6 scenariuszy usuwania z porownaniem przed/po indeksach."
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
            "uzywajac seed_mariadb_faker_data.py (TRUNCATE + seed_all)."
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
    parser.add_argument("--output", default="mariadb/results/mariadb_delete_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3307")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))

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
    )

    if args.both_index_modes:
        index_modes = [False, True]
        mode_label = "BEZ indeksow + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksow"

    print(f"\n>>> MariaDB DELETE Benchmark - tryb: {mode_label} <<<")

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
