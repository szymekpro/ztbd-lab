"""
PostgreSQL DELETE benchmark suite – 6 scenariuszy usuwania.

Scenariusze:
  1. point_delete        – usunięcie pojedynczego rynku (markets)
  2. cascade_delete      – usunięcie albumu (CASCADE -> track_albums, album_artists)
  3. relationship_delete – usunięcie artysty-współtwórcy z track_artists
  4. range_delete        – usunięcie starych chart_entries (> 3 lata)
  5. concurrent_delete   – równoległe usuwanie tracków z wielu wątków
  6. soft_delete         – UPDATE albums SET updated_at=now() zamiast DELETE

WAŻNE: Scenariusze 1–3, 4 i 5 TWORZĄ dane tuż przed pomiarem a następnie je usuwają,
żeby czas dotyczył TYLKO operacji DELETE (bez contamination setupu).
Scenariusz 6 działa na istniejących danych.

Uruchomienie (z katalogu postgres/):
  python benchmark_psql_delete_scenarios.py --no-indexes
  python benchmark_psql_delete_scenarios.py
"""

import argparse
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import mean
from typing import Optional

import psycopg


# ---------------------------------------------------------------------------
# CONFIG / CONNECTION
# ---------------------------------------------------------------------------

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


def _count_rows(conn: psycopg.Connection, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table_name}")
        return int(cur.fetchone()[0])


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


def prepare_scale_data_with_seed_script(
    cfg: DbConfig,
    target_rows: int,
    seed_value: Optional[int],
    pool_size: int,
) -> None:
    import sys
    from pathlib import Path

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


# ---------------------------------------------------------------------------
# INDEX MANAGEMENT
# ---------------------------------------------------------------------------

MANAGED_INDEXES = [
    {
        "name": "idx_chart_entries_chart_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date ON chart_entries(chart_id, chart_date)",
        "drop":   "DROP INDEX IF EXISTS idx_chart_entries_chart_date",
        "new": False,
    },
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


# ---------------------------------------------------------------------------
# HELPER: generate IDs
# ---------------------------------------------------------------------------

def _new_spotify_id(prefix: str = "") -> str:
    seed = prefix + uuid.uuid4().hex
    return seed[:22].ljust(22, "0")


def _new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def _scaled_count(scale: int, fraction: float, *, min_count: int, max_count: int) -> int:
    if scale <= 0:
        return min_count
    return max(min_count, min(int(scale * fraction), max_count))


# ---------------------------------------------------------------------------
# SAMPLE ID FETCHING
# ---------------------------------------------------------------------------

def fetch_sample_ids(conn: psycopg.Connection) -> dict:
    samples: dict = {}
    with conn.cursor() as cur:
        # chart_id for seeding range_delete test data
        cur.execute("SELECT chart_id FROM charts ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        samples["chart_id"] = row[0] if row else None

        # existing album_ids (for soft_delete scenario S6)
        cur.execute(
            """
            SELECT album_id FROM albums
            ORDER BY random()
            LIMIT 100
            """
        )
        rows = cur.fetchall()
        samples["album_ids"] = [r[0] for r in rows] if rows else [1]

        # artist with multiple tracks (for cascade test setup)
        cur.execute(
            """
            SELECT artist_id FROM artists
            ORDER BY random()
            LIMIT 20
            """
        )
        rows = cur.fetchall()
        samples["artist_ids"] = [r[0] for r in rows] if rows else [1]

    conn.commit()
    print(f"[SETUP] Załadowano sample IDs:")
    print(f"  chart_id:    {samples['chart_id']}")
    print(f"  album_ids:   {len(samples['album_ids'])}")
    print(f"  artist_ids:  {len(samples['artist_ids'])}")
    return samples


# ---------------------------------------------------------------------------
# SETUP HELPERS (create temporary data to delete)
# ---------------------------------------------------------------------------

def _setup_market(conn: psycopg.Connection) -> int:
    """Insert a temporary market row and return its market_id."""
    code = uuid.uuid4().hex[:2].upper()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO markets (country_code, name) VALUES (%s, %s) RETURNING market_id",
            (code, f"TempMarket_{uuid.uuid4().hex[:6]}"),
        )
        market_id = int(cur.fetchone()[0])
    conn.commit()
    return market_id


def _setup_album_with_relations(conn: psycopg.Connection, artist_id: int) -> int:
    """Insert album + link to artist, return album_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO albums (spotify_album_id, name, album_type, release_date, total_tracks)
            VALUES (%s, %s, 'single', current_date, 1)
            RETURNING album_id
            """,
            (_new_spotify_id("del_alb"), f"TempAlbum_{uuid.uuid4().hex[:8]}"),
        )
        album_id = int(cur.fetchone()[0])

        # add artist link
        cur.execute(
            "INSERT INTO album_artists (album_id, artist_id, artist_order) VALUES (%s, %s, 1)",
            (album_id, artist_id),
        )

        # add a track + track_album link (CASCADE test)
        cur.execute(
            """
            INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
            VALUES (%s, %s, false, 3.0, 1, 1, %s)
            RETURNING track_id
            """,
            (_new_spotify_id("del_trk"), f"TempTrack_{uuid.uuid4().hex[:8]}", _new_isrc()),
        )
        track_id = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (%s, %s, true)",
            (track_id, album_id),
        )
    conn.commit()
    return album_id


def _setup_track_artist_relation(conn: psycopg.Connection, artist_id: int) -> tuple[int, int]:
    """Insert a new track and link it to artist_id, return (track_id, artist_id)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc)
            VALUES (%s, %s, false, 2.5, 1, 1, %s)
            RETURNING track_id
            """,
            (_new_spotify_id("rel_trk"), f"RelTrack_{uuid.uuid4().hex[:8]}", _new_isrc()),
        )
        track_id = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (%s, %s, 1)",
            (track_id, artist_id),
        )
    conn.commit()
    return track_id, artist_id


def _setup_track_artist_relations_bulk(conn: psycopg.Connection, artist_id: int, count: int) -> str:
    """Insert `count` temporary tracks + relations to one artist and return the spotify_track_id prefix tag."""
    # We avoid returning IDs by tagging spotify_track_id with a unique prefix and using it in SQL.
    tag = uuid.uuid4().hex[:5]
    prefix = f"rb{tag}"  # 2 + 5 = 7 chars

    with conn.cursor() as cur:
        with cur.copy(
            "COPY tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc) FROM STDIN"
        ) as copy:
            for i in range(count):
                spotify_track_id = f"{prefix}{i:015d}"  # 7 + 15 = 22
                # 12 chars total
                isrc = f"PL{tag.upper()}{i:05d}"[:12]
                copy.write_row(
                    (
                        spotify_track_id,
                        f"RelBulk_{tag}_{i}",
                        False,
                        2.5,
                        1,
                        1,
                        isrc,
                    )
                )

        # Link every inserted track to the chosen artist (setup is outside timed window)
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


def _setup_old_chart_entries(conn: psycopg.Connection, chart_id: int, count: int) -> int:
    """Insert `count` chart_entries with chart_date older than 3 years (fast, set-based)."""
    if chart_id is None or count <= 0:
        return 0

    old_date = date.today() - timedelta(days=365 * 4)  # 4 years ago
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH sel AS (
              SELECT track_id, row_number() OVER () AS rn
              FROM (SELECT track_id FROM tracks ORDER BY track_id LIMIT %s) t
            )
            INSERT INTO chart_entries (chart_id, track_id, chart_date, position, streams)
            SELECT
              %s,
              sel.track_id,
              (%s::date - (sel.rn %% 3650) * interval '1 day')::date,
              ((sel.rn - 1) %% 200) + 1,
              (100000 + (random() * 4900000)::int)
            FROM sel
            ON CONFLICT DO NOTHING
            """,
            (count, chart_id, old_date),
        )
        inserted = cur.rowcount or 0
    conn.commit()
    return int(inserted)


def _setup_tracks_for_concurrent_delete(conn: psycopg.Connection, count: int) -> list[int]:
    """Insert `count` temporary tracks (fast) and return their track_ids."""
    if count <= 0:
        return []

    tag = uuid.uuid4().hex[:5]
    prefix = f"cc{tag}"  # 7 chars

    with conn.cursor() as cur:
        with cur.copy(
            "COPY tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc) FROM STDIN"
        ) as copy:
            for i in range(count):
                spotify_track_id = f"{prefix}{i:015d}"  # 7 + 15 = 22
                isrc = f"PL{tag.upper()}{i:05d}"[:12]
                copy.write_row(
                    (
                        spotify_track_id,
                        f"ConcBulk_{tag}_{i}",
                        False,
                        3.0,
                        1,
                        1,
                        isrc,
                    )
                )

        cur.execute(
            "SELECT track_id FROM tracks WHERE spotify_track_id LIKE %s ORDER BY track_id",
            (f"{prefix}%",),
        )
        track_ids = [int(r[0]) for r in cur.fetchall()]

    conn.commit()
    return track_ids


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------

def scenario_point_delete(conn: psycopg.Connection, _samples: dict) -> int:
    """
    S1 – Point Delete: usuń pojedynczy rynek (markets) po market_id.
    Dane są insertowane tuż przed pomiarem; mierzymy czas DELETE.
    """
    market_id = _setup_market(conn)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM markets WHERE market_id = %s", (market_id,))
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


def scenario_cascade_delete(conn: psycopg.Connection, samples: dict) -> tuple[float, int]:
    """
    S2 – Cascade Delete: usuń album -> automatyczny CASCADE na track_albums i album_artists.
    """
    artist_id = random.choice(samples["artist_ids"])
    album_id = _setup_album_with_relations(conn, artist_id)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM albums WHERE album_id = %s", (album_id,))
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


def scenario_relationship_delete(conn: psycopg.Connection, samples: dict, scale: int) -> tuple[float, int]:
    """
    S3 – Relationship Delete (scaled): usuń powiązania track_artists dla paczki tracków.
    Liczba relacji do skasowania skaluje się z `scale`.
    """
    artist_id = random.choice(samples["artist_ids"])
    rel_count = _scaled_count(scale, 0.001, min_count=500, max_count=50000)
    prefix = _setup_track_artist_relations_bulk(conn, artist_id, rel_count)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM track_artists ta
                USING tracks t
                WHERE ta.track_id = t.track_id
                  AND ta.artist_id = %s
                  AND t.spotify_track_id LIKE %s
                """,
                (artist_id, f"{prefix}%"),
            )
            affected = cur.rowcount
    elapsed = time.perf_counter() - start

    # Cleanup outside timed window (leave base dataset intact)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tracks WHERE spotify_track_id LIKE %s", (f"{prefix}%",))

    return elapsed, affected


def scenario_range_delete(conn: psycopg.Connection, samples: dict, scale: int) -> tuple[float, int]:
    """
    S4 – Range Delete: usuń chart_entries starsze niż 3 lata.
    Dane są wstępnie inserowane (ułamek `scale`); mierzymy DELETE.
    """
    chart_id = samples["chart_id"]
    entries_count = _scaled_count(scale, 0.005, min_count=2000, max_count=200000)
    _setup_old_chart_entries(conn, chart_id, count=entries_count)

    cutoff = date.today() - timedelta(days=365 * 3)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM chart_entries WHERE chart_id = %s AND chart_date < %s",
                (chart_id, cutoff),
            )
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


def _concurrent_delete_worker(cfg: DbConfig, chunks: list[list[int]]) -> int:
    if not chunks:
        return 0

    # Important: reuse a single DB connection per worker.
    # Creating thousands of short-lived connections on Windows can exhaust ephemeral ports
    # and fail with WSAEADDRINUSE ("Address already in use").
    with connect_db(cfg) as worker_conn:
        deleted = 0
        # Use one transaction for the whole worker batch.
        with worker_conn.transaction():
            with worker_conn.cursor() as cur:
                for ids in chunks:
                    if not ids:
                        continue
                    cur.execute("DELETE FROM tracks WHERE track_id = ANY(%s)", (ids,))
                    deleted += int(cur.rowcount or 0)
        return deleted


def scenario_concurrent_delete(
    cfg: DbConfig,
    conn: psycopg.Connection,
    scale: int,
    workers: int = 50,
    chunk_size: int = 500,
) -> tuple[float, int]:
    """
    S5 – Concurrent Delete: równoległe usuwanie tracków z wielu wątków.
    Testuje mechanizmy rozwiązywania blokad (deadlocks, row-level locking).
    Liczba usuwanych rekordów skaluje się z `scale`, a liczba wątków to `workers`.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    delete_count = _scaled_count(scale, 0.0005, min_count=workers, max_count=50000)
    track_ids = _setup_tracks_for_concurrent_delete(conn, count=delete_count)

    # Fixed chunk size: number of DB roundtrips grows with scale.
    chunks = [track_ids[i : i + chunk_size] for i in range(0, len(track_ids), chunk_size)]

    # Assign chunks to workers (each worker keeps one connection and processes many chunks).
    buckets: list[list[list[int]]] = [[] for _ in range(max(1, workers))]
    for i, ch in enumerate(chunks):
        buckets[i % len(buckets)].append(ch)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda worker_chunks: _concurrent_delete_worker(cfg, worker_chunks), buckets))
    elapsed = time.perf_counter() - start
    return elapsed, sum(results)


def scenario_soft_delete(conn: psycopg.Connection, samples: dict) -> tuple[float, int]:
    """
    S6 – Soft Delete: UPDATE albums SET updated_at=now() zamiast faktycznego DELETE.
    Testuje koszt I/O 'ukrywania' rekordu vs fizycznego kasowania.
    """
    album_id = random.choice(samples["album_ids"])

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE albums SET updated_at = now() WHERE album_id = %s",
                (album_id,),
            )
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------

def run_benchmark(
    cfg: DbConfig,
    scales: Optional[list[int]],
    runs_per_scenario: int,
    index_modes: list[bool],
    concurrent_workers: int,
    concurrent_chunk_size: int,
    skip_prepare: bool,
    seed_value: Optional[int],
    pool_size: int,
) -> list[dict]:
    results: list[dict] = []

    scales_to_run = scales if scales else [None]

    if not index_modes:
        raise ValueError("index_modes must not be empty")

    for scale in scales_to_run:
        if scale is not None and not skip_prepare:
            print(f"\n[PREP] Seeding scale={scale:,} via seed_psql_faker_data.py ...")
            prepare_scale_data_with_seed_script(
                cfg=cfg,
                target_rows=scale,
                seed_value=seed_value,
                pool_size=pool_size,
            )

        with connect_db(cfg) as conn:
            ensure_existing_schema(conn)
            effective_scale = scale if scale is not None else _count_rows(conn, "tracks")

            # Fetch sample IDs once per scale; index toggling shouldn't change them.
            samples = fetch_sample_ids(conn)

            for with_indexes in index_modes:
                index_label = "with_indexes" if with_indexes else "no_indexes"
                apply_indexes(conn, with_indexes)

                # Scenarios return (elapsed, ops) tuples directly (setup is outside timed window)
                scenario_defs = [
                    ("point_delete",        lambda: scenario_point_delete(conn, samples)),
                    ("cascade_delete",      lambda: scenario_cascade_delete(conn, samples)),
                    ("relationship_delete", lambda: scenario_relationship_delete(conn, samples, effective_scale)),
                    ("range_delete",        lambda: scenario_range_delete(conn, samples, effective_scale)),
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
                    ("soft_delete",         lambda: scenario_soft_delete(conn, samples)),
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

    print("\n=== DELETE Benchmark Summary (avg z prób) ===")
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


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="PostgreSQL DELETE benchmark – 6 scenariuszy usuwania z porównaniem przed/po indeksach."
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
            "używając seed_psql_faker_data.py (TRUNCATE + seed_all)."
        ),
    )
    parser.add_argument("--runs-per-scenario",   type=int, default=3)
    parser.add_argument("--concurrent-workers",  type=int, default=50)
    parser.add_argument(
        "--concurrent-chunk-size",
        type=int,
        default=500,
        help="Ile track_id kasuje jeden worker w pojedynczym DELETE (stały chunk size).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie seeduje danych dla skal (zakłada, że baza jest już przygotowana).",
    )
    parser.add_argument("--output", default="postgres/results/psql_delete_benchmark_results.csv")

    parser.add_argument("--db-host",     default=os.getenv("DB_HOST",     "localhost"))
    parser.add_argument("--db-port",     type=int, default=int(os.getenv("DB_PORT", "5434")))
    parser.add_argument("--db-name",     default=os.getenv("DB_NAME",     "spotify_db"))
    parser.add_argument("--db-user",     default=os.getenv("DB_USER",     "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

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
        mode_label = "BEZ indeksów + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksów"

    print(f"\n>>> PostgreSQL DELETE Benchmark – tryb: {mode_label} <<<")

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
        seed_value=args.seed_value,
        pool_size=args.pool_size,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
