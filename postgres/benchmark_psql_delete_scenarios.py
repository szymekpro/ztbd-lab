"""
PostgreSQL DELETE benchmark suite – 6 scenariuszy usuwania.

Scenariusze:
  1. point_delete        – usunięcie pojedynczego rynku (markets)
  2. cascade_delete      – usunięcie albumu (CASCADE -> track_albums, album_artists)
  3. relationship_delete – usunięcie artysty-współtwórcy z track_artists
  4. range_delete        – usunięcie starych chart_entries (> 3 lata)
  5. concurrent_delete   – równoległe usuwanie tracków z wielu wątków
  6. soft_delete         – UPDATE albums SET updated_at=now() zamiast DELETE

WAŻNE: Scenariusze 1–3 i 5 TWORZĄ dane tuż przed pomiarem a następnie je usuwają,
żeby czas dotyczył TYLKO operacji DELETE (bez contamination setupu).
Scenariusz 4 i 6 działają na istniejących danych.

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


def _setup_old_chart_entries(conn: psycopg.Connection, chart_id: int, count: int = 1000) -> int:
    """Insert `count` chart_entries with chart_date > 3 years ago."""
    if chart_id is None:
        return 0
    old_date = date.today() - timedelta(days=365 * 4)  # 4 years ago

    # We need track_ids; reuse random existing ones
    with conn.cursor() as cur:
        cur.execute("SELECT track_id FROM tracks ORDER BY random() LIMIT %s", (count,))
        track_ids = [r[0] for r in cur.fetchall()]

    if not track_ids:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for i, tid in enumerate(track_ids):
            # vary the date slightly so UNIQUE constraint (chart_id, track_id, chart_date) holds
            entry_date = old_date - timedelta(days=i % 30)
            cur.execute(
                """
                INSERT INTO chart_entries (chart_id, track_id, chart_date, position, streams)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (chart_id, tid, entry_date, (i % 200) + 1, random.randint(100_000, 5_000_000)),
            )
            inserted += 1
    conn.commit()
    return inserted


def _setup_tracks_for_concurrent_delete(conn: psycopg.Connection, count: int) -> list[int]:
    """Insert `count` temporary tracks and return their track_ids."""
    track_ids = []
    with conn.cursor() as cur:
        for _ in range(count):
            cur.execute(
                """
                INSERT INTO tracks (spotify_track_id, name, explicit, duration_min,
                                    disc_number, track_number, isrc)
                VALUES (%s, %s, false, 3.0, 1, 1, %s)
                RETURNING track_id
                """,
                (_new_spotify_id("conc"), f"ConcTrack_{uuid.uuid4().hex[:8]}", _new_isrc()),
            )
            track_ids.append(int(cur.fetchone()[0]))
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


def scenario_relationship_delete(conn: psycopg.Connection, samples: dict) -> tuple[float, int]:
    """
    S3 – Relationship Delete: usuń powiązanie track_artists (co-artist).
    """
    artist_id = random.choice(samples["artist_ids"])
    track_id, artist_id = _setup_track_artist_relation(conn, artist_id)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM track_artists WHERE track_id = %s AND artist_id = %s",
                (track_id, artist_id),
            )
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


def scenario_range_delete(conn: psycopg.Connection, samples: dict) -> tuple[float, int]:
    """
    S4 – Range Delete: usuń chart_entries starsze niż 3 lata.
    Dane są wstępnie inserowane (~1000 wpisów); mierzymy DELETE.
    """
    chart_id = samples["chart_id"]
    _setup_old_chart_entries(conn, chart_id, count=1000)

    cutoff = date.today() - timedelta(days=365 * 3)

    start = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM chart_entries WHERE chart_date < %s",
                (cutoff,),
            )
            affected = cur.rowcount
    elapsed = time.perf_counter() - start
    return elapsed, affected


def _concurrent_delete_worker(cfg: DbConfig, track_id: int) -> int:
    with connect_db(cfg) as worker_conn:
        with worker_conn.transaction():
            with worker_conn.cursor() as cur:
                cur.execute("DELETE FROM tracks WHERE track_id = %s", (track_id,))
                return cur.rowcount


def scenario_concurrent_delete(cfg: DbConfig, conn: psycopg.Connection, workers: int = 50) -> tuple[float, int]:
    """
    S5 – Concurrent Delete: równoległe usuwanie tracków z wielu wątków.
    Testuje mechanizmy rozwiązywania blokad (deadlocks, row-level locking).
    """
    track_ids = _setup_tracks_for_concurrent_delete(conn, count=workers)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda tid: _concurrent_delete_worker(cfg, tid), track_ids))
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
    runs_per_scenario: int,
    with_indexes: bool,
    concurrent_workers: int,
) -> list[dict]:
    results: list[dict] = []

    with connect_db(cfg) as conn:
        apply_indexes(conn, with_indexes)
        samples = fetch_sample_ids(conn)

        index_label = "with_indexes" if with_indexes else "no_indexes"

        # Scenarios return (elapsed, ops) tuples directly (setup is outside timed window)
        scenario_defs = [
            ("point_delete",        lambda: scenario_point_delete(conn, samples)),
            ("cascade_delete",      lambda: scenario_cascade_delete(conn, samples)),
            ("relationship_delete", lambda: scenario_relationship_delete(conn, samples)),
            ("range_delete",        lambda: scenario_range_delete(conn, samples)),
            ("concurrent_delete",   lambda: scenario_concurrent_delete(cfg, conn, concurrent_workers)),
            ("soft_delete",         lambda: scenario_soft_delete(conn, samples)),
        ]

        for scenario_name, scenario_fn in scenario_defs:
            for run_idx in range(1, runs_per_scenario + 1):
                elapsed, ops = scenario_fn()
                results.append(
                    {
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
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in results:
        key = (row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== DELETE Benchmark Summary (avg z prób) ===")
    print(f"{'index_mode':<15} {'scenario':<22} {'avg_sec':>10} {'avg_rows':>10} {'avg_ops/s':>12}")
    print("-" * 72)

    for (index_mode, scenario), rows in sorted(groups.items()):
        avg_sec = mean(r["seconds"] for r in rows)
        avg_rows = mean(r["rows_affected"] for r in rows)
        valid_ops = [r["ops_per_sec"] for r in rows if r["ops_per_sec"] is not None]
        avg_ops = mean(valid_ops) if valid_ops else 0.0
        print(f"{index_mode:<15} {scenario:<22} {avg_sec:>10.6f} {avg_rows:>10.1f} {avg_ops:>12.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["index_mode", "scenario", "run", "seconds", "rows_affected", "ops_per_sec"]
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
    parser.add_argument("--runs-per-scenario",   type=int, default=3)
    parser.add_argument("--concurrent-workers",  type=int, default=50)
    parser.add_argument("--output", default="results/psql_delete_benchmark_results.csv")

    parser.add_argument("--db-host",     default=os.getenv("DB_HOST",     "localhost"))
    parser.add_argument("--db-port",     type=int, default=int(os.getenv("DB_PORT", "5434")))
    parser.add_argument("--db-name",     default=os.getenv("DB_NAME",     "spotify_db"))
    parser.add_argument("--db-user",     default=os.getenv("DB_USER",     "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

    args = parser.parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    with_indexes = not args.no_indexes
    mode_label = "Z indeksami" if with_indexes else "BEZ indeksów"
    print(f"\n>>> PostgreSQL DELETE Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg=cfg,
        runs_per_scenario=args.runs_per_scenario,
        with_indexes=with_indexes,
        concurrent_workers=args.concurrent_workers,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
