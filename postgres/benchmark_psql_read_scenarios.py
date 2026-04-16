"""
PostgreSQL READ benchmark suite – 6 scenariuszy odczytu.

Scenariusze:
  1. point_read            – audio_features po track_id (PK lookup)
  2. partition_read        – wszystkie tracki albumu (JOIN track_albums)
  3. top_n_ranking         – Top-50 chart_entries dla chart + date
  4. secondary_index_read  – tracki z explicit=true (wymaga indeksu)
  5. local_aggregation     – avg(tempo, danceability) per artysta
  6. range_query           – albumy z release_date 2015-2020

Uruchomienie (z katalogu postgres/):
  python benchmark_psql_read_scenarios.py --scales 500000,1000000 --no-indexes
  python benchmark_psql_read_scenarios.py --scales 500000,1000000
"""

import argparse
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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

# Indexes managed by this benchmark (created/dropped to compare before vs after).
# Indexes that exist in the base schema are included so they can also be removed.
MANAGED_INDEXES = [
    {
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date)",
        "drop":   "DROP INDEX IF EXISTS idx_albums_release_date",
        "new": False,
    },
    {
        "name": "idx_chart_entries_chart_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date ON chart_entries(chart_id, chart_date)",
        "drop":   "DROP INDEX IF EXISTS idx_chart_entries_chart_date",
        "new": False,
    },
    {
        "name": "idx_chart_entries_track_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date ON chart_entries(track_id, chart_date)",
        "drop":   "DROP INDEX IF EXISTS idx_chart_entries_track_date",
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
    {
        "name": "idx_tracks_explicit",
        "create": "CREATE INDEX IF NOT EXISTS idx_tracks_explicit ON tracks(explicit) WHERE explicit = true",
        "drop":   "DROP INDEX IF EXISTS idx_tracks_explicit",
        "new": True,
    },
]


def apply_indexes(conn: psycopg.Connection, with_indexes: bool) -> None:
    """Create or drop all managed indexes depending on the with_indexes flag."""
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
# SAMPLE ID FETCHING
# ---------------------------------------------------------------------------

def fetch_sample_ids(conn: psycopg.Connection) -> dict:
    """Fetch random existing IDs needed by the read scenarios."""
    samples: dict = {}
    with conn.cursor() as cur:
        # random track with audio_features
        cur.execute(
            """
            SELECT af.track_id
            FROM audio_features af
            ORDER BY random()
            LIMIT 20
            """
        )
        rows = cur.fetchall()
        samples["track_ids"] = [r[0] for r in rows] if rows else [1]

        # random album that has tracks
        cur.execute(
            """
            SELECT ta.album_id, COUNT(*) as cnt
            FROM track_albums ta
            GROUP BY ta.album_id
            HAVING COUNT(*) > 0
            ORDER BY random()
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        samples["album_ids"] = [r[0] for r in rows] if rows else [1]

        # random chart + date combo with at least 50 entries
        cur.execute(
            """
            SELECT chart_id, chart_date
            FROM chart_entries
            GROUP BY chart_id, chart_date
            HAVING COUNT(*) >= 50
            ORDER BY random()
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        samples["chart_date_pairs"] = [(r[0], r[1]) for r in rows] if rows else [(1, "2024-01-01")]

        # random artist with audio_features data
        cur.execute(
            """
            SELECT ta.artist_id
            FROM track_artists ta
            JOIN audio_features af ON af.track_id = ta.track_id
            GROUP BY ta.artist_id
            HAVING COUNT(*) >= 5
            ORDER BY random()
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        samples["artist_ids"] = [r[0] for r in rows] if rows else [1]

    conn.commit()
    print(f"[SETUP] Załadowano sample IDs:")
    print(f"  track_ids:        {len(samples['track_ids'])} sztuk")
    print(f"  album_ids:        {len(samples['album_ids'])} sztuk")
    print(f"  chart_date_pairs: {len(samples['chart_date_pairs'])} sztuk")
    print(f"  artist_ids:       {len(samples['artist_ids'])} sztuk")
    return samples


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------

def scenario_point_read(conn: psycopg.Connection, samples: dict) -> int:
    """
    S1 – Point Read: pobierz audio_features dla losowego track_id.
    Testuje czysty PK lookup (latency).
    """
    track_id = random.choice(samples["track_ids"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, danceability, energy, key, mode,
                   loudness, speechiness, acousticness, instrumentalness,
                   liveness, valence, tempo, time_signature
            FROM audio_features
            WHERE track_id = %s
            """,
            (track_id,),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


def scenario_partition_read(conn: psycopg.Connection, samples: dict) -> int:
    """
    S2 – Partition Read: wszystkie tracki dla konkretnego albumu.
    Wymaga JOIN przez track_albums; korzysta z idx_track_albums_album_id.
    """
    album_id = random.choice(samples["album_ids"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.track_id, t.name, t.explicit, t.duration_min,
                   t.disc_number, t.track_number
            FROM tracks t
            JOIN track_albums ta ON t.track_id = ta.track_id
            WHERE ta.album_id = %s
            """,
            (album_id,),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


def scenario_top_n_ranking(conn: psycopg.Connection, samples: dict) -> int:
    """
    S3 – Top-N Ranking: Top 50 notowań dla wybranego chart + date.
    Korzysta z idx_chart_entries_chart_date.
    """
    chart_id, chart_date = random.choice(samples["chart_date_pairs"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ce.chart_entry_id, ce.track_id, ce.position, ce.streams,
                   t.name
            FROM chart_entries ce
            JOIN tracks t ON t.track_id = ce.track_id
            WHERE ce.chart_id = %s
              AND ce.chart_date = %s
            ORDER BY ce.position
            LIMIT 50
            """,
            (chart_id, chart_date),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


def scenario_secondary_index_read(conn: psycopg.Connection, _samples: dict) -> int:
    """
    S4 – Secondary Index Read: znajdź 1000 utworów z explicit=true.
    Bez indeksu: seq scan. Z idx_tracks_explicit: index scan.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, name, duration_min
            FROM tracks
            WHERE explicit = true
            LIMIT 1000
            """,
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


def scenario_local_aggregation(conn: psycopg.Connection, samples: dict) -> int:
    """
    S5 – Local Aggregation: średnie tempo i danceability dla artysty.
    Wymaga JOIN track_artists -> audio_features; korzysta z idx_track_artists_artist_id.
    """
    artist_id = random.choice(samples["artist_ids"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS track_count,
                AVG(af.tempo)        AS avg_tempo,
                AVG(af.danceability) AS avg_danceability,
                AVG(af.energy)       AS avg_energy
            FROM track_artists ta
            JOIN audio_features af ON af.track_id = ta.track_id
            WHERE ta.artist_id = %s
            """,
            (artist_id,),
        )
        rows = cur.fetchall()
    conn.commit()
    return 1  # single aggregate row


def scenario_range_query(conn: psycopg.Connection, _samples: dict) -> int:
    """
    S6 – Range Query: albumy wydane w latach 2015–2020.
    Korzysta z idx_albums_release_date.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT album_id, name, release_date, total_tracks
            FROM albums
            WHERE release_date BETWEEN '2015-01-01' AND '2020-12-31'
            ORDER BY release_date
            """,
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------

def timed_run(fn, *args, **kwargs) -> tuple[float, int]:
    start = time.perf_counter()
    ops = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, ops


def run_benchmark(
    cfg: DbConfig,
    runs_per_scenario: int,
    with_indexes: bool,
) -> list[dict]:
    results: list[dict] = []

    with connect_db(cfg) as conn:
        apply_indexes(conn, with_indexes)
        samples = fetch_sample_ids(conn)

        index_label = "with_indexes" if with_indexes else "no_indexes"

        scenarios = [
            ("point_read",            lambda: scenario_point_read(conn, samples)),
            ("partition_read",        lambda: scenario_partition_read(conn, samples)),
            ("top_n_ranking",         lambda: scenario_top_n_ranking(conn, samples)),
            ("secondary_index_read",  lambda: scenario_secondary_index_read(conn, samples)),
            ("local_aggregation",     lambda: scenario_local_aggregation(conn, samples)),
            ("range_query",           lambda: scenario_range_query(conn, samples)),
        ]

        for scenario_name, scenario_fn in scenarios:
            for run_idx in range(1, runs_per_scenario + 1):
                elapsed, ops = timed_run(scenario_fn)
                results.append(
                    {
                        "index_mode": index_label,
                        "scenario": scenario_name,
                        "run": run_idx,
                        "seconds": elapsed,
                        "rows_returned": ops,
                        "rows_per_sec": (ops / elapsed) if elapsed > 0 else None,
                    }
                )

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in results:
        key = (row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== READ Benchmark Summary (avg z prób) ===")
    print(f"{'index_mode':<15} {'scenario':<25} {'avg_sec':>10} {'avg_rows/s':>12}")
    print("-" * 65)

    for (index_mode, scenario), rows in sorted(groups.items()):
        avg_sec = mean(r["seconds"] for r in rows)
        valid_rps = [r["rows_per_sec"] for r in rows if r["rows_per_sec"] is not None]
        avg_rps = mean(valid_rps) if valid_rps else 0.0
        print(f"{index_mode:<15} {scenario:<25} {avg_sec:>10.6f} {avg_rps:>12.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["index_mode", "scenario", "run", "seconds", "rows_returned", "rows_per_sec"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="PostgreSQL READ benchmark – 6 scenariuszy odczytu z porównaniem przed/po indeksach."
    )

    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Usuwa zarządzane indeksy przed testem (tryb 'bez indeksów').",
    )
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--output", default="results/psql_read_benchmark_results.csv")

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
    print(f"\n>>> PostgreSQL READ Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg=cfg,
        runs_per_scenario=args.runs_per_scenario,
        with_indexes=with_indexes,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
