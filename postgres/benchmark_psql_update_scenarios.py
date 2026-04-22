"""
PostgreSQL UPDATE benchmark suite – 6 scenariuszy aktualizacji.

Scenariusze:
  1. point_update        – zmiana nazwy tracka po track_id
  2. nested_update       – zmiana energy w audio_features dla tracka
  3. bulk_update         – explicit=true dla wszystkich tracków z gatunku
  4. atomic_increment    – streams += 1000 w chart_entries (atomowo)
  5. list_append         – dodanie gatunku do artist_genres artysty
  6. cas_update          – zmiana position TYLKO gdy nowa jest lepsza (niższa)

Uruchomienie (z katalogu postgres/):
    python benchmark_psql_update_scenarios.py --scales 500000,1000000,10000000 --both-index-modes
    python benchmark_psql_update_scenarios.py --scales 1000000 --no-indexes
"""

import argparse
import os
import random
import sys
import time
import uuid
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


# ---------------------------------------------------------------------------
# INDEX MANAGEMENT
# ---------------------------------------------------------------------------

MANAGED_INDEXES = [
    {
        "name": "idx_chart_entries_track_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date ON chart_entries(track_id, chart_date)",
        "drop":   "DROP INDEX IF EXISTS idx_chart_entries_track_date",
        "new": False,
    },
    {
        "name": "idx_artist_genres_artist_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_artist_genres_artist_id ON artist_genres(artist_id)",
        "drop":   "DROP INDEX IF EXISTS idx_artist_genres_artist_id",
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
# SCALE / SEEDING
# ---------------------------------------------------------------------------

def parse_scales(scales_str: str) -> list[int]:
    parts = [p.strip() for p in scales_str.split(",") if p.strip()]
    scales: list[int] = []
    for p in parts:
        val = int(p)
        if val <= 0:
            raise ValueError("All scales must be positive")
        scales.append(val)
    if not scales:
        raise ValueError("No scales provided")
    return scales


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
# SAMPLE ID FETCHING
# ---------------------------------------------------------------------------

def fetch_sample_ids(conn: psycopg.Connection) -> dict:
    """Fetch random existing IDs needed by update scenarios."""
    samples: dict = {}
    with conn.cursor() as cur:
        # tracks with audio_features
        cur.execute(
            """
            SELECT af.track_id
            FROM audio_features af
            ORDER BY random()
            LIMIT 50
            """
        )
        rows = cur.fetchall()
        samples["track_ids"] = [r[0] for r in rows] if rows else [1]

        # max track id (for fast random sampling without ORDER BY random())
        cur.execute("SELECT COALESCE(MAX(track_id), 1) FROM tracks")
        samples["max_track_id"] = int(cur.fetchone()[0] or 1)

        # chart_entries to atomically increment
        cur.execute(
            """
            SELECT chart_entry_id, position, streams
            FROM chart_entries
            WHERE streams IS NOT NULL
            ORDER BY random()
            LIMIT 50
            """
        )
        rows = cur.fetchall()
        samples["chart_entries"] = [(r[0], r[1], r[2]) for r in rows] if rows else [(1, 5, 1000)]

        # track_ids with many chart entries (to make scaled atomic increments meaningful)
        cur.execute(
            """
            SELECT track_id, COUNT(*) AS cnt
            FROM chart_entries
            GROUP BY track_id
            HAVING COUNT(*) >= 200
            ORDER BY random()
            LIMIT 20
            """
        )
        rows = cur.fetchall()
        samples["chart_track_ids"] = [r[0] for r in rows] if rows else [1]

        # artists with existing genres (for list_append – we need one more genre to add)
        cur.execute(
            """
            SELECT artist_id
            FROM (SELECT DISTINCT artist_id FROM artist_genres) sub
            ORDER BY random()
            LIMIT 20
            """
        )
        rows = cur.fetchall()
        samples["artist_ids_with_genres"] = [r[0] for r in rows] if rows else [1]

        # all genre ids
        cur.execute("SELECT genre_id FROM genres")
        rows = cur.fetchall()
        samples["genre_ids"] = [r[0] for r in rows] if rows else [1]

        # genre with many tracks (for bulk_update)
        cur.execute(
            """
            SELECT ag.genre_id, COUNT(DISTINCT ta.track_id) as cnt
            FROM artist_genres ag
            JOIN track_artists ta ON ta.artist_id = ag.artist_id
            GROUP BY ag.genre_id
            ORDER BY cnt DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        samples["bulk_genre_ids"] = [r[0] for r in rows] if rows else [1]

    conn.commit()
    print(f"[SETUP] Załadowano sample IDs:")
    print(f"  track_ids (z audio_features): {len(samples['track_ids'])}")
    print(f"  max_track_id:                {samples['max_track_id']}")
    print(f"  chart_entries:                {len(samples['chart_entries'])}")
    print(f"  chart_track_ids:              {len(samples['chart_track_ids'])}")
    print(f"  artist_ids_with_genres:       {len(samples['artist_ids_with_genres'])}")
    print(f"  genre_ids:                    {len(samples['genre_ids'])}")
    print(f"  bulk_genre_ids:               {len(samples['bulk_genre_ids'])}")
    return samples


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------

def scenario_point_update(conn: psycopg.Connection, samples: dict) -> int:
    """
    S1 – Point Update: korekta nazwy tracka po track_id.
    Testuje UPDATE z PK lookup – najszybsza operacja.
    """
    track_id = random.choice(samples["track_ids"])
    new_name = f"Updated Track {uuid.uuid4().hex[:8]}"
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tracks SET name = %s, updated_at = now() WHERE track_id = %s",
                (new_name, track_id),
            )
            return cur.rowcount


def scenario_point_update_scaled(conn: psycopg.Connection, samples: dict, scale: int) -> int:
    """
    S1 (scaled) – Batch Point Update: aktualizuj N losowych tracków po PK.
    N rośnie wraz z scale, ale to wciąż update po PK (ważne dla porównań).
    """
    max_track_id = int(samples.get("max_track_id", 1))
    n = _scaled_count(scale, 0.0001, min_count=200, max_count=20_000)
    track_ids = [random.randint(1, max_track_id) for _ in range(n)]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tracks
                SET
                    name = 'Batch Updated ' || left(md5(random()::text || track_id::text), 8),
                    updated_at = now()
                WHERE track_id = ANY(%s)
                """,
                (track_ids,),
            )
            return cur.rowcount


def scenario_nested_update(conn: psycopg.Connection, samples: dict) -> int:
    """
    S2 – Nested Update: zmiana energy w tabeli audio_features.
    W SQL: UPDATE powiązanej tabeli (1:1). Testuje write na JOIN-owanej strukturze.
    """
    track_id = random.choice(samples["track_ids"])
    new_energy = round(random.uniform(0.01, 0.99), 3)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE audio_features SET energy = %s WHERE track_id = %s",
                (new_energy, track_id),
            )
            return cur.rowcount


def scenario_bulk_update(conn: psycopg.Connection, samples: dict) -> int:
    """
    S3 – Bulk Update: explicit=true dla wszystkich tracków z wybranego gatunku.
    Testuje UPDATE z podzapytaniem – liczy zmodyfikowane wiersze.
    """
    genre_id = random.choice(samples["bulk_genre_ids"])
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tracks
                SET explicit = true, updated_at = now()
                WHERE track_id IN (
                    SELECT DISTINCT ta.track_id
                    FROM track_artists ta
                    JOIN artist_genres ag ON ag.artist_id = ta.artist_id
                    WHERE ag.genre_id = %s
                )
                """,
                (genre_id,),
            )
            return cur.rowcount


def scenario_bulk_update_scaled(conn: psycopg.Connection, samples: dict, scale: int) -> int:
    """
    S3 (scaled) – Bulk Update: przełącz explicit dla N tracków z wybranego gatunku.
    N rośnie wraz z scale, ale jest ograniczane LIMIT, żeby nie robić full-table update.
    """
    genre_id = random.choice(samples["bulk_genre_ids"])
    limit_n = _scaled_count(scale, 0.001, min_count=2_000, max_count=200_000)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH target_tracks AS (
                    SELECT DISTINCT ta.track_id
                    FROM track_artists ta
                    JOIN artist_genres ag ON ag.artist_id = ta.artist_id
                    WHERE ag.genre_id = %s
                    LIMIT %s
                )
                UPDATE tracks t
                SET explicit = NOT t.explicit,
                    updated_at = now()
                FROM target_tracks tt
                WHERE t.track_id = tt.track_id
                """,
                (genre_id, limit_n),
            )
            return cur.rowcount


def scenario_atomic_increment(conn: psycopg.Connection, samples: dict) -> int:
    """
    S4 – Atomic Increment: streams += 1000 dla wybranego chart_entry.
    Testuje atomową inkrementację licznika (brak wyścigu z innymi sesjami).
    """
    chart_entry_id, _pos, _streams = random.choice(samples["chart_entries"])
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE chart_entries
                SET streams = COALESCE(streams, 0) + 1000
                WHERE chart_entry_id = %s
                """,
                (chart_entry_id,),
            )
            return cur.rowcount


def scenario_atomic_increment_scaled(conn: psycopg.Connection, samples: dict, scale: int) -> int:
    """
    S4 (scaled) – Atomic Increment: inkrementuj streams dla N wpisów chart_entries
    dla jednego track_id (ORDER BY chart_date), żeby używać idx_chart_entries_track_date.
    """
    track_id = random.choice(samples.get("chart_track_ids", [1]))
    limit_n = _scaled_count(scale, 0.0002, min_count=500, max_count=50_000)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH target AS (
                    SELECT chart_entry_id
                    FROM chart_entries
                    WHERE track_id = %s
                    ORDER BY chart_date DESC
                    LIMIT %s
                )
                UPDATE chart_entries ce
                SET streams = COALESCE(ce.streams, 0) + 1000
                FROM target t
                WHERE ce.chart_entry_id = t.chart_entry_id
                """,
                (track_id, limit_n),
            )
            return cur.rowcount


def scenario_list_append(conn: psycopg.Connection, samples: dict) -> int:
    """
    S5 – List Append: przypisanie nowego gatunku do artysty (INSERT INTO artist_genres).
    Symuluje 'dopisanie do kolekcji' – w NoSQL to append do tablicy w dokumencie.
    ON CONFLICT DO NOTHING zapobiega duplikatom.
    """
    artist_id = random.choice(samples["artist_ids_with_genres"])
    genre_id = random.choice(samples["genre_ids"])
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artist_genres (artist_id, genre_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (artist_id, genre_id),
            )
            return 1


def scenario_cas_update(conn: psycopg.Connection, samples: dict) -> int:
    """
    S6 – CAS Update (Compare-And-Set): zmień position TYLKO gdy nowa jest niższa.
    Zapobiega regresji pozycji (race condition). W SQL: WHERE position > new_position.
    """
    chart_entry_id, current_position, _streams = random.choice(samples["chart_entries"])
    # Nowa pozycja jest losowo lepsza (niższa) lub gorsza (wyższa)
    new_position = max(1, current_position + random.randint(-10, 10))
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE chart_entries
                SET position = %s
                WHERE chart_entry_id = %s
                  AND position > %s
                """,
                (new_position, chart_entry_id, new_position),
            )
            return cur.rowcount  # 0 jeśli warunek nie spełniony (brak wyścigu)


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
    scales: list[int],
    runs_per_scenario: int,
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

            # Rebuild dataset per mode to avoid cumulative UPDATE effects skewing comparisons.
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
                samples = fetch_sample_ids(conn)

                scenarios = [
                    ("point_update",     lambda: scenario_point_update_scaled(conn, samples, scale=scale)),
                    ("nested_update",    lambda: scenario_nested_update(conn, samples)),
                    ("bulk_update",      lambda: scenario_bulk_update_scaled(conn, samples, scale=scale)),
                    ("atomic_increment", lambda: scenario_atomic_increment_scaled(conn, samples, scale=scale)),
                    ("list_append",      lambda: scenario_list_append(conn, samples)),
                    ("cas_update",       lambda: scenario_cas_update(conn, samples)),
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
                                "rows_affected": ops,
                                "ops_per_sec": (ops / elapsed) if elapsed > 0 and ops > 0 else None,
                            }
                        )

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row.get("scale", 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== UPDATE Benchmark Summary (avg z prób) ===")
    print(f"{'scale':>10} {'index_mode':<15} {'scenario':<20} {'avg_sec':>10} {'avg_rows':>10} {'avg_ops/s':>12}")
    print("-" * 90)

    for (scale, index_mode, scenario), rows in sorted(groups.items()):
        avg_sec = mean(r["seconds"] for r in rows)
        avg_rows = mean(r["rows_affected"] for r in rows)
        valid_ops = [r["ops_per_sec"] for r in rows if r["ops_per_sec"] is not None]
        avg_ops = mean(valid_ops) if valid_ops else 0.0
        print(f"{scale:>10,} {index_mode:<15} {scenario:<20} {avg_sec:>10.6f} {avg_rows:>10.1f} {avg_ops:>12.2f}")


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
        description="PostgreSQL UPDATE benchmark – 6 scenariuszy aktualizacji z porównaniem przed/po indeksach."
    )

    parser.add_argument("--scales", default="500000,1000000,10000000")
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
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie seeduje danych do skali przed testami (szybszy dry-run, wymaga ręcznego przygotowania danych).",
    )
    parser.add_argument("--output", default="postgres/results/psql_update_benchmark_results.csv")

    parser.add_argument("--db-host",     default=os.getenv("DB_HOST",     "localhost"))
    parser.add_argument("--db-port",     type=int, default=int(os.getenv("DB_PORT", "5434")))
    parser.add_argument("--db-name",     default=os.getenv("DB_NAME",     "spotify_db"))
    parser.add_argument("--db-user",     default=os.getenv("DB_USER",     "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

    args = parser.parse_args()

    if args.runs_per_scenario <= 0:
        raise ValueError("runs-per-scenario must be > 0")
    if args.pool_size <= 0:
        raise ValueError("pool-size must be > 0")

    scales = parse_scales(args.scales)

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

    print(f"\n>>> PostgreSQL UPDATE Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg=cfg,
        scales=scales,
        runs_per_scenario=args.runs_per_scenario,
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
