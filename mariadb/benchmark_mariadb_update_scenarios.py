"""
MariaDB UPDATE benchmark suite - 6 scenariuszy aktualizacji.
"""

import argparse
import os
import random
import uuid
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
        "name": "idx_chart_entries_track_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date ON chart_entries(track_id, chart_date)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_track_date ON chart_entries",
        "new": False,
    },
    {
        "name": "idx_artist_genres_genre_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_artist_genres_genre_id ON artist_genres(genre_id)",
        "drop": "DROP INDEX IF EXISTS idx_artist_genres_genre_id ON artist_genres",
        "new": True,
    },
    {
        "name": "idx_track_artists_artist_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id)",
        "drop": "DROP INDEX IF EXISTS idx_track_artists_artist_id ON track_artists",
        "new": True,
    },
    {
        "name": "idx_tracks_explicit",
        "create": "CREATE INDEX IF NOT EXISTS idx_tracks_explicit ON tracks(explicit)",
        "drop": "DROP INDEX IF EXISTS idx_tracks_explicit ON tracks",
        "new": True,
    },
]


def fetch_sample_ids(conn) -> dict:
    samples: dict = {}

    with conn.cursor() as cur:
        cur.execute("SELECT track_id FROM audio_features ORDER BY track_id LIMIT 500")
        track_ids = [int(r[0]) for r in cur.fetchall()]
        samples["track_ids"] = track_ids if track_ids else [1]

        cur.execute("SELECT MAX(track_id) FROM tracks")
        samples["max_track_id"] = int(cur.fetchone()[0] or 1)

        cur.execute(
            """
            SELECT chart_entry_id, position, streams
            FROM chart_entries
            WHERE streams IS NOT NULL
            ORDER BY chart_entry_id
            LIMIT 200
            """
        )
        rows = cur.fetchall()
        samples["chart_entries"] = [(int(r[0]), int(r[1]), int(r[2] or 0)) for r in rows] if rows else [(1, 5, 1000)]

        cur.execute("SELECT DISTINCT track_id FROM chart_entries ORDER BY chart_entry_id LIMIT 200")
        rows = cur.fetchall()
        samples["chart_track_ids"] = [int(r[0]) for r in rows] if rows else [1]

        cur.execute("SELECT DISTINCT artist_id FROM artist_genres ORDER BY artist_id LIMIT 100")
        rows = cur.fetchall()
        samples["artist_ids_with_genres"] = [int(r[0]) for r in rows] if rows else [1]

        cur.execute("SELECT genre_id FROM genres ORDER BY genre_id")
        rows = cur.fetchall()
        samples["genre_ids"] = [int(r[0]) for r in rows] if rows else [1]

        cur.execute(
            """
            SELECT ag.genre_id, COUNT(DISTINCT ta.track_id) AS cnt
            FROM artist_genres ag
            JOIN track_artists ta ON ta.artist_id = ag.artist_id
            GROUP BY ag.genre_id
            ORDER BY cnt DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        samples["bulk_genre_ids"] = [int(r[0]) for r in rows] if rows else [1]

    print("[SETUP] Zaladowano sample IDs:")
    print(f"  track_ids (z audio_features): {len(samples['track_ids'])}")
    print(f"  max_track_id:                 {samples['max_track_id']}")
    print(f"  chart_entries:                {len(samples['chart_entries'])}")
    print(f"  chart_track_ids:              {len(samples['chart_track_ids'])}")
    print(f"  artist_ids_with_genres:       {len(samples['artist_ids_with_genres'])}")
    print(f"  genre_ids:                    {len(samples['genre_ids'])}")
    print(f"  bulk_genre_ids:               {len(samples['bulk_genre_ids'])}")

    return samples


def scenario_point_update_scaled(conn, samples: dict, scale: int) -> int:
    n = scaled_count(scale, 0.0001, min_count=200, max_count=20_000)
    max_track_id = int(samples["max_track_id"])
    track_ids = [random.randint(1, max_track_id) for _ in range(n)]

    with conn.cursor() as cur:
        fmt = ",".join(["%s"] * len(track_ids))
        cur.execute(
            f"""
            UPDATE tracks
            SET name = CONCAT('Batch Updated ', LEFT(MD5(RAND()), 8)),
                updated_at = NOW()
            WHERE track_id IN ({fmt})
            """,
            track_ids,
        )
        affected = int(cur.rowcount)
    conn.commit()
    return affected


def scenario_nested_update(conn, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    new_energy = round(random.uniform(0.01, 0.99), 3)
    with conn.cursor() as cur:
        cur.execute("UPDATE audio_features SET energy = %s WHERE track_id = %s", (new_energy, track_id))
        affected = int(cur.rowcount)
    conn.commit()
    return affected


def scenario_bulk_update_scaled(conn, samples: dict, scale: int) -> int:
    genre_id = random.choice(samples["bulk_genre_ids"])
    limit_n = scaled_count(scale, 0.001, min_count=2_000, max_count=200_000)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tracks
            SET explicit = NOT COALESCE(explicit, 0),
                updated_at = NOW()
            WHERE track_id IN (
                SELECT x.track_id
                FROM (
                    SELECT DISTINCT ta.track_id
                    FROM track_artists ta
                    JOIN artist_genres ag ON ag.artist_id = ta.artist_id
                    WHERE ag.genre_id = %s
                    LIMIT %s
                ) x
            )
            """,
            (genre_id, limit_n),
        )
        affected = int(cur.rowcount)
    conn.commit()
    return affected


def scenario_atomic_increment_scaled(conn, samples: dict, scale: int) -> int:
    track_id = random.choice(samples.get("chart_track_ids", [1]))
    limit_n = scaled_count(scale, 0.0002, min_count=500, max_count=50_000)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE chart_entries
            SET streams = COALESCE(streams, 0) + 1000
            WHERE track_id = %s
            ORDER BY chart_date DESC
            LIMIT %s
            """,
            (track_id, limit_n),
        )
        affected = int(cur.rowcount)
    conn.commit()
    return affected


def scenario_list_append(conn, samples: dict) -> int:
    artist_id = random.choice(samples["artist_ids_with_genres"])
    genre_id = random.choice(samples["genre_ids"])
    with conn.cursor() as cur:
        cur.execute(
            "INSERT IGNORE INTO artist_genres (artist_id, genre_id) VALUES (%s, %s)",
            (artist_id, genre_id),
        )
    conn.commit()
    return 1


def scenario_cas_update(conn, samples: dict) -> int:
    chart_entry_id, current_position, _streams = random.choice(samples["chart_entries"])
    new_position = max(1, current_position + random.randint(-10, 10))
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
        affected = int(cur.rowcount)
    conn.commit()
    return affected


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
                samples = fetch_sample_ids(conn)

                scenarios = [
                    ("point_update", lambda: scenario_point_update_scaled(conn, samples, scale=scale)),
                    ("nested_update", lambda: scenario_nested_update(conn, samples)),
                    ("bulk_update", lambda: scenario_bulk_update_scaled(conn, samples, scale=scale)),
                    ("atomic_increment", lambda: scenario_atomic_increment_scaled(conn, samples, scale=scale)),
                    ("list_append", lambda: scenario_list_append(conn, samples)),
                    ("cas_update", lambda: scenario_cas_update(conn, samples)),
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

    print("\n=== UPDATE Benchmark Summary (avg z prob) ===")
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MariaDB UPDATE benchmark - 6 scenariuszy aktualizacji z porownaniem przed/po indeksach."
    )

    parser.add_argument("--scales", default="500000,1000000,10000000")
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
    parser.add_argument("--runs-per-scenario", type=int, default=3)
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie seeduje danych do skali przed testami (szybszy dry-run, wymaga recznego przygotowania danych).",
    )
    parser.add_argument("--output", default="mariadb/results/mariadb_update_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3307")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))

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
        mode_label = "BEZ indeksow + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksow"

    print(f"\n>>> MariaDB UPDATE Benchmark - tryb: {mode_label} <<<")

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
