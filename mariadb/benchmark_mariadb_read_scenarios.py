"""
MariaDB READ benchmark suite - 6 scenariuszy odczytu.
"""

import argparse
import os
import random
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
        "name": "idx_albums_release_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date)",
        "drop": "DROP INDEX IF EXISTS idx_albums_release_date ON albums",
        "new": False,
    },
    {
        "name": "idx_chart_entries_chart_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date ON chart_entries(chart_id, chart_date)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_chart_date ON chart_entries",
        "new": False,
    },
    {
        "name": "idx_chart_entries_track_date",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date ON chart_entries(track_id, chart_date)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_track_date ON chart_entries",
        "new": False,
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
        cur.execute("SELECT track_id FROM audio_features ORDER BY track_id LIMIT 50")
        samples["track_ids"] = [int(r[0]) for r in cur.fetchall()] or [1]

        cur.execute("SELECT album_id FROM albums ORDER BY album_id LIMIT 50")
        samples["album_ids"] = [int(r[0]) for r in cur.fetchall()] or [1]

        cur.execute("SELECT chart_id FROM charts ORDER BY chart_id LIMIT 10")
        chart_ids = [int(r[0]) for r in cur.fetchall()] or [1]
        samples["chart_ids"] = chart_ids

        pairs: list[tuple[int, object]] = []
        for chart_id in chart_ids:
            cur.execute(
                "SELECT chart_id, chart_date FROM chart_entries WHERE chart_id = %s ORDER BY chart_date DESC LIMIT 20",
                (chart_id,),
            )
            rows = cur.fetchall()
            for row in rows:
                pairs.append((int(row[0]), row[1]))
                if len(pairs) >= 20:
                    break
            if len(pairs) >= 20:
                break
        samples["chart_date_pairs"] = pairs if pairs else [(1, "2024-01-01")]

        cur.execute("SELECT artist_id FROM artists ORDER BY artist_id LIMIT 20")
        samples["artist_ids"] = [int(r[0]) for r in cur.fetchall()] or [1]

    print("[SETUP] Zaladowano sample IDs:")
    print(f"  track_ids:        {len(samples['track_ids'])} sztuk")
    print(f"  album_ids:        {len(samples['album_ids'])} sztuk")
    print(f"  chart_date_pairs: {len(samples['chart_date_pairs'])} sztuk")
    print(f"  chart_ids:        {len(samples['chart_ids'])} sztuk")
    print(f"  artist_ids:       {len(samples['artist_ids'])} sztuk")

    return samples


def scenario_point_read(conn, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, danceability, energy, `key`, mode,
                   loudness, speechiness, acousticness, instrumentalness,
                   liveness, valence, tempo, time_signature
            FROM audio_features
            WHERE track_id = %s
            """,
            (track_id,),
        )
        rows = cur.fetchall()
    return len(rows)


def scenario_partition_read(conn, samples: dict) -> int:
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
    return len(rows)


def scenario_top_n_ranking_scaled(conn, samples: dict, scale: int) -> int:
    chart_id = random.choice(samples["chart_ids"])
    limit_n = scaled_count(scale, 0.00005, min_count=50, max_count=10_000)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ce.chart_entry_id, ce.track_id, ce.position, ce.streams, t.name
            FROM chart_entries ce
            JOIN tracks t ON t.track_id = ce.track_id
            WHERE ce.chart_id = %s
            ORDER BY ce.chart_date DESC, ce.position
            LIMIT %s
            """,
            (chart_id, limit_n),
        )
        rows = cur.fetchall()
    return len(rows)


def scenario_secondary_index_read_scaled(conn, scale: int) -> int:
    limit_n = scaled_count(scale, 0.001, min_count=1_000, max_count=50_000)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, name, duration_min
            FROM tracks
            WHERE explicit = true
            LIMIT %s
            """,
            (limit_n,),
        )
        rows = cur.fetchall()
    return len(rows)


def scenario_local_aggregation(conn, samples: dict) -> int:
    artist_id = random.choice(samples["artist_ids"])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS track_count,
                AVG(af.tempo) AS avg_tempo,
                AVG(af.danceability) AS avg_danceability,
                AVG(af.energy) AS avg_energy
            FROM track_artists ta
            JOIN audio_features af ON af.track_id = ta.track_id
            WHERE ta.artist_id = %s
            """,
            (artist_id,),
        )
        _ = cur.fetchall()
    return 1


def scenario_range_query_scaled(conn, scale: int) -> int:
    limit_n = scaled_count(scale, 0.0002, min_count=2_000, max_count=100_000)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT album_id, name, release_date, total_tracks
            FROM albums
            WHERE release_date BETWEEN '2015-01-01' AND '2020-12-31'
            ORDER BY release_date
            LIMIT %s
            """,
            (limit_n,),
        )
        rows = cur.fetchall()
    return len(rows)


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
        if not skip_prepare:
            prepare_scale_data_with_seed_script(
                cfg=cfg,
                target_rows=scale,
                seed_value=seed_value,
                pool_size=pool_size,
            )

        with connect_db(cfg) as conn:
            ensure_existing_schema(conn)
            samples = fetch_sample_ids(conn)

            for with_indexes in index_modes:
                apply_indexes(conn, MANAGED_INDEXES, with_indexes)
                index_label = "with_indexes" if with_indexes else "no_indexes"

                scenarios = [
                    ("point_read", lambda: scenario_point_read(conn, samples)),
                    ("partition_read", lambda: scenario_partition_read(conn, samples)),
                    ("top_n_ranking", lambda: scenario_top_n_ranking_scaled(conn, samples, scale=scale)),
                    ("secondary_index_read", lambda: scenario_secondary_index_read_scaled(conn, scale=scale)),
                    ("local_aggregation", lambda: scenario_local_aggregation(conn, samples)),
                    ("range_query", lambda: scenario_range_query_scaled(conn, scale=scale)),
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
                                "rows_returned": ops,
                                "rows_per_sec": (ops / elapsed) if elapsed > 0 else None,
                            }
                        )

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row.get("scale", 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== READ Benchmark Summary (avg z prob) ===")
    print(f"{'scale':>10} {'index_mode':<15} {'scenario':<25} {'avg_sec':>10} {'avg_rows/s':>12}")
    print("-" * 80)

    for (scale, index_mode, scenario), rows in sorted(groups.items()):
        avg_sec = mean(r["seconds"] for r in rows)
        valid_rps = [r["rows_per_sec"] for r in rows if r["rows_per_sec"] is not None]
        avg_rps = mean(valid_rps) if valid_rps else 0.0
        print(f"{scale:>10,} {index_mode:<15} {scenario:<25} {avg_sec:>10.6f} {avg_rps:>12.2f}")


def save_results_csv(results: list[dict], out_path: str) -> None:
    import csv

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["scale", "index_mode", "scenario", "run", "seconds", "rows_returned", "rows_per_sec"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MariaDB READ benchmark - 6 scenariuszy odczytu z porownaniem przed/po indeksach."
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
    parser.add_argument("--output", default="mariadb/results/mariadb_read_benchmark_results.csv")

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

    print(f"\n>>> MariaDB READ Benchmark - tryb: {mode_label} <<<")

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
