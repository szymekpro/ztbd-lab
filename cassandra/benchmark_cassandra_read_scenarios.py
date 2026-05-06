"""
Cassandra READ benchmark suite - 6 scenariuszy odczytu.
"""

import argparse
import os
import random
from decimal import Decimal
from statistics import mean
from typing import Optional

from benchmark_cassandra_common import (
    DbConfig,
    apply_indexes,
    close_db,
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
        "create": "CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums (release_date)",
        "drop": "DROP INDEX IF EXISTS idx_albums_release_date",
        "new": False,
    },
    {
        "name": "idx_chart_entries_track_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_id ON chart_entries (track_id)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_track_id",
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
    {
        "name": "idx_tracks_explicit",
        "create": "CREATE INDEX IF NOT EXISTS idx_tracks_explicit ON tracks (explicit)",
        "drop": "DROP INDEX IF EXISTS idx_tracks_explicit",
        "new": True,
    },
]


def fetch_sample_ids(session) -> dict:
    samples: dict = {}

    track_ids = [int(r.track_id) for r in session.execute("SELECT track_id FROM audio_features LIMIT 200")]
    album_ids = [int(r.album_id) for r in session.execute("SELECT album_id FROM albums LIMIT 200")]
    chart_ids = [int(r.chart_id) for r in session.execute("SELECT chart_id FROM charts LIMIT 50")]
    artist_ids = [int(r.artist_id) for r in session.execute("SELECT artist_id FROM artists LIMIT 100")]

    chart_date_pairs: list[tuple[int, object]] = []
    for chart_id in chart_ids[:10]:
        rows = list(
            session.execute(
                "SELECT chart_date FROM chart_entries WHERE chart_id = %s LIMIT 20",
                (chart_id,),
            )
        )
        for row in rows:
            chart_date_pairs.append((chart_id, row.chart_date))
            if len(chart_date_pairs) >= 20:
                break
        if len(chart_date_pairs) >= 20:
            break

    samples["track_ids"] = track_ids if track_ids else [1]
    samples["album_ids"] = album_ids if album_ids else [1]
    samples["chart_ids"] = chart_ids if chart_ids else [1]
    samples["artist_ids"] = artist_ids if artist_ids else [1]
    samples["chart_date_pairs"] = chart_date_pairs if chart_date_pairs else [(1, None)]

    print("[SETUP] Zaladowano sample IDs:")
    print(f"  track_ids:        {len(samples['track_ids'])} sztuk")
    print(f"  album_ids:        {len(samples['album_ids'])} sztuk")
    print(f"  chart_ids:        {len(samples['chart_ids'])} sztuk")
    print(f"  artist_ids:       {len(samples['artist_ids'])} sztuk")

    return samples


def scenario_point_read(session, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    rows = list(
        session.execute(
            """
            SELECT track_id, danceability, energy, key, mode,
                   loudness, speechiness, acousticness, instrumentalness,
                   liveness, valence, tempo, time_signature
            FROM audio_features
            WHERE track_id = %s
            """,
            (track_id,),
        )
    )
    return len(rows)


def scenario_partition_read(session, samples: dict) -> int:
    album_id = random.choice(samples["album_ids"])
    rel_rows = list(
        session.execute(
            """
            SELECT track_id, album_id, is_primary
            FROM track_albums
            WHERE album_id = %s
            ALLOW FILTERING
            """,
            (album_id,),
        )
    )
    return len(rel_rows)


def scenario_top_n_ranking_scaled(session, samples: dict, scale: int) -> int:
    chart_id = random.choice(samples["chart_ids"])
    limit_n = scaled_count(scale, 0.00005, min_count=50, max_count=10_000)
    rows = list(
        session.execute(
            """
            SELECT chart_id, chart_date, track_id, position, streams
            FROM chart_entries
            WHERE chart_id = %s
            ORDER BY chart_date DESC
            LIMIT %s
            """,
            (chart_id, limit_n),
        )
    )
    return len(rows)


def scenario_secondary_index_read_scaled(session, _samples: dict, scale: int) -> int:
    limit_n = scaled_count(scale, 0.001, min_count=1_000, max_count=50_000)
    rows = list(
        session.execute(
            """
            SELECT track_id, name, duration_min
            FROM tracks
            WHERE explicit = true
            LIMIT %s
            ALLOW FILTERING
            """,
            (limit_n,),
        )
    )
    return len(rows)


def scenario_local_aggregation(session, samples: dict) -> int:
    artist_id = random.choice(samples["artist_ids"])
    links = list(
        session.execute(
            """
            SELECT track_id
            FROM track_artists
            WHERE artist_id = %s
            LIMIT 5000
            ALLOW FILTERING
            """,
            (artist_id,),
        )
    )

    if not links:
        return 0

    total_tempo = Decimal("0")
    total_danceability = Decimal("0")
    total_energy = Decimal("0")
    count = 0

    for row in links:
        af = session.execute(
            "SELECT tempo, danceability, energy FROM audio_features WHERE track_id = %s",
            (row.track_id,),
        ).one()
        if af is None:
            continue
        total_tempo += af.tempo or Decimal("0")
        total_danceability += af.danceability or Decimal("0")
        total_energy += af.energy or Decimal("0")
        count += 1

    if count == 0:
        return 0

    _avg_tempo = total_tempo / count
    _avg_danceability = total_danceability / count
    _avg_energy = total_energy / count
    _ = (_avg_tempo, _avg_danceability, _avg_energy)
    return 1


def scenario_range_query_scaled(session, _samples: dict, scale: int) -> int:
    limit_n = scaled_count(scale, 0.0002, min_count=2_000, max_count=100_000)
    rows = list(
        session.execute(
            """
            SELECT album_id, name, release_date, total_tracks
            FROM albums
            WHERE release_date >= '2015-01-01' AND release_date <= '2020-12-31'
            LIMIT %s
            ALLOW FILTERING
            """,
            (limit_n,),
        )
    )
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
                include_audio_features=True,
            )

        cluster = None
        session = None
        try:
            cluster, session = connect_db(cfg)
            ensure_existing_schema(session, cfg.keyspace)
            samples = fetch_sample_ids(session)

            for with_indexes in index_modes:
                apply_indexes(session, MANAGED_INDEXES, with_indexes)
                index_label = "with_indexes" if with_indexes else "no_indexes"

                scenarios = [
                    ("point_read", lambda: scenario_point_read(session, samples)),
                    ("partition_read", lambda: scenario_partition_read(session, samples)),
                    ("top_n_ranking", lambda: scenario_top_n_ranking_scaled(session, samples, scale=scale)),
                    ("secondary_index_read", lambda: scenario_secondary_index_read_scaled(session, samples, scale=scale)),
                    ("local_aggregation", lambda: scenario_local_aggregation(session, samples)),
                    ("range_query", lambda: scenario_range_query_scaled(session, samples, scale=scale)),
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
        finally:
            close_db(cluster, session)

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
        description="Cassandra READ benchmark - 6 scenariuszy odczytu z porownaniem przed/po indeksach."
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
    parser.add_argument("--output", default="cassandra/results/cassandra_read_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "9043")))
    parser.add_argument("--db-keyspace", default=os.getenv("DB_KEYSPACE", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))

    args = parser.parse_args()

    if args.runs_per_scenario <= 0:
        raise ValueError("runs-per-scenario must be > 0")
    if args.pool_size <= 0:
        raise ValueError("pool-size must be > 0")

    scales = parse_scales(args.scales)

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

    print(f"\n>>> Cassandra READ Benchmark - tryb: {mode_label} <<<")

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
