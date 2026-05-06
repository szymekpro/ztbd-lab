"""
Cassandra UPDATE benchmark suite - 6 scenariuszy aktualizacji.
"""

import argparse
import os
import random
from datetime import datetime, timezone
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
    parse_scales,
    prepare_scale_data_with_seed_script,
    scaled_count,
    timed_run,
)


MANAGED_INDEXES = [
    {
        "name": "idx_chart_entries_track_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_chart_entries_track_id ON chart_entries (track_id)",
        "drop": "DROP INDEX IF EXISTS idx_chart_entries_track_id",
        "new": False,
    },
    {
        "name": "idx_artist_genres_genre_id",
        "create": "CREATE INDEX IF NOT EXISTS idx_artist_genres_genre_id ON artist_genres (genre_id)",
        "drop": "DROP INDEX IF EXISTS idx_artist_genres_genre_id",
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


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def fetch_sample_ids(session) -> dict:
    samples: dict = {}

    track_ids = [int(r.track_id) for r in session.execute("SELECT track_id FROM audio_features LIMIT 500")]
    if not track_ids:
        track_ids = [int(r.track_id) for r in session.execute("SELECT track_id FROM tracks LIMIT 500")]

    chart_ids = [int(r.chart_id) for r in session.execute("SELECT chart_id FROM charts LIMIT 20")]
    chart_entries: list[tuple[int, object, int, int, int]] = []
    for chart_id in chart_ids:
        rows = list(
            session.execute(
                "SELECT chart_id, chart_date, track_id, position, streams FROM chart_entries WHERE chart_id = %s LIMIT 200",
                (chart_id,),
            )
        )
        for row in rows:
            chart_entries.append(
                (
                    int(row.chart_id),
                    row.chart_date,
                    int(row.track_id),
                    int(row.position or 0),
                    int(row.streams or 0),
                )
            )
        if len(chart_entries) >= 300:
            break

    artist_ids_with_genres = [
        int(r.artist_id) for r in session.execute("SELECT artist_id FROM artist_genres LIMIT 200")
    ]
    genre_ids = [int(r.genre_id) for r in session.execute("SELECT genre_id FROM genres LIMIT 200")]

    samples["track_ids"] = track_ids if track_ids else [1]
    samples["chart_entries"] = chart_entries if chart_entries else [(1, None, 1, 100, 1000)]
    samples["chart_track_ids"] = list({entry[2] for entry in samples["chart_entries"]})
    samples["artist_ids_with_genres"] = artist_ids_with_genres if artist_ids_with_genres else [1]
    samples["genre_ids"] = genre_ids if genre_ids else [1]
    samples["bulk_genre_ids"] = samples["genre_ids"][:10]

    print("[SETUP] Zaladowano sample IDs:")
    print(f"  track_ids (z audio_features): {len(samples['track_ids'])}")
    print(f"  chart_entries:               {len(samples['chart_entries'])}")
    print(f"  chart_track_ids:             {len(samples['chart_track_ids'])}")
    print(f"  artist_ids_with_genres:      {len(samples['artist_ids_with_genres'])}")
    print(f"  genre_ids:                   {len(samples['genre_ids'])}")

    return samples


def scenario_point_update_scaled(session, samples: dict, scale: int) -> int:
    n = scaled_count(scale, 0.0001, min_count=200, max_count=20_000)
    ids = [random.choice(samples["track_ids"]) for _ in range(n)]
    now = _now()

    stmt = session.prepare("UPDATE tracks SET name = ?, updated_at = ? WHERE track_id = ?")
    params = [(f"Batch Updated {track_id % 1000000}", now, track_id) for track_id in ids]
    execute_concurrent_with_args(
        session,
        stmt,
        params,
        concurrency=min(300, max(50, n // 10)),
        raise_on_first_error=True,
    )

    return n


def scenario_nested_update(session, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    new_energy = Decimal(str(round(random.uniform(0.01, 0.99), 3)))
    session.execute(
        "UPDATE audio_features SET energy = %s WHERE track_id = %s",
        (new_energy, track_id),
    )
    return 1


def scenario_bulk_update_scaled(session, samples: dict, scale: int) -> int:
    genre_id = random.choice(samples["bulk_genre_ids"])
    limit_n = scaled_count(scale, 0.001, min_count=2_000, max_count=200_000)

    artists = list(
        session.execute(
            "SELECT artist_id FROM artist_genres WHERE genre_id = %s LIMIT 2000 ALLOW FILTERING",
            (genre_id,),
        )
    )
    if not artists:
        return 0

    target_track_ids: list[int] = []
    for artist_row in artists:
        if len(target_track_ids) >= limit_n:
            break
        links = session.execute(
            "SELECT track_id FROM track_artists WHERE artist_id = %s LIMIT %s ALLOW FILTERING",
            (artist_row.artist_id, min(5000, limit_n)),
        )
        for link in links:
            target_track_ids.append(int(link.track_id))
            if len(target_track_ids) >= limit_n:
                break

    if not target_track_ids:
        return 0

    now = _now()
    for track_id in target_track_ids:
        session.execute(
            "UPDATE tracks SET explicit = true, updated_at = %s WHERE track_id = %s",
            (now, track_id),
        )

    return len(target_track_ids)


def scenario_atomic_increment_scaled(session, samples: dict, scale: int) -> int:
    track_id = random.choice(samples.get("chart_track_ids", [1]))
    limit_n = scaled_count(scale, 0.0002, min_count=500, max_count=50_000)

    rows = list(
        session.execute(
            """
            SELECT chart_id, chart_date, track_id, streams
            FROM chart_entries
            WHERE track_id = %s
            LIMIT %s
            ALLOW FILTERING
            """,
            (track_id, limit_n),
        )
    )

    if not rows:
        return 0

    for row in rows:
        current_streams = int(row.streams or 0)
        session.execute(
            """
            UPDATE chart_entries
            SET streams = %s
            WHERE chart_id = %s AND chart_date = %s AND track_id = %s
            """,
            (current_streams + 1000, row.chart_id, row.chart_date, row.track_id),
        )

    return len(rows)


def scenario_list_append(session, samples: dict) -> int:
    artist_id = random.choice(samples["artist_ids_with_genres"])
    genre_id = random.choice(samples["genre_ids"])
    session.execute(
        "INSERT INTO artist_genres (artist_id, genre_id) VALUES (%s, %s)",
        (artist_id, genre_id),
    )
    return 1


def scenario_cas_update(session, samples: dict) -> int:
    chart_id, chart_date, track_id, current_position, _streams = random.choice(samples["chart_entries"])
    if chart_date is None:
        return 0

    new_position = max(1, current_position + random.randint(-10, 10))
    result = session.execute(
        """
        UPDATE chart_entries
        SET position = %s
        WHERE chart_id = %s AND chart_date = %s AND track_id = %s
        IF position > %s
        """,
        (new_position, chart_id, chart_date, track_id, new_position),
    ).one()

    if result is None:
        return 0

    applied = bool(getattr(result, "applied", result[0]))
    return 1 if applied else 0


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
                    include_audio_features=True,
                )

            cluster = None
            session = None
            try:
                cluster, session = connect_db(cfg)
                ensure_existing_schema(session, cfg.keyspace)
                apply_indexes(session, MANAGED_INDEXES, with_indexes)
                samples = fetch_sample_ids(session)

                scenarios = [
                    ("point_update", lambda: scenario_point_update_scaled(session, samples, scale=scale)),
                    ("nested_update", lambda: scenario_nested_update(session, samples)),
                    ("bulk_update", lambda: scenario_bulk_update_scaled(session, samples, scale=scale)),
                    ("atomic_increment", lambda: scenario_atomic_increment_scaled(session, samples, scale=scale)),
                    ("list_append", lambda: scenario_list_append(session, samples)),
                    ("cas_update", lambda: scenario_cas_update(session, samples)),
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
            finally:
                close_db(cluster, session)

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
        description="Cassandra UPDATE benchmark - 6 scenariuszy aktualizacji z porownaniem przed/po indeksach."
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
    parser.add_argument("--output", default="cassandra/results/cassandra_update_benchmark_results.csv")

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

    print(f"\n>>> Cassandra UPDATE Benchmark - tryb: {mode_label} <<<")

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
