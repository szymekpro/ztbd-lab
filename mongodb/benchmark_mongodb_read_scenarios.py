"""MongoDB READ benchmark suite – port of the PostgreSQL suite.

Scenarios (same names as in postgres/benchmark_psql_read_scenarios.py):
  1. point_read
  2. partition_read
  3. top_n_ranking
  4. secondary_index_read
  5. local_aggregation
  6. range_query

Notes:
- SQL JOIN-based scenarios are implemented via $lookup aggregation pipelines.
- Output CSV schema matches plot_results.py expectations.
- Default output filename follows the repo convention so plot_results.py can load it
    automatically when run from the repo root.
"""

from __future__ import annotations

import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import argparse
import os
import random
import time
from datetime import datetime, timezone
from statistics import mean
from typing import Optional, Any

try:
    from mongodb.benchmark_mongodb_common import (
        DbConfig,
        apply_indexes,
        connect_db,
        ensure_existing_schema,
        parse_scales,
        prepare_scale_data_with_seed_script,
        _scaled_count,
    )
except ModuleNotFoundError:
    from benchmark_mongodb_common import (  # type: ignore
        DbConfig,
        apply_indexes,
        connect_db,
        ensure_existing_schema,
        parse_scales,
        prepare_scale_data_with_seed_script,
        _scaled_count,
    )


# ---------------------------------------------------------------------------
# SAMPLE ID FETCHING
# ---------------------------------------------------------------------------


def fetch_sample_ids(db: Any, *, scale: int) -> dict:
    samples: dict = {}

    # track_ids: seek-sampling from audio_features (mirrors PostgreSQL approach)
    max_track_doc = db.audio_features.find_one({}, {"track_id": 1, "_id": 0}, sort=[("track_id", -1)])
    max_track_id = int(max_track_doc.get("track_id", 1)) if max_track_doc else 1
    start_track_id = random.randint(1, max_track_id)
    rows = list(
        db.audio_features.find({"track_id": {"$gte": start_track_id}}, {"track_id": 1, "_id": 0})
        .sort("track_id", 1)
        .limit(50)
    )
    if not rows:
        rows = list(db.audio_features.find({}, {"track_id": 1, "_id": 0}).sort("track_id", 1).limit(50))
    samples["track_ids"] = [int(r.get("track_id", 1)) for r in rows] if rows else [1]

    # album_ids: seek-sampling from albums
    max_album_doc = db.albums.find_one({}, {"album_id": 1, "_id": 0}, sort=[("album_id", -1)])
    max_album_id = int(max_album_doc.get("album_id", 1)) if max_album_doc else 1
    start_album_id = random.randint(1, max_album_id)
    album_rows = list(
        db.albums.find({"album_id": {"$gte": start_album_id}}, {"album_id": 1, "_id": 0})
        .sort("album_id", 1)
        .limit(50)
    )
    if not album_rows:
        album_rows = list(db.albums.find({}, {"album_id": 1, "_id": 0}).sort("album_id", 1).limit(50))
    seen_album_ids: set[int] = set()
    album_ids: list[int] = []
    for r in album_rows:
        aid = r.get("album_id")
        if aid is None:
            continue
        aid_i = int(aid)
        if aid_i in seen_album_ids:
            continue
        seen_album_ids.add(aid_i)
        album_ids.append(aid_i)
        if len(album_ids) >= 10:
            break
    samples["album_ids"] = album_ids if album_ids else [1]

    # chart_date_pairs: seek-sampling by chart_entry_id (mirrors PostgreSQL)
    max_ce_doc = db.chart_entries.find_one({}, {"chart_entry_id": 1, "_id": 0}, sort=[("chart_entry_id", -1)])
    max_chart_entry_id = int(max_ce_doc.get("chart_entry_id", 1)) if max_ce_doc else 1
    start_chart_entry_id = random.randint(1, max_chart_entry_id)
    ce_rows = list(
        db.chart_entries.find(
            {"chart_entry_id": {"$gte": start_chart_entry_id}},
            {"chart_id": 1, "chart_date": 1, "_id": 0},
        )
        .sort("chart_entry_id", 1)
        .limit(50)
    )
    if not ce_rows:
        ce_rows = list(
            db.chart_entries.find({}, {"chart_id": 1, "chart_date": 1, "_id": 0})
            .sort("chart_entry_id", 1)
            .limit(50)
        )
    pairs: list[tuple[int, datetime]] = []
    seen_pairs: set[tuple[int, datetime]] = set()
    for r in ce_rows:
        cid_raw = r.get("chart_id")
        cdate = r.get("chart_date")
        if cid_raw is None or not isinstance(cdate, datetime):
            continue
        key = (int(cid_raw), cdate)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        pairs.append(key)
        if len(pairs) >= 10:
            break
    samples["chart_date_pairs"] = pairs if pairs else [(1, datetime(2024, 1, 1, tzinfo=timezone.utc))]

    # chart_ids
    chart_ids = [
        int(r.get("chart_id", 1))
        for r in db.charts.find({}, {"chart_id": 1, "_id": 0}).sort("chart_id", 1).limit(10)
    ]
    samples["chart_ids"] = chart_ids if chart_ids else [1]

    # artist_ids
    artist_ids = [
        int(r.get("artist_id", 1))
        for r in db.artists.find({}, {"artist_id": 1, "_id": 0}).sort("artist_id", 1).limit(10)
    ]
    samples["artist_ids"] = artist_ids if artist_ids else [1]

    print("[SETUP] Załadowano sample IDs:")
    print(f"  track_ids:        {len(samples['track_ids'])} sztuk")
    print(f"  album_ids:        {len(samples['album_ids'])} sztuk")
    print(f"  chart_date_pairs: {len(samples['chart_date_pairs'])} sztuk")
    print(f"  chart_ids:        {len(samples['chart_ids'])} sztuk")
    print(f"  artist_ids:       {len(samples['artist_ids'])} sztuk")
    return samples


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------


def scenario_point_read(db: Any, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    doc = db.audio_features.find_one({"track_id": track_id}, {"_id": 0})
    return 1 if doc else 0


def scenario_partition_read(db: Any, samples: dict) -> int:
    album_id = random.choice(samples["album_ids"])
    track_ids = [
        int(r.get("track_id"))
        for r in db.track_albums.find({"album_id": album_id}, {"track_id": 1, "_id": 0}).limit(5000)
        if r.get("track_id") is not None
    ]
    if not track_ids:
        return 0

    # Fetch tracks in one query.
    docs = list(db.tracks.find({"track_id": {"$in": track_ids}}, {"_id": 0}).limit(len(track_ids)))
    return len(docs)


def scenario_top_n_ranking_scaled(db: Any, samples: dict, *, scale: int) -> int:
    chart_id = random.choice(samples["chart_ids"])
    limit_n = _scaled_count(scale, 0.00005, min_count=50, max_count=10_000)

    pipeline = [
        {"$match": {"chart_id": chart_id}},
        {"$sort": {"chart_date": -1, "position": 1}},
        {"$limit": int(limit_n)},
        {
            "$lookup": {
                "from": "tracks",
                "localField": "track_id",
                "foreignField": "track_id",
                "as": "track",
            }
        },
        {"$project": {"_id": 0, "chart_entry_id": 1, "track_id": 1, "position": 1, "streams": 1, "track.name": 1}},
    ]
    rows = list(db.chart_entries.aggregate(pipeline, allowDiskUse=True))
    return len(rows)


def scenario_secondary_index_read_scaled(db: Any, _samples: dict, *, scale: int) -> int:
    limit_n = _scaled_count(scale, 0.001, min_count=1_000, max_count=50_000)
    rows = list(db.tracks.find({"explicit": True}, {"track_id": 1, "name": 1, "duration_min": 1, "_id": 0}).limit(int(limit_n)))
    return len(rows)


def scenario_local_aggregation(db: Any, samples: dict) -> int:
    artist_id = random.choice(samples["artist_ids"])

    pipeline = [
        {"$match": {"artist_id": artist_id}},
        {"$lookup": {"from": "audio_features", "localField": "track_id", "foreignField": "track_id", "as": "af"}},
        {"$unwind": "$af"},
        {
            "$group": {
                "_id": None,
                "track_count": {"$sum": 1},
                "avg_tempo": {"$avg": "$af.tempo"},
                "avg_danceability": {"$avg": "$af.danceability"},
                "avg_energy": {"$avg": "$af.energy"},
            }
        },
        {"$project": {"_id": 0}},
    ]

    rows = list(db.track_artists.aggregate(pipeline, allowDiskUse=True))
    return 1 if rows else 0


def scenario_range_query_scaled(db: Any, _samples: dict, *, scale: int) -> int:
    limit_n = _scaled_count(scale, 0.0002, min_count=2_000, max_count=100_000)
    start = datetime(2015, 1, 1, tzinfo=timezone.utc)
    end = datetime(2020, 12, 31, tzinfo=timezone.utc)

    rows = list(
        db.albums.find(
            {"release_date": {"$gte": start, "$lte": end}},
            {"album_id": 1, "name": 1, "release_date": 1, "total_tracks": 1, "_id": 0},
        )
        .sort("release_date", 1)
        .limit(int(limit_n))
    )
    return len(rows)


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------


def timed_run(fn, *args, **kwargs) -> tuple[float, int]:
    start = time.perf_counter()
    ops = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, int(ops)


def run_benchmark(
    cfg: DbConfig,
    *,
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

        client, db = connect_db(cfg)
        try:
            ensure_existing_schema(db)
            samples = fetch_sample_ids(db, scale=scale)

            for with_indexes in index_modes:
                apply_indexes(db, with_indexes)
                index_label = "with_indexes" if with_indexes else "no_indexes"

                scenarios = [
                    ("point_read", lambda: scenario_point_read(db, samples)),
                    ("partition_read", lambda: scenario_partition_read(db, samples)),
                    ("top_n_ranking", lambda: scenario_top_n_ranking_scaled(db, samples, scale=scale)),
                    ("secondary_index_read", lambda: scenario_secondary_index_read_scaled(db, samples, scale=scale)),
                    ("local_aggregation", lambda: scenario_local_aggregation(db, samples)),
                    ("range_query", lambda: scenario_range_query_scaled(db, samples, scale=scale)),
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
            client.close()

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row.get("scale", 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== MongoDB READ Benchmark Summary (avg z prób) ===")
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
        description=(
            "MongoDB READ benchmark suite (6 scenariuszy) – kompatybilny z plot_results.py."
        )
    )

    parser.add_argument("--scales", default="500000,1000000,10000000")
    parser.add_argument("--runs-per-scenario", type=int, default=3)
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
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="mongodb/results/mongodb_read_benchmark_results.csv")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "27018")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))
    parser.add_argument("--db-auth-source", default=os.getenv("DB_AUTH_SOURCE", "spotify"))

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
        auth_source=args.db_auth_source,
    )

    if args.both_index_modes:
        index_modes = [False, True]
        mode_label = "BEZ indeksów + Z indeksami"
    else:
        with_indexes = not args.no_indexes
        index_modes = [with_indexes]
        mode_label = "Z indeksami" if with_indexes else "BEZ indeksów"

    print(f"\n>>> MongoDB READ Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg,
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
