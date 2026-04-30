"""MongoDB UPDATE benchmark suite – port of the PostgreSQL suite.

Scenarios (same names as in postgres/benchmark_psql_update_scenarios.py):
  1. point_update
  2. nested_update
  3. bulk_update
  4. atomic_increment
  5. list_append
  6. cas_update

Notes:
- SQL subqueries / JOIN updates are approximated with aggregations + update_many.
- list_append uses insert into artist_genres (unique compound index) which is
  the closest equivalent to an "append to collection" in this schema.
- Output CSV schema matches visualization/plot_results.py expectations.
- Default output filename matches PostgreSQL so plot_results.py works unchanged
  when called with --results-dir ../mongodb/results.
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
import uuid
from datetime import datetime, timezone
from statistics import mean
from typing import Optional, Any

from pymongo.errors import DuplicateKeyError

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

    # Seek-sample by existing audio_features (mirrors PostgreSQL approach).
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

    samples["max_track_id"] = max_track_id

    chart_entries = list(
        db.chart_entries.find({"streams": {"$exists": True}}, {"chart_entry_id": 1, "position": 1, "streams": 1, "_id": 0}).limit(50)
    )
    if not chart_entries:
        chart_entries = [{"chart_entry_id": 1, "position": 5, "streams": 1000}]
    samples["chart_entries"] = [
        (int(r.get("chart_entry_id", 1)), int(r.get("position", 1)), int(r.get("streams", 0) or 0))
        for r in chart_entries
    ]

    # track_ids present in chart_entries
    seen: set[int] = set()
    chart_track_ids: list[int] = []
    for r in db.chart_entries.find({}, {"track_id": 1, "_id": 0}).limit(1000):
        tid = r.get("track_id")
        if tid is None:
            continue
        tid_i = int(tid)
        if tid_i in seen:
            continue
        seen.add(tid_i)
        chart_track_ids.append(tid_i)
        if len(chart_track_ids) >= 20:
            break
    samples["chart_track_ids"] = chart_track_ids if chart_track_ids else [1]

    # artists with existing genres
    artist_ids_with_genres = list(db.artist_genres.distinct("artist_id"))
    artist_ids_with_genres = [int(a) for a in artist_ids_with_genres if a is not None]
    samples["artist_ids_with_genres"] = artist_ids_with_genres[:20] if artist_ids_with_genres else [1]

    genre_ids = [int(r.get("genre_id", 1)) for r in db.genres.find({}, {"genre_id": 1, "_id": 0}) if r.get("genre_id") is not None]
    samples["genre_ids"] = genre_ids if genre_ids else [1]

    # bulk_genre_ids: keep setup cheap.
    # In our synthetic data, tracks are distributed evenly across artists, so
    # "genres with many artists" correlates with "genres with many tracks".
    try:
        top = list(
            db.artist_genres.aggregate(
                [
                    {"$group": {"_id": "$genre_id", "cnt": {"$sum": 1}}},
                    {"$sort": {"cnt": -1}},
                    {"$limit": 10},
                ],
                allowDiskUse=True,
            )
        )
        bulk_genre_ids = [int(r.get("_id")) for r in top if r.get("_id") is not None]
    except Exception:
        bulk_genre_ids = []
    samples["bulk_genre_ids"] = bulk_genre_ids if bulk_genre_ids else (
        samples["genre_ids"][:10] if samples["genre_ids"] else [1]
    )

    print("[SETUP] Załadowano sample IDs:")
    print(f"  track_ids (z audio_features): {len(samples['track_ids'])}")
    print(f"  max_track_id:                {samples['max_track_id']}")
    print(f"  chart_entries:               {len(samples['chart_entries'])}")
    print(f"  chart_track_ids:             {len(samples['chart_track_ids'])}")
    print(f"  artist_ids_with_genres:      {len(samples['artist_ids_with_genres'])}")
    print(f"  genre_ids:                   {len(samples['genre_ids'])}")
    print(f"  bulk_genre_ids:              {len(samples['bulk_genre_ids'])}")
    return samples


# ---------------------------------------------------------------------------
# SCENARIOS
# ---------------------------------------------------------------------------


def scenario_point_update(db: Any, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    new_name = f"Updated Track {uuid.uuid4().hex[:8]}"
    res = db.tracks.update_one(
        {"track_id": track_id},
        {"$set": {"name": new_name, "updated_at": datetime.now(tz=timezone.utc)}},
    )
    return int(res.modified_count)


def scenario_nested_update(db: Any, samples: dict) -> int:
    track_id = random.choice(samples["track_ids"])
    new_energy = round(random.uniform(0.01, 0.99), 3)
    res = db.audio_features.update_one({"track_id": track_id}, {"$set": {"energy": new_energy}})
    return int(res.modified_count)


def _pick_target_track_ids_for_genre(
    db: Any,
    *,
    genre_id: int,
    limit_n: int,
    artist_cap: int = 0,
) -> list[int]:
    """Pick distinct track_ids for tracks in a given genre.

    The naive $lookup (artist_genres -> track_artists) can explode in no-index
    mode (nested scans). Instead we:
    - fetch artist_ids for the genre (small collection),
    - scan track_artists once with $match artist_id in [...],
    - group distinct track_id and LIMIT.

    With indexes enabled, this uses idx_track_artists_artist_id; without indexes,
    it is still bounded to a single scan.
    """

    artist_ids = db.artist_genres.distinct("artist_id", {"genre_id": int(genre_id)})
    artist_ids = [int(a) for a in artist_ids if a is not None]
    if artist_cap and artist_cap > 0:
        artist_ids = artist_ids[: int(artist_cap)]
    if not artist_ids:
        return []

    pipeline = [
        {"$match": {"artist_id": {"$in": artist_ids}}},
        {"$group": {"_id": "$track_id"}},
        {"$limit": int(limit_n)},
    ]
    rows = list(db.track_artists.aggregate(pipeline, allowDiskUse=True))
    return [int(r.get("_id")) for r in rows if r.get("_id") is not None]


def scenario_bulk_update_scaled(
    db: Any,
    samples: dict,
    *,
    scale: int,
    with_indexes: bool,
    bulk_fraction: float,
    bulk_min: int,
    bulk_max: int,
    bulk_max_no_indexes: int,
    bulk_artist_cap: int,
) -> int:
    genre_id = int(random.choice(samples["bulk_genre_ids"]))
    limit_n = _scaled_count(scale, float(bulk_fraction), min_count=int(bulk_min), max_count=int(bulk_max))
    if (not with_indexes) and bulk_max_no_indexes and bulk_max_no_indexes > 0:
        limit_n = min(int(limit_n), int(bulk_max_no_indexes))

    track_ids = _pick_target_track_ids_for_genre(
        db,
        genre_id=genre_id,
        limit_n=limit_n,
        artist_cap=bulk_artist_cap,
    )
    if not track_ids:
        return 0

    # Toggle explicit for selected tracks using aggregation update pipeline.
    res = db.tracks.update_many(
        {"track_id": {"$in": track_ids}},
        [
            {"$set": {"explicit": {"$not": "$explicit"}, "updated_at": datetime.now(tz=timezone.utc)}},
        ],
    )
    return int(res.modified_count)


def scenario_atomic_increment(db: Any, samples: dict) -> int:
    chart_entry_id, _pos, _streams = random.choice(samples["chart_entries"])
    res = db.chart_entries.update_one({"chart_entry_id": chart_entry_id}, {"$inc": {"streams": 1000}})
    return int(res.modified_count)


def scenario_list_append(db: Any, samples: dict) -> int:
    artist_id = int(random.choice(samples["artist_ids_with_genres"]))
    genre_id = int(random.choice(samples["genre_ids"]))

    try:
        db.artist_genres.insert_one({"artist_id": artist_id, "genre_id": genre_id})
    except DuplicateKeyError:
        pass
    return 1


def scenario_cas_update(db: Any, samples: dict) -> int:
    chart_entry_id, current_position, _streams = random.choice(samples["chart_entries"])
    new_position = max(1, int(current_position) + random.randint(-10, 10))

    res = db.chart_entries.update_one(
        {"chart_entry_id": int(chart_entry_id), "position": {"$gt": int(new_position)}},
        {"$set": {"position": int(new_position)}},
    )
    return int(res.modified_count)


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
    bulk_update_fraction: float,
    bulk_update_min: int,
    bulk_update_max: int,
    bulk_update_max_no_indexes: int,
    bulk_update_artist_cap: int,
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

            client, db = connect_db(cfg)
            try:
                ensure_existing_schema(db)
                apply_indexes(db, with_indexes)
                samples = fetch_sample_ids(db, scale=scale)

                scenarios = [
                    ("point_update", lambda: scenario_point_update(db, samples)),
                    ("nested_update", lambda: scenario_nested_update(db, samples)),
                    (
                        "bulk_update",
                        lambda: scenario_bulk_update_scaled(
                            db,
                            samples,
                            scale=scale,
                            with_indexes=with_indexes,
                            bulk_fraction=bulk_update_fraction,
                            bulk_min=bulk_update_min,
                            bulk_max=bulk_update_max,
                            bulk_max_no_indexes=bulk_update_max_no_indexes,
                            bulk_artist_cap=bulk_update_artist_cap,
                        ),
                    ),
                    ("atomic_increment", lambda: scenario_atomic_increment(db, samples)),
                    ("list_append", lambda: scenario_list_append(db, samples)),
                    ("cas_update", lambda: scenario_cas_update(db, samples)),
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
                client.close()

    return results


def print_summary(results: list[dict]) -> None:
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for row in results:
        key = (row.get("scale", 0), row["index_mode"], row["scenario"])
        groups.setdefault(key, []).append(row)

    print("\n=== MongoDB UPDATE Benchmark Summary (avg z prób) ===")
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
        description="MongoDB UPDATE benchmark suite (6 scenariuszy) – kompatybilny z visualization/plot_results.py."
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

    # Tuning knobs for runtime-bounded no-index runs.
    parser.add_argument(
        "--bulk-update-fraction",
        type=float,
        default=0.001,
        help="Fraction of scale used for bulk_update LIMIT (default mirrors PostgreSQL scaling).",
    )
    parser.add_argument(
        "--bulk-update-min",
        type=int,
        default=2000,
        help="Minimum LIMIT for bulk_update (default mirrors PostgreSQL).",
    )
    parser.add_argument(
        "--bulk-update-max",
        type=int,
        default=200000,
        help="Maximum LIMIT for bulk_update (default mirrors PostgreSQL).",
    )
    parser.add_argument(
        "--bulk-update-max-no-indexes",
        type=int,
        default=5000,
        help="Hard cap for bulk_update LIMIT when index_mode=no_indexes (keeps runtime bounded).",
    )
    parser.add_argument(
        "--bulk-update-artist-cap",
        type=int,
        default=0,
        help="Optional cap on number of artists considered for a genre (0 = no cap).",
    )
    parser.add_argument("--seed-value", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=10000)
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Nie prefilluje danych do skali przed testami (szybszy dry-run).",
    )
    parser.add_argument("--output", default="mongodb/results/mongodb_update_benchmark_results.csv")

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

    print(f"\n>>> MongoDB UPDATE Benchmark – tryb: {mode_label} <<<")

    results = run_benchmark(
        cfg,
        scales=scales,
        runs_per_scenario=args.runs_per_scenario,
        skip_prepare=args.skip_prepare,
        seed_value=args.seed_value,
        pool_size=args.pool_size,
        index_modes=index_modes,
        bulk_update_fraction=args.bulk_update_fraction,
        bulk_update_min=args.bulk_update_min,
        bulk_update_max=args.bulk_update_max,
        bulk_update_max_no_indexes=args.bulk_update_max_no_indexes,
        bulk_update_artist_cap=args.bulk_update_artist_cap,
    )

    save_results_csv(results, args.output)
    print_summary(results)
    print(f"\nZapisano wyniki do: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
