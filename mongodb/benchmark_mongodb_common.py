"""Common helpers for MongoDB benchmark suites.

This mirrors the PostgreSQL benchmark scripts structure so that:
- seeding is reproducible per scale,
- results can be saved in the same CSV schema expected by visualization/plot_results.py.

MongoDB connection defaults match docker-compose.yml (host=localhost, port=27018).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone
from typing import Optional, Any


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    auth_source: str


def connect_db(cfg: DbConfig, *, max_pool_size: int = 200):
    try:
        from pymongo import MongoClient
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "PyMongo is required for MongoDB benchmarks. Install it with: pip install pymongo"
        ) from e

    client = MongoClient(
        host=cfg.host,
        port=cfg.port,
        username=cfg.user,
        password=cfg.password,
        authSource=cfg.auth_source,
        tz_aware=True,
        tzinfo=timezone.utc,
        maxPoolSize=max_pool_size,
    )
    return client, client[cfg.dbname]


def ensure_existing_schema(db: Any) -> None:
    required = {
        "tracks",
        "artists",
        "albums",
        "audio_features",
        "track_artists",
        "track_albums",
        "artist_genres",
        "charts",
        "chart_entries",
        "markets",
        "genres",
    }
    existing = set(db.list_collection_names())
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            "Required MongoDB collections not found. Start DB with docker-compose so init.mongo.js runs. "
            f"Missing: {', '.join(missing)}"
        )

    # Baseline index parity with SQL primary keys:
    # Many benchmark queries join / filter by numeric *_id fields (especially tracks.track_id).
    # Without an index on tracks.track_id, $lookup and $in queries can degrade to full scans
    # and appear to "hang" on large scales.
    try:
        db.tracks.create_index([("track_id", 1)], name="idx_tracks_track_id")
    except Exception:
        # If index creation fails (e.g., permissions), continue and let benchmark run.
        pass


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
    """Rebuild the dataset for the given scale using seed_mongodb_faker_data.py."""
    import sys
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import seed_mongodb_faker_data as seed_script

    seed_cfg = seed_script.DbConfig(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        auth_source=cfg.auth_source,
    )

    client, db = seed_script.connect_db(seed_cfg)
    try:
        seed_script.seed_all(
            db,
            n_genres=30,
            n_artists=max(50, target_rows // 20000),
            n_albums=max(80, target_rows // 10000),
            n_tracks=target_rows,
            seed=seed_value,
            truncate=True,
            pool_size=pool_size,
        )
    finally:
        client.close()


# ---------------------------------------------------------------------------
# INDEX MANAGEMENT
# ---------------------------------------------------------------------------

MANAGED_INDEXES = [
    {
        "collection": "chart_entries",
        "name": "idx_chart_entries_chart_date",
        "keys": [("chart_id", 1), ("chart_date", -1), ("position", 1)],
        "kwargs": {},
        "new": False,
    },
    {
        "collection": "chart_entries",
        "name": "idx_chart_entries_track_date",
        "keys": [("track_id", 1), ("chart_date", -1)],
        "kwargs": {},
        "new": False,
    },
    {
        "collection": "track_albums",
        "name": "idx_track_albums_album_id",
        "keys": [("album_id", 1)],
        "kwargs": {},
        "new": True,
    },
    {
        "collection": "track_artists",
        "name": "idx_track_artists_artist_id",
        "keys": [("artist_id", 1)],
        "kwargs": {},
        "new": True,
    },
    {
        "collection": "tracks",
        "name": "idx_tracks_explicit_true",
        "keys": [("explicit", 1)],
        "kwargs": {"partialFilterExpression": {"explicit": True}},
        "new": True,
    },
    # Base schema index (created by init.mongo.js). We manage it to compare before/after.
    {
        "collection": "albums",
        "name": "release_date_1",
        "keys": [("release_date", 1)],
        "kwargs": {},
        "new": False,
    },
]


def apply_indexes(db: Any, with_indexes: bool) -> None:
    action = "Tworzenie" if with_indexes else "Usuwanie"
    print(f"\n[INDEX] {action} indeksów...")

    for idx in MANAGED_INDEXES:
        coll = db[idx["collection"]]
        name = idx["name"]
        keys = idx["keys"]
        kwargs = idx.get("kwargs", {})

        if with_indexes:
            label = "(nowy)" if idx.get("new") else "(schemat)"
            print(f"  CREATE  {idx['collection']}.{name} {label}")
            # create_index is idempotent with same name+spec; if a different spec
            # exists under the same name, Mongo will error, which is fine.
            coll.create_index(keys, name=name, **kwargs)
        else:
            if name == "_id_":
                continue
            print(f"  DROP    {idx['collection']}.{name}")
            try:
                coll.drop_index(name)
            except Exception:
                # Missing index is OK (e.g., first run in no-indexes mode).
                pass

    print("[INDEX] Gotowe.\n")


def default_mongo_config_from_env() -> DbConfig:
    return DbConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "27018")),
        dbname=os.getenv("DB_NAME", "spotify"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PASSWORD", "user"),
        auth_source=os.getenv("DB_AUTH_SOURCE", "spotify"),
    )
