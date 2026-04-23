import argparse
import os
import random
import string
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from faker import Faker


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    auth_source: str


_BASE62 = string.digits + string.ascii_letters
_BASE36 = string.digits + string.ascii_uppercase

_BATCH_SIZE = 50_000


def _u01(track_id: int, salt: int, mod: int = 1000) -> float:
    # Deterministic pseudo-random in [0, 1).
    return ((track_id * 1103515245 + salt) % mod) / float(mod)


def base_n_encode(value: int, alphabet: str) -> str:
    if value < 0:
        raise ValueError("value must be >= 0")
    if value == 0:
        return alphabet[0]
    base = len(alphabet)
    out: list[str] = []
    while value:
        value, rem = divmod(value, base)
        out.append(alphabet[rem])
    return "".join(reversed(out))


def spotify_id_from_int(i: int, namespace: str) -> str:
    raw = base_n_encode(i, _BASE62)
    return (namespace + raw.rjust(21, _BASE62[0]))[:22]


def isrc_from_int(i: int, year: int = 25) -> str:
    country = "PL"
    registrant_idx = i // 100000
    registrant = base_n_encode(registrant_idx, _BASE36).rjust(3, "0")[:3]
    serial = i % 100000
    return f"{country}{registrant}{year:02d}{serial:05d}"


def dt_utc(d: date) -> datetime:
    return datetime.combine(d, time.min, tzinfo=timezone.utc)


def connect_db(cfg: DbConfig):
    try:
        from pymongo import MongoClient
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "PyMongo is required for MongoDB seeding. Install it with: pip install pymongo"
        ) from e

    client = MongoClient(
        host=cfg.host,
        port=cfg.port,
        username=cfg.user,
        password=cfg.password,
        authSource=cfg.auth_source,
        tz_aware=True,
    )
    return client, client[cfg.dbname]


def ensure_schema(db) -> None:
    required = {"tracks"}
    existing = set(db.list_collection_names())
    if not required.issubset(existing):
        raise RuntimeError(
            "Schema not found (missing tracks collection). Start DB with docker-compose so init.mongo.js runs."
        )


def truncate_all(db) -> None:
    tables = [
        "chart_entries",
        "charts",
        "audio_features",
        "track_albums",
        "track_artists",
        "album_artists",
        "artist_genres",
        "tracks",
        "albums",
        "artists",
        "markets",
        "genres",
    ]
    for table in tables:
        db[table].delete_many({})


def seed_genres(db, fake: Faker, n: int) -> list[int]:
    base = [
        "pop",
        "rock",
        "hip hop",
        "edm",
        "jazz",
        "classical",
        "r&b",
        "metal",
        "indie",
        "folk",
        "latin",
        "k-pop",
        "reggaeton",
        "blues",
        "punk",
        "house",
        "techno",
        "ambient",
        "lo-fi",
    ]
    while len(base) < n:
        base.append(fake.word().lower())

    docs = []
    genre_ids: list[int] = []
    for i, name in enumerate(base[:n], start=1):
        docs.append(
            {
                "genre_id": i,
                "name": name[:100],
                "created_at": datetime.now(tz=timezone.utc),
            }
        )
        genre_ids.append(i)

    if docs:
        db.genres.insert_many(docs, ordered=False)
    return genre_ids


def seed_markets(db) -> list[int]:
    markets = [
        (None, "Global"),
        ("US", "United States"),
        ("GB", "United Kingdom"),
        ("PL", "Poland"),
        ("DE", "Germany"),
        ("FR", "France"),
        ("ES", "Spain"),
        ("IT", "Italy"),
        ("BR", "Brazil"),
        ("JP", "Japan"),
    ]
    docs = []
    ids: list[int] = []
    for i, (code, name) in enumerate(markets, start=1):
        doc = {"market_id": i, "name": name}
        if code is not None:
            doc["country_code"] = code
        docs.append(doc)
        ids.append(i)

    db.markets.insert_many(docs, ordered=False)
    return ids


def seed_artists(db, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> list[int]:
    rng = random.Random(seed)
    name_pool = [fake.name()[:255] for _ in range(max(100, min(pool_size, 5000)))]
    genre_word_pool = [fake.word().lower() for _ in range(2000)]

    docs = []
    artist_ids: list[int] = []
    now = datetime.now(tz=timezone.utc)
    for i in range(1, n + 1):
        name = name_pool[(i - 1) % len(name_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
        raw_genres_text = ", ".join(
            {genre_word_pool[rng.randrange(len(genre_word_pool))] for _ in range(rng.randint(1, 4))}
        )[:500]
        raw_genres_text = raw_genres_text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        docs.append(
            {
                "artist_id": i,
                "name": f"{name} {i}" if n > len(name_pool) else name,
                "raw_genres_text": raw_genres_text,
                "created_at": now,
            }
        )
        artist_ids.append(i)

    if docs:
        db.artists.insert_many(docs, ordered=False)
    return artist_ids


def seed_albums(db, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> list[int]:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 5)).rstrip(".")[:255]
        for _ in range(max(200, min(pool_size, 10000)))
    ]

    docs = []
    album_ids: list[int] = []
    now = datetime.now(tz=timezone.utc)
    for i in range(1, n + 1):
        spotify_album_id = spotify_id_from_int(i - 1, "a")
        name = title_pool[(i - 1) % len(title_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
        album_type = rng.choice(["album", "single", "compilation", "ep", None])
        release = fake.date_between(date(2009, 1, 1), date(2025, 12, 31))

        doc = {
            "album_id": i,
            "spotify_album_id": spotify_album_id,
            "name": name,
            "release_date": dt_utc(release),
            "total_tracks": rng.randint(1, 30),
            "created_at": now,
        }
        if album_type is not None:
            doc["album_type"] = album_type

        docs.append(doc)
        album_ids.append(i)

    if docs:
        db.albums.insert_many(docs, ordered=False)
    return album_ids


def seed_tracks(db, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> list[int]:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 6)).rstrip(".")[:255]
        for _ in range(max(500, min(pool_size, 20000)))
    ]

    docs: list[dict] = []
    track_ids: list[int] = []
    now = datetime.now(tz=timezone.utc)
    for i in range(1, n + 1):
        spotify_track_id = spotify_id_from_int(i - 1, "t")
        name = title_pool[(i - 1) % len(title_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
        docs.append(
            {
                "track_id": i,
                "spotify_track_id": spotify_track_id,
                "name": name,
                "explicit": rng.choice([True, False]),
                "duration_min": round(rng.uniform(1.0, 9.5), 3),
                "disc_number": rng.choice([1, 2]),
                "track_number": rng.randint(1, 20),
                "isrc": isrc_from_int(i - 1, year=25),
                "created_at": now,
            }
        )

        # Avoid holding huge lists in memory for large scales.
        if len(docs) >= _BATCH_SIZE:
            db.tracks.insert_many(docs, ordered=False)
            docs.clear()

        # Only keep explicit ids for small-ish scales (used for charts sampling etc.).
        if n <= 1_000_000:
            track_ids.append(i)

    if docs:
        db.tracks.insert_many(docs, ordered=False)

    if n <= 1_000_000:
        return track_ids
    return list(range(1, min(n, 5000) + 1))


def seed_relations(
    db,
    artist_ids: list[int],
    genre_ids: list[int],
    album_ids: list[int],
    track_ids: list[int],
    genres_per_artist: int = 2,
    artists_per_album: int = 1,
    artists_per_track: int = 2,
) -> None:
    artist_genres_docs: list[dict] = []
    album_artists_docs: list[dict] = []
    track_artists_docs: list[dict] = []
    track_albums_docs: list[dict] = []

    if artist_ids and genre_ids:
        for artist_id in artist_ids:
            for gs in range(1, max(1, genres_per_artist) + 1):
                genre_idx = (artist_id - 1 + gs - 1) % len(genre_ids)
                artist_genres_docs.append({"artist_id": artist_id, "genre_id": genre_ids[genre_idx]})

                if len(artist_genres_docs) >= _BATCH_SIZE:
                    db.artist_genres.insert_many(artist_genres_docs, ordered=False)
                    artist_genres_docs.clear()

    if album_ids and artist_ids:
        for album_id in album_ids:
            for gs in range(1, max(1, artists_per_album) + 1):
                artist_idx = (album_id - 1 + gs - 1) % len(artist_ids)
                album_artists_docs.append(
                    {
                        "album_id": album_id,
                        "artist_id": artist_ids[artist_idx],
                        "artist_order": gs,
                    }
                )

                if len(album_artists_docs) >= _BATCH_SIZE:
                    db.album_artists.insert_many(album_artists_docs, ordered=False)
                    album_artists_docs.clear()

    if track_ids and artist_ids:
        for track_id in track_ids:
            for gs in range(1, max(1, artists_per_track) + 1):
                artist_idx = (track_id - 1 + gs - 1) % len(artist_ids)
                track_artists_docs.append(
                    {
                        "track_id": track_id,
                        "artist_id": artist_ids[artist_idx],
                        "artist_order": gs,
                    }
                )

                if len(track_artists_docs) >= _BATCH_SIZE:
                    db.track_artists.insert_many(track_artists_docs, ordered=False)
                    track_artists_docs.clear()

    if track_ids and album_ids:
        for track_id in track_ids:
            album_idx = (track_id - 1) % len(album_ids)
            track_albums_docs.append(
                {
                    "track_id": track_id,
                    "album_id": album_ids[album_idx],
                    "is_primary": True,
                }
            )

            if len(track_albums_docs) >= _BATCH_SIZE:
                db.track_albums.insert_many(track_albums_docs, ordered=False)
                track_albums_docs.clear()

    if artist_genres_docs:
        db.artist_genres.insert_many(artist_genres_docs, ordered=False)
    if album_artists_docs:
        db.album_artists.insert_many(album_artists_docs, ordered=False)
    if track_artists_docs:
        db.track_artists.insert_many(track_artists_docs, ordered=False)
    if track_albums_docs:
        db.track_albums.insert_many(track_albums_docs, ordered=False)


def seed_audio_features(db, track_ids: list[int], seed: Optional[int]) -> None:
    docs: list[dict] = []
    for track_id in track_ids:
        docs.append(
            {
                "track_id": track_id,
                "danceability": round(_u01(track_id, 12345), 3),
                "energy": round(_u01(track_id, 67890), 3),
                "key": int((track_id * 1103515245 + 11111) % 12),
                "mode": int((track_id * 1103515245 + 22222) % 2),
                "loudness": -round(((track_id * 1103515245 + 33333) % 35000) / 1000.0, 3),
                "speechiness": round(_u01(track_id, 44444), 3),
                "acousticness": round(_u01(track_id, 55555), 3),
                "instrumentalness": round(((track_id * 1103515245 + 66666) % 100000) / 100000.0, 5),
                "liveness": round(_u01(track_id, 77777), 3),
                "valence": round(_u01(track_id, 88888), 3),
                "tempo": round(60.0 + (((track_id * 1103515245 + 99999) % 12000) / 100.0), 2),
                "time_signature": [3, 4, 5][(track_id * 1103515245 + 13579) % 3],
            }
        )

        if len(docs) >= _BATCH_SIZE:
            db.audio_features.insert_many(docs, ordered=False)
            docs.clear()

    if docs:
        db.audio_features.insert_many(docs, ordered=False)


def seed_charts(db, market_ids: list[int]) -> list[int]:
    chart_ids: list[int] = []
    docs = []
    current_id = 1
    for market_id in market_ids:
        for name, chart_type in [("Top 50", "top"), ("Viral 50", "viral")]:
            docs.append(
                {
                    "chart_id": current_id,
                    "provider": "spotify",
                    "name": name,
                    "chart_type": chart_type,
                    "market_id": market_id,
                }
            )
            chart_ids.append(current_id)
            current_id += 1

    if docs:
        db.charts.insert_many(docs, ordered=False)
    return chart_ids


def seed_chart_entries(
    db,
    chart_ids: list[int],
    track_ids: list[int],
    seed: Optional[int],
    days: int = 7,
    top_n: int = 50,
) -> None:
    if not chart_ids or not track_ids:
        return

    rng = random.Random(seed)
    docs = []
    chart_entry_id = 1
    start = date.today() - timedelta(days=days)

    for chart_id in chart_ids:
        for d in range(days):
            chart_date = dt_utc(start + timedelta(days=d))
            chosen = rng.sample(track_ids, k=min(len(track_ids), top_n))
            for pos, track_id in enumerate(chosen, start=1):
                docs.append(
                    {
                        "chart_entry_id": chart_entry_id,
                        "chart_id": chart_id,
                        "track_id": track_id,
                        "chart_date": chart_date,
                        "position": pos,
                        "streams": rng.randint(10_000, 5_000_000),
                    }
                )
                chart_entry_id += 1

    if docs:
        db.chart_entries.insert_many(docs, ordered=False)


def seed_all(
    db,
    n_genres: int,
    n_artists: int,
    n_albums: int,
    n_tracks: int,
    seed: Optional[int] = None,
    truncate: bool = False,
    pool_size: int = 10000,
) -> None:
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    ensure_schema(db)
    if truncate:
        truncate_all(db)

    genre_ids = seed_genres(db, fake, n=n_genres)
    market_ids = seed_markets(db)
    artist_ids = seed_artists(db, fake, n=n_artists, pool_size=pool_size, seed=seed)
    album_ids = seed_albums(db, fake, n=n_albums, pool_size=pool_size, seed=seed)
    track_ids = seed_tracks(db, fake, n=n_tracks, pool_size=pool_size, seed=seed)

    seed_relations(db, artist_ids, genre_ids, album_ids, track_ids)
    seed_audio_features(db, track_ids, seed=seed)

    chart_ids = seed_charts(db, market_ids)
    seed_chart_entries(db, chart_ids, track_ids[:5000], seed=seed)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the MongoDB spotify database with fake data using Faker.")

    parser.add_argument("--genres", type=int, default=30)
    parser.add_argument("--artists", type=int, default=50)
    parser.add_argument("--albums", type=int, default=80)
    parser.add_argument("--tracks", type=int, default=200)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete all spotify collections before seeding.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=10000,
        help="Size of Faker-generated name pools (higher = more variety, slower startup).",
    )

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "27018")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))
    parser.add_argument("--db-auth-source", default=os.getenv("DB_AUTH_SOURCE", "spotify"))

    args = parser.parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        auth_source=args.db_auth_source,
    )

    client, db = connect_db(cfg)
    try:
        seed_all(
            db,
            n_genres=args.genres,
            n_artists=args.artists,
            n_albums=args.albums,
            n_tracks=args.tracks,
            seed=args.seed,
            truncate=args.truncate,
            pool_size=args.pool_size,
        )
    finally:
        client.close()

    print("Seeding completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
