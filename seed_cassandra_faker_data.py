import argparse
import os
import random
import string
import sys
import time as pytime
from itertools import islice
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Optional, Sequence

from faker import Faker


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    keyspace: str
    user: Optional[str]
    password: Optional[str]


@dataclass(frozen=True)
class WriteTuning:
    concurrency: int = 300
    chunk_size: int = 20000
    progress_every: int = 200000


def _prepare_cassandra_runtime() -> None:
    # cassandra-driver 3.x expects asyncore by default.
    # Python 3.12 removed it, so use the pyasyncore backport when available.
    if sys.version_info < (3, 12):
        return

    try:
        import asyncore  # type: ignore # provided by pyasyncore on Python 3.12+

        _ = asyncore
        return
    except ImportError:
        pass

    # Fallback: gevent reactor can also work when asyncore backport is not installed.
    try:
        from gevent import monkey

        if not monkey.is_module_patched("socket"):
            monkey.patch_all()
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "Python 3.12 with cassandra-driver requires an event-loop backend. "
            "Install one of: pip install pyasyncore (recommended) OR pip install gevent"
        ) from e



def connect_db(cfg: DbConfig):
    _prepare_cassandra_runtime()

    try:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "cassandra-driver is required for Cassandra seeding. Install it with: pip install cassandra-driver"
        ) from e

    auth_provider = None
    if cfg.user:
        auth_provider = PlainTextAuthProvider(username=cfg.user, password=cfg.password or "")

    cluster = Cluster([cfg.host], port=cfg.port, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(cfg.keyspace)
    return cluster, session


_BASE62 = string.digits + string.ascii_letters
_BASE36 = string.digits + string.ascii_uppercase


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


def _decimal(num: float, places: int) -> Decimal:
    return Decimal(f"{num:.{places}f}")


def ensure_schema(session, keyspace: str) -> None:
    row = session.execute(
        """
        SELECT table_name
        FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = 'tracks'
        """,
        (keyspace,),
    ).one()
    if row is None:
        raise RuntimeError(
            "Schema not found (missing spotify.tracks table). Start DB with docker-compose so init.cassandra.cql runs."
        )


def insert_many(
    session,
    cql: str,
    rows: Iterable[Sequence[object]],
    tuning: WriteTuning,
    label: str,
) -> int:
    try:
        from cassandra.concurrent import execute_concurrent_with_args
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("Missing cassandra-driver") from e

    iterator = iter(rows)
    statement = session.prepare(cql)
    loaded = 0
    started = pytime.perf_counter()

    while True:
        batch = list(islice(iterator, tuning.chunk_size))
        if not batch:
            break
        execute_concurrent_with_args(
            session,
            statement,
            batch,
            concurrency=tuning.concurrency,
            raise_on_first_error=True,
        )
        loaded += len(batch)
        if tuning.progress_every > 0 and loaded % tuning.progress_every == 0:
            elapsed = pytime.perf_counter() - started
            print(f"{label}: {loaded} rows ({elapsed:.1f}s)")

    elapsed = pytime.perf_counter() - started
    print(f"{label}: done {loaded} rows ({elapsed:.1f}s)")

    return loaded


def truncate_all(session) -> None:
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
        session.execute(f"TRUNCATE {table}")


def seed_genres(session, fake: Faker, n: int, tuning: WriteTuning) -> int:
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

    now = datetime.now(tz=timezone.utc)
    rows = [(i, name[:100], now) for i, name in enumerate(base[:n], start=1)]
    insert_many(session, "INSERT INTO genres (genre_id, name, created_at) VALUES (?, ?, ?)", rows, tuning, "genres")
    return n


def seed_markets(session, tuning: WriteTuning) -> int:
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

    rows = []
    for i, (code, name) in enumerate(markets, start=1):
        rows.append((i, code, name))

    insert_many(session, "INSERT INTO markets (market_id, country_code, name) VALUES (?, ?, ?)", rows, tuning, "markets")
    return len(markets)


def seed_artists(
    session,
    fake: Faker,
    n: int,
    pool_size: int,
    seed: Optional[int],
    tuning: WriteTuning,
) -> int:
    rng = random.Random(seed)
    name_pool = [fake.name()[:255] for _ in range(max(100, min(pool_size, 5000)))]
    genre_word_pool = [fake.word().lower() for _ in range(2000)]
    now = datetime.now(tz=timezone.utc)

    def rows() -> Iterable[Sequence[object]]:
        for i in range(1, n + 1):
            name = name_pool[(i - 1) % len(name_pool)]
            name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
            raw_genres_text = ", ".join(
                {genre_word_pool[rng.randrange(len(genre_word_pool))] for _ in range(rng.randint(1, 4))}
            )[:500]
            raw_genres_text = raw_genres_text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
            yield (i, f"{name} {i}" if n > len(name_pool) else name, raw_genres_text, now, now)

    insert_many(
        session,
        "INSERT INTO artists (artist_id, name, raw_genres_text, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        rows(),
        tuning,
        "artists",
    )
    return n


def seed_albums(
    session,
    fake: Faker,
    n: int,
    pool_size: int,
    seed: Optional[int],
    tuning: WriteTuning,
) -> int:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 5)).rstrip(".")[:255]
        for _ in range(max(200, min(pool_size, 10000)))
    ]
    now = datetime.now(tz=timezone.utc)

    def rows() -> Iterable[Sequence[object]]:
        for i in range(1, n + 1):
            spotify_album_id = spotify_id_from_int(i - 1, "a")
            name = title_pool[(i - 1) % len(title_pool)]
            name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
            album_type = rng.choice(["album", "single", "compilation", "ep", None])
            release = fake.date_between(date(2009, 1, 1), date(2025, 12, 31))
            total_tracks = rng.randint(1, 30)
            yield (i, spotify_album_id, name, album_type, release, total_tracks, now, now)

    insert_many(
        session,
        """
        INSERT INTO albums (
          album_id, spotify_album_id, name, album_type, release_date, total_tracks, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows(),
        tuning,
        "albums",
    )
    return n


def seed_tracks(
    session,
    fake: Faker,
    n: int,
    pool_size: int,
    seed: Optional[int],
    tuning: WriteTuning,
) -> int:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 6)).rstrip(".")[:255]
        for _ in range(max(500, min(pool_size, 20000)))
    ]
    now = datetime.now(tz=timezone.utc)

    def rows() -> Iterable[Sequence[object]]:
        decimal_fn = _decimal
        randint = rng.randint
        choice = rng.choice
        uniform = rng.uniform
        for i in range(1, n + 1):
            spotify_track_id = spotify_id_from_int(i - 1, "t")
            name = title_pool[(i - 1) % len(title_pool)]
            name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
            yield (
                i,
                spotify_track_id,
                name,
                choice([True, False]),
                decimal_fn(uniform(1.0, 9.5), 3),
                choice([1, 2]),
                randint(1, 20),
                isrc_from_int(i - 1, year=25),
                now,
                now,
            )

    insert_many(
        session,
        """
        INSERT INTO tracks (
          track_id, spotify_track_id, name, explicit, duration_min,
          disc_number, track_number, isrc, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows(),
        tuning,
        "tracks",
    )
    return n


def seed_relations(
    session,
    n_artists: int,
    n_genres: int,
    n_albums: int,
    n_tracks: int,
    tuning: WriteTuning,
    genres_per_artist: int = 2,
    artists_per_album: int = 1,
    artists_per_track: int = 2,
) -> None:
    if n_artists > 0 and n_genres > 0:
        def artist_genres_rows() -> Iterable[Sequence[object]]:
            for artist_id in range(1, n_artists + 1):
                for gs in range(1, max(1, genres_per_artist) + 1):
                    genre_id = ((artist_id + gs - 2) % n_genres) + 1
                    yield (artist_id, genre_id)

        insert_many(
            session,
            "INSERT INTO artist_genres (artist_id, genre_id) VALUES (?, ?)",
            artist_genres_rows(),
            tuning,
            "artist_genres",
        )

    if n_albums > 0 and n_artists > 0:
        def album_artists_rows() -> Iterable[Sequence[object]]:
            for album_id in range(1, n_albums + 1):
                for gs in range(1, max(1, artists_per_album) + 1):
                    artist_id = ((album_id + gs - 2) % n_artists) + 1
                    yield (album_id, artist_id, gs)

        insert_many(
            session,
            "INSERT INTO album_artists (album_id, artist_id, artist_order) VALUES (?, ?, ?)",
            album_artists_rows(),
            tuning,
            "album_artists",
        )

    if n_tracks > 0 and n_artists > 0:
        def track_artists_rows() -> Iterable[Sequence[object]]:
            for track_id in range(1, n_tracks + 1):
                for gs in range(1, max(1, artists_per_track) + 1):
                    artist_id = ((track_id + gs - 2) % n_artists) + 1
                    yield (track_id, artist_id, gs)

        insert_many(
            session,
            "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (?, ?, ?)",
            track_artists_rows(),
            tuning,
            "track_artists",
        )

    if n_tracks > 0 and n_albums > 0:
        def track_albums_rows() -> Iterable[Sequence[object]]:
            for track_id in range(1, n_tracks + 1):
                album_id = ((track_id - 1) % n_albums) + 1
                yield (track_id, album_id, True)

        insert_many(
            session,
            "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (?, ?, ?)",
            track_albums_rows(),
            tuning,
            "track_albums",
        )


def seed_audio_features(session, n_tracks: int, seed: Optional[int], tuning: WriteTuning) -> None:
    if n_tracks <= 0:
        return

    rng = random.Random(seed)

    def rows() -> Iterable[Sequence[object]]:
        decimal_fn = _decimal
        rand = rng.random
        randint = rng.randint
        choice = rng.choice
        for track_id in range(1, n_tracks + 1):
            yield (
                track_id,
                decimal_fn(rand(), 3),
                decimal_fn(rand(), 3),
                randint(0, 11),
                randint(0, 1),
                decimal_fn(-rand() * 35.0, 3),
                decimal_fn(rand(), 3),
                decimal_fn(rand(), 3),
                decimal_fn(rand(), 5),
                decimal_fn(rand(), 3),
                decimal_fn(rand(), 3),
                decimal_fn(60.0 + rand() * 120.0, 2),
                choice([3, 4, 5]),
            )

    insert_many(
        session,
        """
        INSERT INTO audio_features (
          track_id, danceability, energy, "key", mode, loudness, speechiness,
          acousticness, instrumentalness, liveness, valence, tempo, time_signature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows(),
        tuning,
        "audio_features",
    )


def seed_charts(session, n_markets: int, tuning: WriteTuning) -> list[int]:
    rows = []
    chart_ids: list[int] = []
    current_id = 1
    for market_id in range(1, n_markets + 1):
        for name, chart_type in [("Top 50", "top"), ("Viral 50", "viral")]:
            rows.append((current_id, "spotify", name, chart_type, market_id))
            chart_ids.append(current_id)
            current_id += 1

    insert_many(
        session,
        "INSERT INTO charts (chart_id, provider, name, chart_type, market_id) VALUES (?, ?, ?, ?, ?)",
        rows,
        tuning,
        "charts",
    )
    return chart_ids


def seed_chart_entries(
    session,
    chart_ids: list[int],
    chart_track_pool: int,
    seed: Optional[int],
    tuning: WriteTuning,
    days: int = 7,
    top_n: int = 50,
) -> None:
    if not chart_ids or chart_track_pool <= 0:
        return

    rng = random.Random(seed)
    start = date.today() - timedelta(days=days)
    def rows() -> Iterable[Sequence[object]]:
        chart_entry_id = 1
        sample_size = min(chart_track_pool, top_n)
        track_range = range(1, chart_track_pool + 1)
        for chart_id in chart_ids:
            for d in range(days):
                chart_date = start + timedelta(days=d)
                chosen = rng.sample(track_range, k=sample_size)
                for pos, track_id in enumerate(chosen, start=1):
                    yield (chart_id, chart_date, track_id, chart_entry_id, pos, rng.randint(10_000, 5_000_000))
                    chart_entry_id += 1

    insert_many(
        session,
        """
        INSERT INTO chart_entries (chart_id, chart_date, track_id, chart_entry_id, position, streams)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows(),
        tuning,
        "chart_entries",
    )


def seed_all(
    session,
    keyspace: str,
    n_genres: int,
    n_artists: int,
    n_albums: int,
    n_tracks: int,
    seed: Optional[int] = None,
    truncate: bool = False,
    pool_size: int = 10000,
    write_tuning: WriteTuning = WriteTuning(),
    fast_mode: bool = False,
) -> None:
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    ensure_schema(session, keyspace)
    if truncate:
        truncate_all(session)

    genre_count = seed_genres(session, fake, n=n_genres, tuning=write_tuning)
    market_count = seed_markets(session, tuning=write_tuning)
    artist_count = seed_artists(session, fake, n=n_artists, pool_size=pool_size, seed=seed, tuning=write_tuning)
    album_count = seed_albums(session, fake, n=n_albums, pool_size=pool_size, seed=seed, tuning=write_tuning)
    track_count = seed_tracks(session, fake, n=n_tracks, pool_size=pool_size, seed=seed, tuning=write_tuning)

    artists_per_track = 1 if fast_mode else 2
    seed_relations(
        session,
        n_artists=artist_count,
        n_genres=genre_count,
        n_albums=album_count,
        n_tracks=track_count,
        tuning=write_tuning,
        artists_per_track=artists_per_track,
    )
    if not fast_mode:
        seed_audio_features(session, n_tracks=track_count, seed=seed, tuning=write_tuning)

    chart_ids = seed_charts(session, market_count, tuning=write_tuning)
    if not fast_mode:
        seed_chart_entries(
            session,
            chart_ids,
            chart_track_pool=min(track_count, 5000),
            seed=seed,
            tuning=write_tuning,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the Cassandra spotify keyspace with fake data using Faker.")

    parser.add_argument("--genres", type=int, default=30)
    parser.add_argument("--artists", type=int, default=50)
    parser.add_argument("--albums", type=int, default=80)
    parser.add_argument("--tracks", type=int, default=200)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="TRUNCATE all spotify tables before seeding.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=10000,
        help="Size of Faker-generated name pools (higher = more variety, slower startup).",
    )
    parser.add_argument(
        "--write-concurrency",
        type=int,
        default=300,
        help="Concurrent in-flight writes per chunk (higher can be faster, but too high may overload DB).",
    )
    parser.add_argument(
        "--write-chunk-size",
        type=int,
        default=20000,
        help="Rows sent in one chunk to execute_concurrent_with_args.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=200000,
        help="Progress log interval per table (rows). Set 0 to disable progress logs.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Faster mode: fewer heavy writes (artists_per_track=1, skips audio_features and chart_entries).",
    )

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "9043")))
    parser.add_argument("--db-keyspace", default=os.getenv("DB_KEYSPACE", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))

    args = parser.parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user or None,
        password=args.db_password or None,
    )
    write_tuning = WriteTuning(
        concurrency=max(1, args.write_concurrency),
        chunk_size=max(1000, args.write_chunk_size),
        progress_every=max(0, args.progress_every),
    )

    cluster = None
    session = None
    try:
        cluster, session = connect_db(cfg)
        seed_all(
            session,
            keyspace=cfg.keyspace,
            n_genres=args.genres,
            n_artists=args.artists,
            n_albums=args.albums,
            n_tracks=args.tracks,
            seed=args.seed,
            truncate=args.truncate,
            pool_size=args.pool_size,
            write_tuning=write_tuning,
            fast_mode=args.fast,
        )
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()

    print("Seeding completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

