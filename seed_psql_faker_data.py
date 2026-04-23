import argparse
import os
import random
import string
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from faker import Faker


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def connect_db(cfg: DbConfig):
    import psycopg

    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.execute("SET search_path TO spotify, public;")
    return conn


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
    # Deterministic 22-char ID per row. No in-memory uniqueness tracking.
    raw = base_n_encode(i, _BASE62)
    return (namespace + raw.rjust(21, _BASE62[0]))[:22]


def isrc_from_int(i: int, year: int = 25) -> str:
    # 12-char ISRC-like code: CCXXXYYNNNNN
    # For 1,000,000 rows we spread across multiple registrant codes:
    # registrant changes every 100000, serial is 00000-99999.
    country = "PL"
    registrant_idx = i // 100000
    registrant = base_n_encode(registrant_idx, _BASE36).rjust(3, "0")[:3]
    serial = i % 100000
    return f"{country}{registrant}{year:02d}{serial:05d}"


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'spotify' AND table_name = 'tracks'
            """
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                "Schema spotify.tracks not found. Start DB with docker-compose so init.sql runs."
            )


def truncate_all(conn) -> None:
    # Fast reset for re-seeding; avoids COPY failing on unique constraints.
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
              chart_entries,
              charts,
              audio_features,
              track_albums,
              track_artists,
              album_artists,
              artist_genres,
              tracks,
              albums,
              artists,
              markets,
              genres
            RESTART IDENTITY CASCADE
            """
        )
    conn.commit()


def seed_genres(conn, fake: Faker, n: int) -> None:
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

    with conn.cursor() as cur:
        for name in base[:n]:
            cur.execute(
                """
                INSERT INTO genres (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                """,
                (name[:100],),
            )
    conn.commit()


def seed_markets(conn) -> None:
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
    with conn.cursor() as cur:
        for code, name in markets:
            cur.execute(
                """
                INSERT INTO markets (country_code, name)
                VALUES (%s, %s)
                ON CONFLICT (country_code) DO UPDATE SET name = EXCLUDED.name
                """,
                (code, name),
            )
    conn.commit()


def seed_artists(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    name_pool = [fake.name()[:255] for _ in range(max(100, min(pool_size, 5000)))]
    genre_word_pool = [fake.word().lower() for _ in range(2000)]

    with conn.cursor() as cur:
        with cur.copy("COPY artists (name, raw_genres_text) FROM STDIN") as copy:
            for i in range(n):
                name = name_pool[i % len(name_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
                raw_genres_text = ", ".join(
                    {genre_word_pool[rng.randrange(len(genre_word_pool))] for _ in range(rng.randint(1, 4))}
                )[:500]
                raw_genres_text = raw_genres_text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                copy.write_row((f"{name} {i}" if n > len(name_pool) else name, raw_genres_text))
    conn.commit()


def seed_albums(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 5)).rstrip(".")[:255]
        for _ in range(max(200, min(pool_size, 10000)))
    ]

    with conn.cursor() as cur:
        with cur.copy(
            "COPY albums (spotify_album_id, name, album_type, release_date, total_tracks) FROM STDIN"
        ) as copy:
            for i in range(n):
                spotify_album_id = spotify_id_from_int(i, "a")
                name = title_pool[i % len(title_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
                album_type = rng.choice(["album", "single", "compilation", "ep", None])
                release = fake.date_between(date(2009, 1, 1), date(2025, 12, 31))
                total_tracks = rng.randint(1, 30)
                copy.write_row((spotify_album_id, name, album_type, str(release), total_tracks))
    conn.commit()


def seed_tracks(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    # Generate a small pool of Faker-made titles to keep it fast.
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 6)).rstrip(".")[:255]
        for _ in range(max(500, min(pool_size, 20000)))
    ]

    with conn.cursor() as cur:
        with cur.copy(
            "COPY tracks (spotify_track_id, name, explicit, duration_min, disc_number, track_number, isrc) FROM STDIN"
        ) as copy:
            for i in range(n):
                spotify_track_id = spotify_id_from_int(i, "t")
                name = title_pool[i % len(title_pool)].replace("\t", " ").replace("\r", " ").replace("\n", " ")
                explicit = rng.choice([True, False])
                duration_min = round(rng.uniform(1.0, 9.5), 3)
                disc_number = rng.choice([1, 2])
                track_number = rng.randint(1, 20)
                isrc = isrc_from_int(i, year=25)
                copy.write_row(
                    (
                        spotify_track_id,
                        name,
                        explicit,
                        duration_min,
                        disc_number,
                        track_number,
                        isrc,
                    )
                )
    conn.commit()


def _id_span(cur, table: str, id_col: str) -> tuple[int, int, int]:
    cur.execute(f"SELECT min({id_col}), max({id_col}), count(*) FROM {table}")
    mn, mx, cnt = cur.fetchone()
    if cnt == 0:
        return (0, -1, 0)
    return (int(mn), int(mx), int(cnt))


def seed_relations_fast(
    conn,
    genres_per_artist: int = 2,
    artists_per_album: int = 1,
    artists_per_track: int = 2,
) -> None:
    with conn.cursor() as cur:
        min_genre, _, genre_count = _id_span(cur, "genres", "genre_id")
        min_artist, _, artist_count = _id_span(cur, "artists", "artist_id")
        min_album, _, album_count = _id_span(cur, "albums", "album_id")
        min_track, _, track_count = _id_span(cur, "tracks", "track_id")

        if genre_count and artist_count:
            cur.execute(
                """
                INSERT INTO artist_genres (artist_id, genre_id)
                SELECT a.artist_id,
                       ((a.artist_id - %s + gs - 1) %% %s) + %s
                FROM artists a
                CROSS JOIN generate_series(1, %s) gs
                ON CONFLICT DO NOTHING
                """,
                (min_artist, genre_count, min_genre, max(1, genres_per_artist)),
            )

        if album_count and artist_count:
            cur.execute(
                """
                INSERT INTO album_artists (album_id, artist_id, artist_order)
                SELECT al.album_id,
                       ((al.album_id - %s + gs - 1) %% %s) + %s,
                       gs
                FROM albums al
                CROSS JOIN generate_series(1, %s) gs
                ON CONFLICT DO NOTHING
                """,
                (min_album, artist_count, min_artist, max(1, artists_per_album)),
            )

        if track_count and artist_count:
            cur.execute(
                """
                INSERT INTO track_artists (track_id, artist_id, artist_order)
                SELECT t.track_id,
                       ((t.track_id - %s + gs - 1) %% %s) + %s,
                       gs
                FROM tracks t
                CROSS JOIN generate_series(1, %s) gs
                ON CONFLICT DO NOTHING
                """,
                (min_track, artist_count, min_artist, max(1, artists_per_track)),
            )

        if track_count and album_count:
            cur.execute(
                """
                INSERT INTO track_albums (track_id, album_id, is_primary)
                SELECT t.track_id,
                       ((t.track_id - %s) %% %s) + %s,
                       TRUE
                FROM tracks t
                ON CONFLICT DO NOTHING
                """,
                (min_track, album_count, min_album),
            )
    conn.commit()


def seed_audio_features_fast(conn) -> None:
    """Seed spotify.audio_features for tracks that don't have it yet.

    This can be a bottleneck for large scales. After TRUNCATE + reseed, track_id
    can be large (500k..10M). We keep this a *single* SQL statement, avoid
    `COUNT(*)` scans, avoid `ORDER BY random()`, and avoid per-row `random()` calls.
    Values are synthetic, deterministic, and derived from track_id.
    """

    # Bulk insert: reduce commit latency (acceptable for synthetic seeding).
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SET LOCAL synchronous_commit TO off")
            cur.execute("SET LOCAL jit TO off")

            cur.execute("SELECT 1 FROM tracks LIMIT 1")
            if cur.fetchone() is None:
                return

            cur.execute("SELECT 1 FROM audio_features LIMIT 1")
            audio_features_empty = cur.fetchone() is None

            # Deterministic pseudo-random values derived from track_id.
            # This is dramatically faster than calling random() 10+ times per row.
            sql_body = """
                INSERT INTO audio_features (
                    track_id, danceability, energy, key, mode, loudness, speechiness,
                    acousticness, instrumentalness, liveness, valence, tempo, time_signature
                )
                SELECT t.track_id,
                       round((( (t.track_id::bigint * 1103515245 + 12345)  % 1000 )::numeric) / 1000, 3),
                       round((( (t.track_id::bigint * 1103515245 + 67890)  % 1000 )::numeric) / 1000, 3),
                       ((t.track_id::bigint * 1103515245 + 11111) % 12)::smallint,
                       ((t.track_id::bigint * 1103515245 + 22222) % 2)::smallint,
                       -round((( (t.track_id::bigint * 1103515245 + 33333) % 35000 )::numeric) / 1000, 3),
                       round((( (t.track_id::bigint * 1103515245 + 44444)  % 1000 )::numeric) / 1000, 3),
                       round((( (t.track_id::bigint * 1103515245 + 55555)  % 1000 )::numeric) / 1000, 3),
                       round((( (t.track_id::bigint * 1103515245 + 66666)  % 100000 )::numeric) / 100000, 5),
                       round((( (t.track_id::bigint * 1103515245 + 77777)  % 1000 )::numeric) / 1000, 3),
                       round((( (t.track_id::bigint * 1103515245 + 88888)  % 1000 )::numeric) / 1000, 3),
                       round((60.0 + (((t.track_id::bigint * 1103515245 + 99999) % 12000)::numeric) / 100), 2),
                       (ARRAY[3,4,5])[1 + (((t.track_id::bigint * 1103515245 + 13579) % 3)::int)]::smallint
                FROM tracks t
            """

            if audio_features_empty:
                cur.execute(sql_body)
            else:
                cur.execute(
                    sql_body
                    + """
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM audio_features af
                        WHERE af.track_id = t.track_id
                    )
                    """
                )


def seed_charts(conn, market_ids: list[int]) -> list[int]:
    chart_ids: list[int] = []
    with conn.cursor() as cur:
        for market_id in market_ids:
            for name, chart_type in [
                ("Top 50", "top"),
                ("Viral 50", "viral"),
            ]:
                cur.execute(
                    """
                    INSERT INTO charts (provider, name, chart_type, market_id)
                    VALUES ('spotify', %s, %s, %s)
                    ON CONFLICT (provider, name, market_id) DO UPDATE SET
                      chart_type = COALESCE(EXCLUDED.chart_type, charts.chart_type)
                    RETURNING chart_id
                    """,
                    (name, chart_type, market_id),
                )
                chart_ids.append(int(cur.fetchone()[0]))
    conn.commit()
    return chart_ids


def seed_chart_entries(conn, chart_ids: list[int], track_ids: list[int], days: int = 7, top_n: int = 50) -> None:
    if not track_ids:
        return

    start = date.today() - timedelta(days=days)
    with conn.cursor() as cur:
        for chart_id in chart_ids:
            for d in range(days):
                chart_date = start + timedelta(days=d)
                chosen = random.sample(track_ids, k=min(len(track_ids), top_n))
                for pos, track_id in enumerate(chosen, start=1):
                    cur.execute(
                        """
                        INSERT INTO chart_entries (chart_id, track_id, chart_date, position, streams)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (chart_id, track_id, chart_date) DO NOTHING
                        """,
                        (
                            chart_id,
                            track_id,
                            chart_date,
                            pos,
                            random.randint(10_000, 5_000_000),
                        ),
                    )
    conn.commit()


def seed_all(
    conn,
    n_genres: int,
    n_artists: int,
    n_albums: int,
    n_tracks: int,
    seed: Optional[int] = None,
    truncate: bool = False,
    pool_size: int = 1000,
    include_audio_features: bool = True,
) -> None:
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    ensure_schema(conn)
    if truncate:
        truncate_all(conn)

    seed_genres(conn, fake, n=n_genres)
    seed_markets(conn)
    seed_artists(conn, fake, n=n_artists, pool_size=pool_size, seed=seed)
    seed_albums(conn, fake, n=n_albums, pool_size=pool_size, seed=seed)
    seed_tracks(conn, fake, n=n_tracks, pool_size=pool_size, seed=seed)

    seed_relations_fast(conn)
    if include_audio_features:
        seed_audio_features_fast(conn)

    # Charts are tiny; keep simple row-by-row approach.
    with conn.cursor() as cur:
        cur.execute("SELECT market_id FROM markets ORDER BY market_id")
        market_ids = [int(r[0]) for r in cur.fetchall()]
        cur.execute("SELECT track_id FROM tracks ORDER BY track_id LIMIT 5000")
        track_ids = [int(r[0]) for r in cur.fetchall()]
    chart_ids = seed_charts(conn, market_ids)
    seed_chart_entries(conn, chart_ids, track_ids)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the spotify schema with fake data using Faker.")

    parser.add_argument("--genres", type=int, default=30)
    parser.add_argument("--artists", type=int, default=50)
    parser.add_argument("--albums", type=int, default=80)
    parser.add_argument("--tracks", type=int, default=200)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="TRUNCATE all spotify tables before seeding (recommended for big loads)",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=10000,
        help="Size of Faker-generated name pools (higher = more variety, slower startup)",
    )
    parser.add_argument(
        "--skip-audio-features",
        action="store_true",
        help="Skip seeding spotify.audio_features (faster; only use if your benchmarks don't need it)",
    )

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "5434")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify_db"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

    args = parser.parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    with connect_db(cfg) as conn:
        seed_all(
            conn,
            n_genres=args.genres,
            n_artists=args.artists,
            n_albums=args.albums,
            n_tracks=args.tracks,
            seed=args.seed,
            truncate=args.truncate,
            pool_size=args.pool_size,
            include_audio_features=not args.skip_audio_features,
        )

    print("Seeding completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
