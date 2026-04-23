import argparse
import os
import random
import string
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence

from faker import Faker


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def connect_db(cfg: DbConfig):
    try:
        import pymysql
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "PyMySQL is required for MariaDB seeding. Install it with: pip install PyMySQL"
        ) from e

    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.dbname,
        autocommit=False,
        charset="utf8mb4",
        local_infile=True,
    )


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


def ensure_schema(conn, dbname: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = 'tracks'
            """,
            (dbname,),
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                "Schema not found (missing tracks table). Start DB with docker-compose so init.mariadb.sql runs."
            )


def _escape_tsv(value) -> str:
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return s


def _write_tsv(path: Path, rows: Iterable[Sequence[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write("\t".join(_escape_tsv(v) for v in row))
            f.write("\n")


def _load_local_infile(conn, table: str, columns: list[str], tsv_path: Path) -> int:
    # Use forward slashes for MySQL string literal stability on Windows.
    filename = str(tsv_path.resolve()).replace("\\", "/")
    cols = ", ".join(columns)
    sql = f"""
    LOAD DATA LOCAL INFILE '{filename}'
    INTO TABLE {table}
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY '\t' ESCAPED BY '\\\\'
    LINES TERMINATED BY '\n'
    ({cols})
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        loaded = int(cur.rowcount)
        msg = getattr(getattr(cur, "_result", None), "message", None)
        if isinstance(msg, (bytes, bytearray)):
            msg = msg.decode("utf-8", errors="replace")
        if msg:
            print(f"LOAD DATA {table}: {msg}")

        # If there were warnings, show a small sample.
        # (SHOW WARNINGS returns only the last statement's warnings.)
        cur.execute("SHOW WARNINGS LIMIT 5")
        warnings = cur.fetchall()
        if warnings:
            print(f"LOAD DATA {table}: first warnings:")
            for level, code, message in warnings:
                print(f"  - {level} {code}: {message}")

        return loaded


def truncate_all(conn) -> None:
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

    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in tables:
            cur.execute(f"TRUNCATE TABLE {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
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
                "INSERT IGNORE INTO genres (name) VALUES (%s)",
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
        # NULL values don't participate in UNIQUE constraints in MariaDB,
        # so handle the "Global" row explicitly.
        cur.execute("SELECT market_id FROM markets WHERE country_code IS NULL LIMIT 1")
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO markets (country_code, name) VALUES (NULL, %s)", ("Global",))
        else:
            cur.execute("UPDATE markets SET name=%s WHERE market_id=%s", ("Global", row[0]))

        for code, name in markets:
            if code is None:
                continue
            cur.execute(
                """
                INSERT INTO markets (country_code, name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE name = VALUES(name)
                """,
                (code, name),
            )
    conn.commit()


def seed_artists(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    name_pool = [fake.name()[:255] for _ in range(max(100, min(pool_size, 5000)))]
    genre_word_pool = [fake.word().lower() for _ in range(2000)]

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "artists.tsv"

        def rows():
            for i in range(n):
                name = name_pool[i % len(name_pool)]
                name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                raw_genres_text = ", ".join(
                    {genre_word_pool[rng.randrange(len(genre_word_pool))] for _ in range(rng.randint(1, 4))}
                )[:500]
                raw_genres_text = raw_genres_text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                yield (f"{name} {i}" if n > len(name_pool) else name, raw_genres_text)

        _write_tsv(path, rows())
        loaded = _load_local_infile(conn, "artists", ["name", "raw_genres_text"], path)
        if loaded != n:
            print(f"Loaded artists: {loaded}/{n}")
    conn.commit()


def seed_albums(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 5)).rstrip(".")[:255]
        for _ in range(max(200, min(pool_size, 10000)))
    ]

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "albums.tsv"

        def rows():
            for i in range(n):
                spotify_album_id = spotify_id_from_int(i, "a")
                name = title_pool[i % len(title_pool)]
                name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                album_type = rng.choice(["album", "single", "compilation", "ep", None])
                release = fake.date_between(date(2009, 1, 1), date(2025, 12, 31))
                total_tracks = rng.randint(1, 30)
                yield (spotify_album_id, name, album_type, str(release), total_tracks)

        _write_tsv(path, rows())
        loaded = _load_local_infile(
            conn,
            "albums",
            ["spotify_album_id", "name", "album_type", "release_date", "total_tracks"],
            path,
        )
        if loaded != n:
            print(f"Loaded albums: {loaded}/{n}")
    conn.commit()


def seed_tracks(conn, fake: Faker, n: int, pool_size: int, seed: Optional[int]) -> None:
    rng = random.Random(seed)
    title_pool = [
        fake.sentence(nb_words=rng.randint(2, 6)).rstrip(".")[:255]
        for _ in range(max(500, min(pool_size, 20000)))
    ]

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "tracks.tsv"

        def rows():
            for i in range(n):
                spotify_track_id = spotify_id_from_int(i, "t")
                name = title_pool[i % len(title_pool)]
                name = name.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                explicit = rng.choice([True, False])
                duration_min = round(rng.uniform(1.0, 9.5), 3)
                disc_number = rng.choice([1, 2])
                track_number = rng.randint(1, 20)
                isrc = isrc_from_int(i, year=25)
                yield (
                    spotify_track_id,
                    name,
                    explicit,
                    duration_min,
                    disc_number,
                    track_number,
                    isrc,
                )

        _write_tsv(path, rows())
        loaded = _load_local_infile(
            conn,
            "tracks",
            [
                "spotify_track_id",
                "name",
                "explicit",
                "duration_min",
                "disc_number",
                "track_number",
                "isrc",
            ],
            path,
        )
        if loaded != n:
            print(
                f"Loaded tracks: {loaded}/{n} (if this is unexpected, verify your command line and whether you used --truncate)"
            )
    conn.commit()


def _id_span(conn, table: str, id_col: str) -> tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN({id_col}), MAX({id_col}), COUNT(*) FROM {table}")
        mn, mx, cnt = cur.fetchone()
    if cnt == 0:
        return (0, -1, 0)
    return (int(mn), int(mx), int(cnt))


def _numbers_derived_table_sql(n: int) -> str:
    if n <= 0:
        raise ValueError("n must be >= 1")
    # MariaDB-friendly, avoids WITH/CTE edge-cases and is plenty fast
    # for our small per-row multiplicities (1-3).
    parts = ["SELECT 1 AS n"]
    for i in range(2, n + 1):
        parts.append(f"UNION ALL SELECT {i}")
    return "(" + "\n".join(parts) + ") seq"


def seed_relations_fast(
    conn,
    genres_per_artist: int = 2,
    artists_per_album: int = 1,
    artists_per_track: int = 2,
) -> None:
    min_genre, _, genre_count = _id_span(conn, "genres", "genre_id")
    min_artist, _, artist_count = _id_span(conn, "artists", "artist_id")
    min_album, _, album_count = _id_span(conn, "albums", "album_id")
    min_track, _, track_count = _id_span(conn, "tracks", "track_id")

    with conn.cursor() as cur:
        if genre_count and artist_count:
            seq_sql = _numbers_derived_table_sql(max(1, genres_per_artist))
            cur.execute(
                f"""
                INSERT IGNORE INTO artist_genres (artist_id, genre_id)
                SELECT a.artist_id,
                       MOD(a.artist_id - %s + seq.n - 1, %s) + %s
                FROM artists a
                CROSS JOIN {seq_sql}
                """,
                (min_artist, genre_count, min_genre),
            )

        if album_count and artist_count:
            seq_sql = _numbers_derived_table_sql(max(1, artists_per_album))
            cur.execute(
                f"""
                INSERT IGNORE INTO album_artists (album_id, artist_id, artist_order)
                SELECT al.album_id,
                       MOD(al.album_id - %s + seq.n - 1, %s) + %s,
                       seq.n
                FROM albums al
                CROSS JOIN {seq_sql}
                """,
                (min_album, artist_count, min_artist),
            )

        if track_count and artist_count:
            seq_sql = _numbers_derived_table_sql(max(1, artists_per_track))
            cur.execute(
                f"""
                INSERT IGNORE INTO track_artists (track_id, artist_id, artist_order)
                SELECT t.track_id,
                       MOD(t.track_id - %s + seq.n - 1, %s) + %s,
                       seq.n
                FROM tracks t
                CROSS JOIN {seq_sql}
                """,
                (min_track, artist_count, min_artist),
            )

        if track_count and album_count:
            cur.execute(
                """
                INSERT IGNORE INTO track_albums (track_id, album_id, is_primary)
                SELECT t.track_id,
                       MOD(t.track_id - %s, %s) + %s,
                       TRUE
                FROM tracks t
                """,
                (min_track, album_count, min_album),
            )

    conn.commit()


def seed_audio_features_fast(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT IGNORE INTO audio_features (
              track_id, danceability, energy, `key`, mode, loudness, speechiness,
              acousticness, instrumentalness, liveness, valence, tempo, time_signature
            )
            SELECT t.track_id,
                   ROUND(MOD(t.track_id * 1103515245 + 12345, 1000) / 1000.0, 3),
                   ROUND(MOD(t.track_id * 1103515245 + 67890, 1000) / 1000.0, 3),
                   MOD(t.track_id * 1103515245 + 11111, 12),
                   MOD(t.track_id * 1103515245 + 22222, 2),
                   -ROUND(MOD(t.track_id * 1103515245 + 33333, 35000) / 1000.0, 3),
                   ROUND(MOD(t.track_id * 1103515245 + 44444, 1000) / 1000.0, 3),
                   ROUND(MOD(t.track_id * 1103515245 + 55555, 1000) / 1000.0, 3),
                   ROUND(MOD(t.track_id * 1103515245 + 66666, 100000) / 100000.0, 5),
                   ROUND(MOD(t.track_id * 1103515245 + 77777, 1000) / 1000.0, 3),
                   ROUND(MOD(t.track_id * 1103515245 + 88888, 1000) / 1000.0, 3),
                   ROUND(60.0 + MOD(t.track_id * 1103515245 + 99999, 12000) / 100.0, 2),
                   ELT(1 + MOD(t.track_id * 1103515245 + 13579, 3), 3, 4, 5)
            FROM tracks t
            LEFT JOIN audio_features af ON af.track_id = t.track_id
            WHERE af.track_id IS NULL
            """
        )
    conn.commit()


def seed_charts(conn, market_ids: list[int]) -> list[int]:
    chart_ids: list[int] = []
    with conn.cursor() as cur:
        for market_id in market_ids:
            for name, chart_type in [("Top 50", "top"), ("Viral 50", "viral")]:
                cur.execute(
                    """
                    INSERT INTO charts (provider, name, chart_type, market_id)
                    VALUES ('spotify', %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      chart_type = COALESCE(VALUES(chart_type), chart_type),
                      chart_id = LAST_INSERT_ID(chart_id)
                    """,
                    (name, chart_type, market_id),
                )
                chart_ids.append(int(cur.lastrowid))
    conn.commit()
    return chart_ids


def seed_chart_entries(
    conn,
    chart_ids: list[int],
    track_ids: list[int],
    days: int = 7,
    top_n: int = 50,
) -> None:
    if not track_ids:
        return

    start = date.today() - timedelta(days=days)
    with conn.cursor() as cur:
        for chart_id in chart_ids:
            for d in range(days):
                chart_date = start + timedelta(days=d)
                chosen = random.sample(track_ids, k=min(len(track_ids), top_n))
                rows = []
                for pos, track_id in enumerate(chosen, start=1):
                    rows.append(
                        (
                            chart_id,
                            track_id,
                            chart_date,
                            pos,
                            random.randint(10_000, 5_000_000),
                        )
                    )
                cur.executemany(
                    """
                    INSERT IGNORE INTO chart_entries (chart_id, track_id, chart_date, position, streams)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    rows,
                )
    conn.commit()


def seed_all(
    conn,
    dbname: str,
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

    ensure_schema(conn, dbname)
    if truncate:
        truncate_all(conn)

    seed_genres(conn, fake, n=n_genres)
    seed_markets(conn)
    seed_artists(conn, fake, n=n_artists, pool_size=pool_size, seed=seed)
    seed_albums(conn, fake, n=n_albums, pool_size=pool_size, seed=seed)
    seed_tracks(conn, fake, n=n_tracks, pool_size=pool_size, seed=seed)

    seed_relations_fast(conn)
    seed_audio_features_fast(conn)

    # Charts are tiny; keep simple approach.
    with conn.cursor() as cur:
        cur.execute("SELECT market_id FROM markets ORDER BY market_id")
        market_ids = [int(r[0]) for r in cur.fetchall()]
        cur.execute("SELECT track_id FROM tracks ORDER BY track_id LIMIT 5000")
        track_ids = [int(r[0]) for r in cur.fetchall()]

    chart_ids = seed_charts(conn, market_ids)
    seed_chart_entries(conn, chart_ids, track_ids)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the MariaDB spotify schema with fake data using Faker.")

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

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3307")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "user"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "user"))

    args = parser.parse_args()

    cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    conn = connect_db(cfg)
    try:
        seed_all(
            conn,
            dbname=cfg.dbname,
            n_genres=args.genres,
            n_artists=args.artists,
            n_albums=args.albums,
            n_tracks=args.tracks,
            seed=args.seed,
            truncate=args.truncate,
            pool_size=args.pool_size,
        )
        conn.commit()
    finally:
        conn.close()

    print("Seeding completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
