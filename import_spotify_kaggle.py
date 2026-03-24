import argparse
import ast
import csv
import importlib
import os
import random
import zlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Optional


_PSYCOPG: Any = None


def load_psycopg() -> Any:
    global _PSYCOPG
    if _PSYCOPG is not None:
        return _PSYCOPG
    try:
        _PSYCOPG = importlib.import_module("psycopg")
        return _PSYCOPG
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "Missing dependency 'psycopg'. Install it with: pip install -r requirements.txt"
        ) from e


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def pick(row: dict[str, Any], candidates: Iterable[str]) -> Optional[str]:
    for key in candidates:
        if key in row and not _is_empty(row[key]):
            return str(row[key]).strip()
    return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if _is_empty(value):
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def parse_smallint(value: Optional[str]) -> Optional[int]:
    parsed = parse_int(value)
    if parsed is None:
        return None
    if not (-32768 <= parsed <= 32767):
        return None
    return parsed


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if _is_empty(value):
        return None
    s = str(value).strip().lower()
    if s in {"true", "t", "1", "yes", "y"}:
        return True
    if s in {"false", "f", "0", "no", "n"}:
        return False
    return None


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if _is_empty(value):
        return None
    try:
        return Decimal(str(value).strip())
    except Exception:
        return None


def parse_date(value: Optional[str]) -> Optional[date]:
    if _is_empty(value):
        return None
    s = str(value).strip()
    try:
        # YYYY-MM-DD
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        # YYYY-MM
        if len(s) == 7 and s[4] == "-":
            y, m = s.split("-")
            return date(int(y), int(m), 1)
        # YYYY
        if len(s) == 4 and s.isdigit():
            return date(int(s), 1, 1)
    except Exception:
        return None
    return None


def parse_listish(value: Optional[str]) -> list[str]:
    """Parse values like: "['A', 'B']", "A, B", "A;B"."""
    if _is_empty(value):
        return []
    s = str(value).strip()

    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if not _is_empty(x)]
        except Exception:
            pass

    # Fallback split
    parts: list[str] = [s]
    for sep in [";", "|"]:
        if sep in s:
            parts = [p for chunk in parts for p in chunk.split(sep)]

    # Commas are common separators; try them last.
    if any("," in p for p in parts):
        parts = [p for chunk in parts for p in chunk.split(",")]

    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned


def generate_isrc(spotify_track_id: str, release_date: Optional[date]) -> str:
    """Generate a 12-character ISRC-like code: CCXXXYYNNNNN.

    This is not an official registrant code; it's a deterministic, unique-enough
    identifier suitable for a student DB with a UNIQUE constraint.
    """
    country = "PL"
    registrant = "ZZZ"
    year = release_date.year % 100 if release_date else 25
    base = zlib.crc32(spotify_track_id.encode("utf-8")) % 100000
    return f"{country}{registrant}{year:02d}{base:05d}"


def bump_isrc(isrc: str, salt: int) -> str:
    if len(isrc) != 12:
        return isrc
    prefix = isrc[:7]  # CC + XXX + YY
    try:
        num = int(isrc[7:])
    except Exception:
        return isrc
    num = (num + salt) % 100000
    return f"{prefix}{num:05d}"


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def connect_db(cfg: DbConfig) -> Any:
    psycopg = load_psycopg()
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.execute("SET search_path TO spotify, public;")
    return conn


def ensure_schema(conn: Any) -> None:
    # Light check: if core table doesn't exist, init.sql probably wasn't applied.
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


def upsert_artist(
    cur: Any,
    name: str,
    raw_genres_text: Optional[str],
) -> int:
    # Best-effort de-dup by artist name.
    cur.execute("SELECT artist_id FROM artists WHERE lower(name) = lower(%s) LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        artist_id = int(row[0])
        cur.execute(
            """
            UPDATE artists
            SET
              name = %s,
              raw_genres_text = COALESCE(%s, raw_genres_text),
              updated_at = now()
            WHERE artist_id = %s
            RETURNING artist_id
            """,
            (name, raw_genres_text, artist_id),
        )
        return int(cur.fetchone()[0])

    cur.execute(
        """
        INSERT INTO artists (name, raw_genres_text, updated_at)
        VALUES (%s, %s, now())
        RETURNING artist_id
        """,
        (name, raw_genres_text),
    )
    return int(cur.fetchone()[0])


def upsert_album(
    cur: Any,
    spotify_album_id: Optional[str],
    name: str,
    album_type: Optional[str],
    release_date: Optional[date],
    total_tracks: Optional[int],
) -> int:
    if spotify_album_id:
        cur.execute(
            """
            INSERT INTO albums (
                            spotify_album_id, name, album_type, release_date,
                            total_tracks, updated_at
            )
                        VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (spotify_album_id) DO UPDATE SET
              name = EXCLUDED.name,
              album_type = COALESCE(EXCLUDED.album_type, albums.album_type),
              release_date = COALESCE(EXCLUDED.release_date, albums.release_date),
              total_tracks = COALESCE(EXCLUDED.total_tracks, albums.total_tracks),
              updated_at = now()
            RETURNING album_id
            """,
            (
                spotify_album_id,
                name,
                album_type,
                release_date,
                total_tracks,
            ),
        )
        return int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT album_id
        FROM albums
        WHERE lower(name) = lower(%s)
          AND (%s IS NULL OR release_date = %s)
        LIMIT 1
        """,
        (name, release_date, release_date),
    )
    row = cur.fetchone()
    if row:
        album_id = int(row[0])
        cur.execute(
            """
            UPDATE albums
            SET
              name = %s,
              album_type = COALESCE(%s, album_type),
              release_date = COALESCE(%s, release_date),
              total_tracks = COALESCE(%s, total_tracks),
              updated_at = now()
            WHERE album_id = %s
            RETURNING album_id
            """,
            (
                name,
                album_type,
                release_date,
                total_tracks,
                album_id,
            ),
        )
        return int(cur.fetchone()[0])

    cur.execute(
        """
        INSERT INTO albums (
                    spotify_album_id, name, album_type, release_date,
                    total_tracks, updated_at
        )
                VALUES (NULL, %s, %s, %s, %s, now())
        RETURNING album_id
        """,
        (
            name,
            album_type,
            release_date,
            total_tracks,
        ),
    )
    return int(cur.fetchone()[0])


def upsert_track(
    cur: Any,
    spotify_track_id: str,
    name: str,
    explicit: Optional[bool],
    duration_min: Optional[Decimal],
    disc_number: Optional[int],
    track_number: Optional[int],
    isrc: Optional[str],
) -> int:
    psycopg = load_psycopg()
    unique_violation = getattr(getattr(psycopg, "errors", None), "UniqueViolation", None)

    base_isrc = isrc
    for attempt in range(0, 25):
        attempt_isrc = base_isrc if attempt == 0 else bump_isrc(base_isrc or "", attempt)

        cur.execute("SAVEPOINT sp_upsert_track")
        try:
            cur.execute(
                """
                INSERT INTO tracks (
                  spotify_track_id, name, explicit, duration_min, disc_number, track_number,
                  isrc, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (spotify_track_id) DO UPDATE SET
                  name = EXCLUDED.name,
                  explicit = COALESCE(EXCLUDED.explicit, tracks.explicit),
                  duration_min = COALESCE(EXCLUDED.duration_min, tracks.duration_min),
                  disc_number = COALESCE(EXCLUDED.disc_number, tracks.disc_number),
                  track_number = COALESCE(EXCLUDED.track_number, tracks.track_number),
                  isrc = COALESCE(tracks.isrc, EXCLUDED.isrc),
                  updated_at = now()
                RETURNING track_id
                """,
                (
                    spotify_track_id,
                    name,
                    explicit,
                    duration_min,
                    disc_number,
                    track_number,
                    attempt_isrc if attempt_isrc else None,
                ),
            )
            track_id = int(cur.fetchone()[0])
            cur.execute("RELEASE SAVEPOINT sp_upsert_track")
            return track_id
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT sp_upsert_track")
            cur.execute("RELEASE SAVEPOINT sp_upsert_track")
            if unique_violation is not None and isinstance(e, unique_violation):
                # Most likely ISRC collision (isrc has a UNIQUE constraint).
                continue
            raise

    raise RuntimeError("Failed to insert track due to repeated ISRC collisions")


def upsert_genre(cur: Any, name: str) -> int:
    cur.execute(
        """
        INSERT INTO genres (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING genre_id
        """,
        (name,),
    )
    return int(cur.fetchone()[0])


def add_artist_genre(cur: Any, artist_id: int, genre_id: int) -> None:
    cur.execute(
        """
        INSERT INTO artist_genres (artist_id, genre_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (artist_id, genre_id),
    )


def add_track_artist(
    cur: Any, track_id: int, artist_id: int, order: int
) -> None:
    cur.execute(
        """
        INSERT INTO track_artists (track_id, artist_id, artist_order)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (track_id, artist_id, order),
    )


def add_album_artist(
    cur: Any, album_id: int, artist_id: int, order: int
) -> None:
    cur.execute(
        """
        INSERT INTO album_artists (album_id, artist_id, artist_order)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (album_id, artist_id, order),
    )


def add_track_album(cur: Any, track_id: int, album_id: int) -> None:
    cur.execute(
        """
        INSERT INTO track_albums (track_id, album_id, is_primary)
        VALUES (%s, %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (track_id, album_id),
    )


def upsert_audio_features(cur: Any, track_id: int, features: dict[str, Any]) -> None:
    if not any(v is not None for v in features.values()):
        return

    cur.execute(
        """
        INSERT INTO audio_features (
          track_id, danceability, energy, key, mode, loudness, speechiness,
          acousticness, instrumentalness, liveness, valence, tempo, time_signature
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO UPDATE SET
          danceability = COALESCE(EXCLUDED.danceability, audio_features.danceability),
          energy = COALESCE(EXCLUDED.energy, audio_features.energy),
          key = COALESCE(EXCLUDED.key, audio_features.key),
          mode = COALESCE(EXCLUDED.mode, audio_features.mode),
          loudness = COALESCE(EXCLUDED.loudness, audio_features.loudness),
          speechiness = COALESCE(EXCLUDED.speechiness, audio_features.speechiness),
          acousticness = COALESCE(EXCLUDED.acousticness, audio_features.acousticness),
          instrumentalness = COALESCE(EXCLUDED.instrumentalness, audio_features.instrumentalness),
          liveness = COALESCE(EXCLUDED.liveness, audio_features.liveness),
          valence = COALESCE(EXCLUDED.valence, audio_features.valence),
          tempo = COALESCE(EXCLUDED.tempo, audio_features.tempo),
          time_signature = COALESCE(EXCLUDED.time_signature, audio_features.time_signature)
        """,
        (
            track_id,
            features["danceability"],
            features["energy"],
            features["key"],
            features["mode"],
            features["loudness"],
            features["speechiness"],
            features["acousticness"],
            features["instrumentalness"],
            features["liveness"],
            features["valence"],
            features["tempo"],
            features["time_signature"],
        ),
    )


def analyze_csv(path: str, limit: int = 10) -> None:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError("CSV has no header")

        print(f"Columns ({len(reader.fieldnames)}): {reader.fieldnames}")
        print("\nFirst rows:")

        rows: list[dict[str, Any]] = []
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(row)

        for idx, row in enumerate(rows, start=1):
            print(f"\n--- Row {idx} ---")
            # Print only non-empty fields to keep it readable.
            for k, v in row.items():
                if not _is_empty(v):
                    print(f"{k}: {v}")


def import_csv(conn: Any, path: str, commit_every: int = 500) -> None:
    track_id_keys = ["spotify_track_id", "track_id", "id", "trackid", "track_uri", "track uri"]
    track_name_keys = ["track_name", "track", "name", "track name"]

    artist_names_keys = ["artists", "artist_name", "artist", "artist name"]

    album_name_keys = ["album_name", "album", "album name"]
    album_id_keys = ["spotify_album_id", "album_id", "albumid", "album uri", "album_uri"]

    genres_keys = ["genres", "artist_genres", "artist genres", "genre"]

    explicit_keys = ["explicit"]
    duration_min_keys = ["track_duration_min", "duration_min", "duration (min)", "track duration min"]
    disc_number_keys = ["disc_number", "disc number"]
    track_number_keys = ["track_number", "track number"]
    isrc_keys = ["isrc"]

    album_type_keys = ["album_type", "album type"]
    release_date_keys = ["release_date", "album_release_date", "album release date"]
    total_tracks_keys = [
        "total_tracks",
        "total tracks",
        "album_total_tracks",
        "album total tracks",
        "album_total",
        "album total",
    ]

    audio_map = {
        "danceability": ["danceability"],
        "energy": ["energy"],
        "key": ["key"],
        "mode": ["mode"],
        "loudness": ["loudness"],
        "speechiness": ["speechiness"],
        "acousticness": ["acousticness"],
        "instrumentalness": ["instrumentalness"],
        "liveness": ["liveness"],
        "valence": ["valence"],
        "tempo": ["tempo"],
        "time_signature": ["time_signature", "time signature"],
    }

    inserted_tracks = 0
    skipped_rows = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError("CSV has no header")

        for row_idx, row in enumerate(reader, start=1):
            spotify_track_id = pick(row, track_id_keys)
            if not spotify_track_id:
                skipped_rows += 1
                continue

            track_name = pick(row, track_name_keys) or "(unknown)"
            explicit = parse_bool(pick(row, explicit_keys))
            duration_min = parse_decimal(pick(row, duration_min_keys))
            # Requirement: random disc_number in {1, 2}
            disc_number = random.choice([1, 2])
            track_number = parse_smallint(pick(row, track_number_keys))
            release_date = parse_date(pick(row, release_date_keys))
            # Requirement: generate a unique ISRC for each track
            isrc = generate_isrc(spotify_track_id, release_date)

            artist_names = parse_listish(pick(row, artist_names_keys))
            if not artist_names:
                # Allow import even with missing artist; we just won't create relations.
                artist_names = []

            album_name = pick(row, album_name_keys)
            spotify_album_id = pick(row, album_id_keys)
            album_type = pick(row, album_type_keys)
            total_tracks = parse_int(pick(row, total_tracks_keys))

            raw_genres_text = pick(row, genres_keys)
            genres = parse_listish(raw_genres_text)

            track_db_id = upsert_track(
                cur,
                spotify_track_id=spotify_track_id,
                name=track_name,
                explicit=explicit,
                duration_min=duration_min,
                disc_number=disc_number,
                track_number=track_number,
                isrc=isrc,
            )

            album_db_id: Optional[int] = None
            if album_name or spotify_album_id:
                album_db_id = upsert_album(
                    cur,
                    spotify_album_id=spotify_album_id,
                    name=album_name or "(unknown)",
                    album_type=album_type,
                    release_date=release_date,
                    total_tracks=total_tracks,
                )
                add_track_album(cur, track_db_id, album_db_id)

            artist_db_ids: list[int] = []
            for order, artist_name in enumerate(artist_names, start=1):
                artist_db_id = upsert_artist(
                    cur,
                    name=artist_name,
                    raw_genres_text=raw_genres_text,
                )
                artist_db_ids.append(artist_db_id)
                add_track_artist(cur, track_db_id, artist_db_id, order)
                if album_db_id is not None:
                    add_album_artist(cur, album_db_id, artist_db_id, order)

            # Genres -> tie to artists (best-effort)
            if genres and artist_db_ids:
                for genre_name in genres:
                    genre_name = genre_name.strip()
                    if not genre_name:
                        continue
                    genre_id = upsert_genre(cur, genre_name)
                    for artist_db_id in artist_db_ids:
                        add_artist_genre(cur, artist_db_id, genre_id)

            # Audio features
            features: dict[str, Any] = {}
            for feat, keys in audio_map.items():
                raw = pick(row, keys)
                if feat in {"key", "mode", "time_signature"}:
                    features[feat] = parse_smallint(raw)
                else:
                    features[feat] = parse_decimal(raw)
            upsert_audio_features(cur, track_db_id, features)

            inserted_tracks += 1

            if inserted_tracks % commit_every == 0:
                conn.commit()
                print(f"Committed {inserted_tracks} tracks...")

        conn.commit()

    print(f"Imported tracks: {inserted_tracks}")
    if skipped_rows:
        print(f"Skipped rows missing track id: {skipped_rows}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze and import Kaggle Spotify Global Music Dataset CSV into the Postgres schema created by init.sql.",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to a CSV file (e.g., track_data_final.csv or spotify_data clean.csv)",
    )
    parser.add_argument("--analyze", action="store_true", help="Print columns and first 10 rows")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Import into Postgres")
    parser.add_argument("--limit", type=int, default=10, help="How many rows to print in analyze mode")
    parser.add_argument("--commit-every", type=int, default=500, help="Commit every N inserted tracks")

    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "spotify_db"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", "pass"))

    args = parser.parse_args()

    if not args.analyze and not args.do_import:
        # Default behavior: do both
        args.analyze = True
        args.do_import = True

    if args.analyze:
        analyze_csv(args.csv, limit=args.limit)

    if args.do_import:
        cfg = DbConfig(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )
        with connect_db(cfg) as conn:
            ensure_schema(conn)
            import_csv(conn, args.csv, commit_every=args.commit_every)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
