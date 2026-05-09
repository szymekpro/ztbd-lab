import argparse
import os
import random
import string
import sys
import time as pytime
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    concurrency: int = 500
    chunk_size: int = 50000
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
        from gevent import monkey  # type: ignore[import-not-found]

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

    # Cassandra in Docker can take a while to start or briefly restart under load.
    # Retrying here prevents flaky benchmark runs (seed is called before every scale).
    try:
        from cassandra.cluster import NoHostAvailable
    except Exception:  # pragma: no cover
        NoHostAvailable = Exception  # type: ignore

    last_exc: Optional[Exception] = None
    session = None
    for attempt in range(1, 31):
        try:
            session = cluster.connect()
            break
        except NoHostAvailable as exc:  # type: ignore[misc]
            last_exc = exc
            pytime.sleep(min(10.0, 0.5 + attempt * 0.5))

    if session is None:
        try:
            cluster.shutdown()
        finally:
            raise last_exc if last_exc is not None else RuntimeError("Unable to connect to Cassandra")

    try:
        session.set_keyspace(cfg.keyspace)
    except Exception as exc:
        # First-run convenience: create/init the schema when docker init hasn't run.
        try:
            from cassandra import InvalidRequest  # type: ignore

            is_missing_keyspace = isinstance(exc, InvalidRequest) and "does not exist" in str(exc)
        except Exception:  # pragma: no cover
            is_missing_keyspace = False

        if not is_missing_keyspace:
            raise

        _bootstrap_schema_from_repo_init(session, keyspace=cfg.keyspace)
        session.set_keyspace(cfg.keyspace)
    return cluster, session


def _bootstrap_schema_from_repo_init(session, keyspace: str) -> None:
    init_path = Path(__file__).resolve().parent / "init.cassandra.cql"
    if not init_path.exists():
        raise RuntimeError(
            f"Missing {init_path.name}; cannot auto-create Cassandra schema. "
            "Start DB with docker-compose so init.cassandra.cql runs."
        )

    raw = init_path.read_text(encoding="utf-8")
    # Make the init script work with a custom keyspace name.
    raw = re.sub(
        r"(?im)CREATE\s+KEYSPACE\s+IF\s+NOT\s+EXISTS\s+\w+",
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace}",
        raw,
    )
    raw = re.sub(r"(?im)USE\s+\w+\s*;", f"USE {keyspace};", raw)

    # Strip '-- ...' comments and split into statements.
    cql = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--"))
    statements = [stmt.strip() for stmt in cql.split(";") if stmt.strip()]

    print(f"[INIT] Bootstrapping Cassandra schema from {init_path.name} (keyspace={keyspace})")

    post_keyspace: list[str] = []
    for stmt in statements:
        if re.match(r"(?is)^USE\s+", stmt):
            continue
        if re.match(r"(?is)^CREATE\s+KEYSPACE\s+", stmt):
            session.execute(stmt)
        else:
            post_keyspace.append(stmt)

    session.set_keyspace(keyspace)
    for stmt in post_keyspace:
        session.execute(stmt)
    print("[INIT] Cassandra schema ready")


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
        # Try bootstrapping automatically (helps when init script wasn't executed).
        _bootstrap_schema_from_repo_init(session, keyspace=keyspace)
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
                "Schema not found (missing tracks table). "
                "Start DB with docker-compose or run init.cassandra.cql manually."
            )


_SEED_RETRY_ATTEMPTS = 4
_SEED_RETRY_BACKOFF = [2.0, 5.0, 15.0]  # seconds between retries


def insert_many(
    session,
    cql: str,
    rows: Iterable[Sequence[object]],
    tuning: WriteTuning,
    label: str,
) -> int:
    try:
        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.cluster import NoHostAvailable
        from cassandra import OperationTimedOut, WriteTimeout, WriteFailure
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("Missing cassandra-driver") from e

    _transient = (NoHostAvailable, OperationTimedOut, WriteTimeout, WriteFailure, ConnectionError)

    iterator = iter(rows)
    statement = session.prepare(cql)
    loaded = 0
    started = pytime.perf_counter()

    while True:
        batch = list(islice(iterator, tuning.chunk_size))
        if not batch:
            break

        for attempt in range(_SEED_RETRY_ATTEMPTS):
            try:
                execute_concurrent_with_args(
                    session,
                    statement,
                    batch,
                    concurrency=tuning.concurrency,
                    raise_on_first_error=True,
                )
                break
            except _transient as exc:
                if attempt >= _SEED_RETRY_ATTEMPTS - 1:
                    raise
                wait = _SEED_RETRY_BACKOFF[min(attempt, len(_SEED_RETRY_BACKOFF) - 1)]
                print(f"[SEED] {label}: transient error (attempt {attempt + 1}), retry in {wait:.0f}s — {exc}")
                pytime.sleep(wait)

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
    parallel_workers: int = 1,
) -> None:
    tasks: list[tuple[str, str, callable[[], Iterable[Sequence[object]]]]] = []

    def ceil_div(n: int, d: int) -> int:
        return (n + d - 1) // d

    def id_ranges(total: int, per_range: int) -> Iterable[tuple[int, int]]:
        if total <= 0:
            return
        step = max(1, int(per_range))
        for start_id in range(1, total + 1, step):
            end_id = min(total, start_id + step - 1)
            yield start_id, end_id

    requested_workers = max(1, int(parallel_workers))
    # Aim for enough tasks to keep workers busy, but keep ranges reasonably large.
    desired_ranges = max(1, requested_workers * 4)
    min_ids_per_task = 10_000

    artist_range_size = max(min_ids_per_task, ceil_div(n_artists, desired_ranges)) if n_artists > 0 else 0
    album_range_size = max(min_ids_per_task, ceil_div(n_albums, desired_ranges)) if n_albums > 0 else 0
    track_range_size = max(min_ids_per_task, ceil_div(n_tracks, desired_ranges)) if n_tracks > 0 else 0

    if n_artists > 0 and n_genres > 0:
        cql = "INSERT INTO artist_genres (artist_id, genre_id) VALUES (?, ?)"
        for start_id, end_id in id_ranges(n_artists, artist_range_size):
            label = "artist_genres" if start_id == 1 and end_id == n_artists else f"artist_genres[{start_id}-{end_id}]"

            def rows_fn(start_id: int = start_id, end_id: int = end_id) -> Iterable[Sequence[object]]:
                for artist_id in range(start_id, end_id + 1):
                    for gs in range(1, max(1, genres_per_artist) + 1):
                        genre_id = ((artist_id + gs - 2) % n_genres) + 1
                        yield (artist_id, genre_id)

            tasks.append((label, cql, rows_fn))

    if n_albums > 0 and n_artists > 0:
        cql = "INSERT INTO album_artists (album_id, artist_id, artist_order) VALUES (?, ?, ?)"
        for start_id, end_id in id_ranges(n_albums, album_range_size):
            label = "album_artists" if start_id == 1 and end_id == n_albums else f"album_artists[{start_id}-{end_id}]"

            def rows_fn(start_id: int = start_id, end_id: int = end_id) -> Iterable[Sequence[object]]:
                for album_id in range(start_id, end_id + 1):
                    for gs in range(1, max(1, artists_per_album) + 1):
                        artist_id = ((album_id + gs - 2) % n_artists) + 1
                        yield (album_id, artist_id, gs)

            tasks.append((label, cql, rows_fn))

    if n_tracks > 0 and n_artists > 0:
        cql = "INSERT INTO track_artists (track_id, artist_id, artist_order) VALUES (?, ?, ?)"
        for start_id, end_id in id_ranges(n_tracks, track_range_size):
            label = "track_artists" if start_id == 1 and end_id == n_tracks else f"track_artists[{start_id}-{end_id}]"

            def rows_fn(start_id: int = start_id, end_id: int = end_id) -> Iterable[Sequence[object]]:
                for track_id in range(start_id, end_id + 1):
                    for gs in range(1, max(1, artists_per_track) + 1):
                        artist_id = ((track_id + gs - 2) % n_artists) + 1
                        yield (track_id, artist_id, gs)

            tasks.append((label, cql, rows_fn))

    if n_tracks > 0 and n_albums > 0:
        cql = "INSERT INTO track_albums (track_id, album_id, is_primary) VALUES (?, ?, ?)"
        for start_id, end_id in id_ranges(n_tracks, track_range_size):
            label = "track_albums" if start_id == 1 and end_id == n_tracks else f"track_albums[{start_id}-{end_id}]"

            def rows_fn(start_id: int = start_id, end_id: int = end_id) -> Iterable[Sequence[object]]:
                for track_id in range(start_id, end_id + 1):
                    album_id = ((track_id - 1) % n_albums) + 1
                    yield (track_id, album_id, True)

            tasks.append((label, cql, rows_fn))

    if not tasks:
        return

    max_workers = requested_workers
    max_workers = min(max_workers, len(tasks))
    if max_workers <= 1:
        for label, cql, rows_fn in tasks:
            insert_many(session, cql, rows_fn(), tuning, label)
        return

    per_task_concurrency = max(1, tuning.concurrency // max_workers)
    task_tuning = WriteTuning(
        concurrency=per_task_concurrency,
        chunk_size=tuning.chunk_size,
        progress_every=tuning.progress_every,
    )
    print(
        f"[SEED] relations: parallel workers={max_workers}, tasks={len(tasks)}, per-task concurrency={per_task_concurrency} "
        f"(total budget≈{per_task_concurrency * max_workers})"
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(insert_many, session, cql, rows_fn(), task_tuning, label) for label, cql, rows_fn in tasks]
        for fut in as_completed(futures):
            fut.result()


def seed_audio_features(
    session,
    n_tracks: int,
    seed: Optional[int],
    tuning: WriteTuning,
    parallel_workers: int = 1,
    batch_tracks: int = 0,
) -> None:
    if n_tracks <= 0:
        return

    def rows_range(start_id: int, end_id: int) -> Iterable[Sequence[object]]:
        decimal_fn = _decimal
        for track_id in range(start_id, end_id + 1):
            u01a = ((track_id * 1103515245 + 12345) % 1000) / 1000.0
            u01b = ((track_id * 1103515245 + 67890) % 1000) / 1000.0
            u01c = ((track_id * 1103515245 + 44444) % 1000) / 1000.0
            u01d = ((track_id * 1103515245 + 55555) % 1000) / 1000.0
            u01e = ((track_id * 1103515245 + 77777) % 1000) / 1000.0
            u01f = ((track_id * 1103515245 + 88888) % 1000) / 1000.0
            yield (
                track_id,
                decimal_fn(u01a, 3),
                decimal_fn(u01b, 3),
                int((track_id * 1103515245 + 11111) % 12),
                int((track_id * 1103515245 + 22222) % 2),
                decimal_fn(-(((track_id * 1103515245 + 33333) % 35000) / 1000.0), 3),
                decimal_fn(u01c, 3),
                decimal_fn(u01d, 3),
                decimal_fn(((track_id * 1103515245 + 66666) % 100000) / 100000.0, 5),
                decimal_fn(u01e, 3),
                decimal_fn(u01f, 3),
                decimal_fn(60.0 + (((track_id * 1103515245 + 99999) % 12000) / 100.0), 2),
                [3, 4, 5][(track_id * 1103515245 + 13579) % 3],
            )

    cql = """
        INSERT INTO audio_features (
          track_id, danceability, energy, "key", mode, loudness, speechiness,
          acousticness, instrumentalness, liveness, valence, tempo, time_signature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    max_workers = max(1, int(parallel_workers))
    if max_workers <= 1:
        insert_many(session, cql, rows_range(1, n_tracks), tuning, "audio_features")
        return

    def ceil_div(n: int, d: int) -> int:
        return (n + d - 1) // d

    # Auto mode: choose tracks/batch to create enough batches to keep workers busy.
    if int(batch_tracks) > 0:
        tracks_per_batch = int(batch_tracks)
        auto = False
    else:
        desired_batches = max(1, max_workers * 4)
        suggested = ceil_div(n_tracks, desired_batches)
        min_batch = max(1_000, min(5_000, tuning.chunk_size))
        max_batch = max(50_000, tuning.chunk_size * 10)
        tracks_per_batch = max(min_batch, min(max_batch, suggested))
        auto = True

    batches: list[tuple[int, int]] = []
    for start_id in range(1, n_tracks + 1, tracks_per_batch):
        end_id = min(n_tracks, start_id + tracks_per_batch - 1)
        batches.append((start_id, end_id))

    max_workers = min(max_workers, len(batches))
    per_task_concurrency = max(1, tuning.concurrency // max_workers)
    task_tuning = WriteTuning(
        concurrency=per_task_concurrency,
        chunk_size=tuning.chunk_size,
        # Avoid extremely noisy logs from many batches.
        progress_every=0,
    )
    print(
        f"[SEED] audio_features: parallel workers={max_workers}, batches={len(batches)}, "
        f"tracks/batch={tracks_per_batch}{' (auto)' if auto else ''}, per-task concurrency={per_task_concurrency} "
        f"(total budget≈{per_task_concurrency * max_workers})"
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(insert_many, session, cql, rows_range(start_id, end_id), task_tuning, f"audio_features[{start_id}-{end_id}]")
            for start_id, end_id in batches
        ]
        for fut in as_completed(futures):
            fut.result()


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
    include_audio_features: bool = True,
    relations_workers: int = 4,
    audio_features_workers: int = 4,
    audio_features_batch_tracks: int = 0,
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
        parallel_workers=max(1, int(relations_workers)),
    )

    _seed_audio = include_audio_features and not fast_mode
    if _seed_audio:
        seed_audio_features(
            session,
            n_tracks=track_count,
            seed=seed,
            tuning=write_tuning,
            parallel_workers=max(1, int(audio_features_workers)),
            batch_tracks=max(0, int(audio_features_batch_tracks)),
        )

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

    parser.add_argument(
        "--relations-workers",
        type=int,
        default=int(os.getenv("RELATIONS_WORKERS", "12")),
        help=(
            "Parallel workers for relation tables (artist_genres/album_artists/track_artists/track_albums). "
            "Total driver concurrency budget is split across workers. Set 1 to disable parallelism."
        ),
    )
    parser.add_argument(
        "--audio-workers",
        type=int,
        default=int(os.getenv("AUDIO_WORKERS", "12")),
        help=(
            "Parallel workers for audio_features batches. Total driver concurrency budget is split across workers. "
            "Set 1 to disable parallelism."
        ),
    )
    parser.add_argument(
        "--audio-batch-tracks",
        type=int,
        default=int(os.getenv("AUDIO_BATCH_TRACKS", "0")),
        help=(
            "How many tracks per audio_features batch when --audio-workers>1. "
            "0 = auto (aims for ~4 batches per worker; bounded by write_chunk_size and a safe min/max)."
        ),
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
            relations_workers=args.relations_workers,
            audio_features_workers=args.audio_workers,
            audio_features_batch_tracks=args.audio_batch_tracks,
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

