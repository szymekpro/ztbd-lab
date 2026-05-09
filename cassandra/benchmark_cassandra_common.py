from __future__ import annotations

import random
import string
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Optional


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    keyspace: str
    user: Optional[str]
    password: Optional[str]


def _prepare_cassandra_runtime() -> None:
    import sys

    if sys.version_info < (3, 12):
        return

    try:
        import asyncore  # type: ignore

        _ = asyncore
        return
    except ImportError:
        pass

    try:
        from gevent import monkey  # type: ignore[import-not-found]

        if not monkey.is_module_patched("socket"):
            monkey.patch_all()
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "Python 3.12 with cassandra-driver requires pyasyncore or gevent. "
            "Install one of: pip install pyasyncore OR pip install gevent"
        ) from e


def connect_db(cfg: DbConfig):
    _prepare_cassandra_runtime()

    try:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "cassandra-driver is required. Install with: pip install cassandra-driver"
        ) from e

    auth_provider = None
    if cfg.user:
        auth_provider = PlainTextAuthProvider(username=cfg.user, password=cfg.password or "")

    cluster = Cluster([cfg.host], port=cfg.port, auth_provider=auth_provider)

    # Cassandra may not be ready immediately after container start/restart.
    # A small retry loop avoids flaky benchmark runs.
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
            time.sleep(min(10.0, 0.5 + attempt * 0.5))

    if session is None:
        try:
            cluster.shutdown()
        finally:
            raise last_exc if last_exc is not None else RuntimeError("Unable to connect to Cassandra")

    try:
        session.set_keyspace(cfg.keyspace)
    except Exception as exc:
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
    init_path = Path(__file__).resolve().parents[1] / "init.cassandra.cql"
    if not init_path.exists():
        raise RuntimeError(
            f"Missing {init_path.name}; cannot auto-create Cassandra schema. "
            "Start DB with docker-compose so init.cassandra.cql runs."
        )

    raw = init_path.read_text(encoding="utf-8")
    raw = re.sub(
        r"(?im)CREATE\s+KEYSPACE\s+IF\s+NOT\s+EXISTS\s+\w+",
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace}",
        raw,
    )
    raw = re.sub(r"(?im)USE\s+\w+\s*;", f"USE {keyspace};", raw)
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


def close_db(cluster, session) -> None:
    try:
        if session is not None:
            session.shutdown()
    finally:
        if cluster is not None:
            cluster.shutdown()


def ensure_existing_schema(session, keyspace: str) -> None:
    row = session.execute(
        """
        SELECT table_name
        FROM system_schema.tables
        WHERE keyspace_name = %s
          AND table_name IN ('artists', 'albums', 'tracks', 'track_artists', 'track_albums')
        """,
        (keyspace,),
    ).one()
    if row is None:
        _bootstrap_schema_from_repo_init(session, keyspace=keyspace)
        row = session.execute(
            """
            SELECT table_name
            FROM system_schema.tables
            WHERE keyspace_name = %s
              AND table_name IN ('artists', 'albums', 'tracks', 'track_artists', 'track_albums')
            """,
            (keyspace,),
        ).one()
        if row is None:
            raise RuntimeError(
                "Required schema/tables not found even after init. "
                "Start DB with docker-compose or run init.cassandra.cql manually."
            )


def parse_scales(raw: str) -> list[int]:
    scales: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        value = int(text)
        if value <= 0:
            raise ValueError("Scale must be positive")
        scales.append(value)
    if not scales:
        raise ValueError("At least one scale is required")
    return scales


def scaled_count(scale: int, fraction: float, *, min_count: int, max_count: int) -> int:
    if scale <= 0:
        return min_count
    return max(min_count, min(int(scale * fraction), max_count))


def timed_run(fn, *args, **kwargs) -> tuple[float, int]:
    start = time.perf_counter()
    ops = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, int(ops)


def prepare_scale_data_with_seed_script(
    cfg: DbConfig,
    target_rows: int,
    seed_value: Optional[int],
    pool_size: int,
    include_audio_features: bool = True,
) -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import seed_cassandra_faker_data as seed_script

    seed_cfg = seed_script.DbConfig(
        host=cfg.host,
        port=cfg.port,
        keyspace=cfg.keyspace,
        user=cfg.user,
        password=cfg.password,
    )

    cluster = None
    session = None
    try:
        cluster, session = seed_script.connect_db(seed_cfg)
        seed_script.seed_all(
            session,
            keyspace=seed_cfg.keyspace,
            n_genres=30,
            n_artists=max(50, target_rows // 20000),
            n_albums=max(80, target_rows // 10000),
            n_tracks=target_rows,
            seed=seed_value,
            truncate=True,
            pool_size=pool_size,
            fast_mode=False,
            include_audio_features=include_audio_features,
        )
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()

def apply_indexes(session, managed_indexes: list[dict], with_indexes: bool) -> None:
    action = "Tworzenie" if with_indexes else "Usuwanie"
    print(f"\n[INDEX] {action} indeksow...")
    for idx in managed_indexes:
        sql = idx["create"] if with_indexes else idx["drop"]
        label = "(nowy)" if idx.get("new") else "(schemat)"
        print(f"  {'CREATE' if with_indexes else 'DROP':6s}  {idx['name']} {label}")
        session.execute(sql)
    print("[INDEX] Gotowe.\n")


def wait_for_secondary_indexes(
    session,
    managed_indexes: list[dict],
    *,
    max_total_seconds: float = 600.0,
    step_seconds: float = 30.0,
) -> None:
    """Wait until secondary indexes become queryable.

    Cassandra builds secondary indexes asynchronously on existing data. Immediately querying after
    CREATE INDEX can fail with IndexNotAvailableException. This helper polls index-backed queries
    (one per managed index) until they succeed or the time budget is exhausted.

    It is best-effort: after timeout it logs a warning and returns.
    """

    targets: list[tuple[str, str, str]] = []  # (index_name, table, column)
    for idx in managed_indexes:
        create_stmt = str(idx.get("create") or "")
        m = re.search(r"(?is)\bON\s+([a-zA-Z_][\w]*)\s*\(\s*([a-zA-Z_][\w]*)\s*\)", create_stmt)
        if not m:
            continue
        table = m.group(1)
        column = m.group(2)
        targets.append((str(idx.get("name") or "<index>"), table, column))

    if not targets:
        return

    # Fetch one sample value per index column, so the readiness probe uses the index.
    samples: dict[tuple[str, str], object] = {}
    for _idx_name, table, column in targets:
        key = (table, column)
        if key in samples:
            continue
        try:
            row = session.execute(f"SELECT {column} FROM {table} LIMIT 1").one()
        except Exception:
            continue
        if row is None:
            continue
        value = getattr(row, column, None)
        if value is None:
            continue
        samples[key] = value

    # Only wait for those where we can probe with an actual value.
    probe_targets = [(idx_name, table, column) for (idx_name, table, column) in targets if (table, column) in samples]
    if not probe_targets:
        return

    deadline = time.monotonic() + max(0.0, float(max_total_seconds))
    attempt = 0
    last_exc: Optional[Exception] = None

    while True:
        attempt += 1
        pending: list[str] = []

        for idx_name, table, column in probe_targets:
            value = samples[(table, column)]
            try:
                session.execute(
                    f"SELECT * FROM {table} WHERE {column} = %s LIMIT 1",
                    (value,),
                )
            except Exception as exc:
                last_exc = exc
                pending.append(idx_name)

        if not pending:
            return

        now = time.monotonic()
        remaining = deadline - now
        if remaining <= 0:
            msg = (
                f"[WARN] secondary indexes not available after {max_total_seconds:.0f}s; "
                f"still failing: {', '.join(sorted(set(pending)))}"
            )
            if last_exc is not None:
                msg += f" — last error: {type(last_exc).__name__}: {last_exc}"
            print(msg)
            return

        # Log a compact status line occasionally.
        if attempt == 1 or attempt % 2 == 0:
            print(
                f"[INDEX] Waiting for indexes ({len(pending)} pending): {', '.join(sorted(set(pending)))} "
                f"(sleep {min(step_seconds, remaining):.0f}s, budget left {remaining:.0f}s)"
            )

        time.sleep(min(float(step_seconds), remaining))


def new_bigint_id() -> int:
    return uuid.uuid4().int & ((1 << 63) - 1)


def new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def random_text(size: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=size))
