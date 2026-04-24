from __future__ import annotations

import random
import string
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
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
        from gevent import monkey

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
    session = cluster.connect()
    session.set_keyspace(cfg.keyspace)
    return cluster, session


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
        raise RuntimeError(
            "Required spotify schema/tables not found. Run init.cassandra.cql first."
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


def new_bigint_id() -> int:
    return uuid.uuid4().int & ((1 << 63) - 1)


def new_spotify_id(prefix: str) -> str:
    seed = prefix + uuid.uuid4().hex
    return (seed[:22]).ljust(22, "0")


def new_isrc() -> str:
    return ("PL" + uuid.uuid4().hex.upper())[:12]


def random_text(size: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=size))
