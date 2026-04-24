from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


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
            "PyMySQL is required for MariaDB benchmarking. Install with: pip install PyMySQL"
        ) from e

    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.dbname,
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )


def ensure_existing_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name IN ('artists', 'albums', 'tracks', 'track_artists', 'track_albums')
            """
        )
        found = int(cur.fetchone()[0])
        if found < 5:
            raise RuntimeError("Required spotify schema/tables not found. Run init.mariadb.sql first.")


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

    import seed_mariadb_faker_data as seed_script

    seed_cfg = seed_script.DbConfig(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

    conn = seed_script.connect_db(seed_cfg)
    try:
        seed_script.seed_all(
            conn,
            dbname=seed_cfg.dbname,
            n_genres=30,
            n_artists=max(50, target_rows // 20000),
            n_albums=max(80, target_rows // 10000),
            n_tracks=target_rows,
            seed=seed_value,
            truncate=True,
            pool_size=pool_size,
        )
        conn.commit()
    finally:
        conn.close()


def apply_indexes(conn, managed_indexes: list[dict], with_indexes: bool) -> None:
    action = "Tworzenie" if with_indexes else "Usuwanie"
    print(f"\n[INDEX] {action} indeksow...")
    with conn.cursor() as cur:
        for idx in managed_indexes:
            sql = idx["create"] if with_indexes else idx["drop"]
            label = "(nowy)" if idx.get("new") else "(schemat)"
            print(f"  {'CREATE' if with_indexes else 'DROP':6s}  {idx['name']} {label}")
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                err_no = int(exc.args[0]) if getattr(exc, "args", None) else None
                # MariaDB/InnoDB may block DROP for indexes bound to FK constraints.
                if not with_indexes and err_no in (1091, 1553):
                    print(f"  SKIP    {idx['name']} (pomijam: {exc})")
                    continue
                raise
    print("[INDEX] Gotowe.\n")
