# -*- coding: utf-8 -*-
"""分仓台账通用设施：路径、连接、事务、锁与错误对象。

台账目录结构::

    <ledger>/
      ledger.db       台账库（WAL）
      .ledger.lock    跨进程写入串行化用

约定与裁决工作区一致：所有业务写入在单个 ``BEGIN IMMEDIATE`` 事务内完成，
并持有目录级文件锁；进程在写入中退出不会留下可见的半版、部分摘要、
错误锁位或与清单版本不符的回执。
"""
from __future__ import annotations

import contextlib
import fcntl
import os
import sqlite3
from typing import Iterator, List, Optional

SCHEMA_FILE = "ledger_schema.sql"
DB_NAME = "ledger.db"
LOCK_NAME = ".ledger.lock"

SCHEMA_VERSION = "1"


class LedgerError(Exception):
    """台账层面的可定位错误；可携带多条错误（整批拒绝时报全）。"""

    def __init__(self, code: str, message: str,
                 detail: Optional[dict] = None,
                 errors: Optional[List[dict]] = None):
        self.code = code
        self.message = message
        self.detail = detail or {}
        self.errors = errors or []
        super().__init__(f"{code}: {message}")

    def to_dict(self) -> dict:
        out = {"code": self.code, "message": self.message, **self.detail}
        if self.errors:
            out["errors"] = self.errors
        return out


def db_path(ledger: str) -> str:
    return os.path.join(os.path.abspath(ledger), DB_NAME)


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.isolation_level = None  # 显式事务，见 transaction()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA temp_store=FILE")  # 大分量/大清单的临时索引可落磁盘


def init_schema(conn: sqlite3.Connection) -> None:
    path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())


def connect(ledger: str, *, create: bool = False) -> sqlite3.Connection:
    """打开台账库；默认要求已建账。"""
    path = db_path(ledger)
    if create:
        os.makedirs(os.path.abspath(ledger), exist_ok=True)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        init_schema(conn)
        conn.commit()
        return conn
    if not os.path.isfile(path):
        raise LedgerError("LEDGER_NOT_FOUND", f"ledger not initialised: {path}")
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """显式事务：异常一律回滚，绝不留下半版。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextlib.contextmanager
def exclusive(ledger: str) -> Iterator[None]:
    """跨进程排它锁；与 SQLite 事务叠加，保证并发写入不互相覆盖。"""
    os.makedirs(os.path.abspath(ledger), exist_ok=True)
    path = os.path.join(os.path.abspath(ledger), LOCK_NAME)
    fh = open(path, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute(
        "SELECT value FROM lg_meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO lg_meta(key, value) VALUES (?,?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def ledger_version(conn: sqlite3.Connection) -> int:
    return int(meta_get(conn, "ledger_version") or "0")
