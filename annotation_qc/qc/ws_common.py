# -*- coding: utf-8 -*-
"""裁决工作区通用设施：路径、连接、事务、锁与标识。

工作区目录结构::

    <workspace>/
      workspace.db      工作区库（WAL）
      base.db           初始化时复制、此后只读的底稿构建库副本
      .ws.lock          跨进程合批/发布串行化用

所有业务写入都在单个 ``BEGIN IMMEDIATE`` 事务里完成；进程退出或
异常时不会留下可见的半版。
"""
from __future__ import annotations

import contextlib
import fcntl
import os
import sqlite3
from datetime import datetime, timezone
from typing import Iterator, Optional

from .util import canonical_json, stable_id

SCHEMA_FILE = "ws_schema.sql"
DB_NAME = "workspace.db"
BASE_COPY_NAME = "base.db"
LOCK_NAME = ".ws.lock"

HEAD = "HEAD"


class WsError(Exception):
    """工作区层面的可定位错误。"""

    def __init__(self, code: str, message: str, detail: Optional[dict] = None):
        self.code = code
        self.message = message
        self.detail = detail or {}
        super().__init__(f"{code}: {message}")

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, **self.detail}


def db_path(workspace: str) -> str:
    return os.path.join(os.path.abspath(workspace), DB_NAME)


def base_copy_path(workspace: str) -> str:
    return os.path.join(os.path.abspath(workspace), BASE_COPY_NAME)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    # 关闭 sqlite3 的隐式事务，改由 transaction() 显式开/提交，
    # 避免 BEGIN 落在隐式事务里。
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA temp_store=FILE")  # 高扇出时临时表可落磁盘


def init_schema(conn: sqlite3.Connection) -> None:
    path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())


def connect(workspace: str, *, create: bool = False) -> sqlite3.Connection:
    """打开工作区库；默认要求已初始化。"""
    path = db_path(workspace)
    if create:
        os.makedirs(os.path.abspath(workspace), exist_ok=True)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        init_schema(conn)
        conn.commit()
        return conn
    if not os.path.isfile(path):
        raise WsError("WORKSPACE_NOT_FOUND", f"workspace not initialised: {path}")
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    attach_base(conn, workspace)
    return conn


def attach_base(conn: sqlite3.Connection, workspace: str) -> None:
    """把工作区内的只读底稿副本挂为 ``base`` 库（证据查询用）。"""
    copy = base_copy_path(workspace)
    if not os.path.isfile(copy):
        return
    existing = {
        r[1] for r in conn.execute("PRAGMA database_list").fetchall()
    }
    if "base" in existing:
        return
    uri = "file:" + os.path.abspath(copy) + "?mode=ro&immutable=1"
    conn.execute("ATTACH DATABASE ? AS base", (uri,))


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection, *, immediate: bool = True) -> Iterator[None]:
    """显式事务：异常一律回滚，绝不留下半版。"""
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextlib.contextmanager
def exclusive(workspace: str) -> Iterator[None]:
    """跨进程排它锁；与 SQLite 事务叠加，保证不互相覆盖。"""
    os.makedirs(os.path.abspath(workspace), exist_ok=True)
    path = os.path.join(os.path.abspath(workspace), LOCK_NAME)
    fh = open(path, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM ws_meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO ws_meta(key, value) VALUES (?,?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def resolve_revision(conn: sqlite3.Connection, rev: str) -> sqlite3.Row:
    """按 rev_id / 数字序号 / HEAD 定位审定版本。"""
    if rev is None or str(rev).upper() == HEAD:
        head = meta_get(conn, "head_rev")
        if head is None:
            raise WsError("NO_HEAD", "workspace has no head revision")
        row = conn.execute("SELECT * FROM revision WHERE rev_id=?", (head,)).fetchone()
    elif str(rev).isdigit():
        row = conn.execute(
            "SELECT * FROM revision WHERE seq=?", (int(rev),)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM revision WHERE rev_id=?", (rev,)
        ).fetchone()
    if row is None:
        raise WsError("REVISION_NOT_FOUND", f"unknown revision: {rev}")
    return row


def head_revision(conn: sqlite3.Connection) -> sqlite3.Row:
    return resolve_revision(conn, HEAD)


def relation_key(doc_id: str, rel_type: str, from_fragment: str,
                 to_fragment: str) -> str:
    """有序端点 + 类型构成的关系判定键（反向天然是另一个键）。"""
    return canonical_json([doc_id, rel_type, from_fragment, to_fragment])


def relation_key_parts(key: str) -> tuple:
    import json

    doc, rel_type, frm, to = json.loads(key)
    return doc, rel_type, frm, to


def fragment_uid(doc_id: str, layer: str, start: int, end: int,
                 label: str) -> str:
    """与构建阶段完全一致的审定片段身份。"""
    return stable_id("frag", doc_id, layer, start, end, label)


def revision_id(seq: int, parent_rev: Optional[str], ticket_uid: Optional[str],
                digest: str) -> str:
    return stable_id("rev", seq, parent_rev or "", ticket_uid or "", digest)


def release_id(rev_id: str, doc_ids: list) -> str:
    return stable_id("release", rev_id, sorted(doc_ids))


def ticket_uid(reviewer_id: str, base_rev: str, content: dict) -> str:
    return stable_id("tkt", reviewer_id, base_rev, content)
