# -*- coding: utf-8 -*-
"""发布后独立盲审：路径、连接、事务、锁与错误对象。

审计目录结构::

    <audit>/
      audit.db       审计库（WAL）
      .audit.lock    跨进程创建/提交串行化用

约定与裁决工作区、分仓台账一致：
- 已登记发布只作为只读上游（``mode=ro``），审计不回写质检库、
  裁决工作区或发布内容；
- 所有业务写入在单个 ``BEGIN IMMEDIATE`` 事务内完成，进程中断不会
  留下可见的半批结果；
- 业务标识为内容派生的文本 ID，盲审标识由本批随机密钥派生。
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from typing import Iterator, Optional

from .util import canonical_json

SCHEMA_FILE = "audit_schema.sql"
DB_NAME = "audit.db"
LOCK_NAME = ".audit.lock"

CREATE = "create"
SUBMIT = "submit"

VERDICT_CONFORMING = "conforming"
VERDICT_ISSUE = "issue"


class AuditError(Exception):
    """盲审层面的可定位错误；可携带多条逐项错误（整批拒绝时报全）。"""

    def __init__(self, code: str, message: str,
                 detail: Optional[dict] = None,
                 errors: Optional[list] = None):
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


def audit_db_path(audit_dir: str) -> str:
    return os.path.join(os.path.abspath(audit_dir), DB_NAME)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.isolation_level = None  # 显式事务，见 transaction()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    # 抽样/导出的排序临时索引可落磁盘，Python 侧只保留 O(样本量) 状态
    conn.execute("PRAGMA temp_store=FILE")


def init_schema(conn: sqlite3.Connection) -> None:
    path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())


def connect(audit_dir: str, *, create: bool = False) -> sqlite3.Connection:
    """打开审计库；默认要求已初始化。"""
    path = audit_db_path(audit_dir)
    if create:
        os.makedirs(os.path.abspath(audit_dir), exist_ok=True)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        init_schema(conn)
        conn.commit()
        return conn
    if not os.path.isfile(path):
        raise AuditError("AUDIT_NOT_FOUND",
                         f"audit store not initialised: {path}")
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def open_audit(audit_dir: str) -> sqlite3.Connection:
    """打开已初始化审计库；不存在则就地初始化空库。"""
    if os.path.isfile(audit_db_path(audit_dir)):
        return connect(audit_dir)
    return connect(audit_dir, create=True)


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """显式事务：异常一律回滚，绝不留下半批结果。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextlib.contextmanager
def exclusive(audit_dir: str) -> Iterator[None]:
    """跨进程排它锁；与 SQLite 事务叠加，保证不互相覆盖。"""
    os.makedirs(os.path.abspath(audit_dir), exist_ok=True)
    path = os.path.join(os.path.abspath(audit_dir), LOCK_NAME)
    fh = open(path, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute(
        "SELECT value FROM aud_meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO aud_meta(key, value) VALUES (?,?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def request_hash(kind: str, payload: dict) -> str:
    """请求内容的规范化摘要（不含 request_id 本身）。"""
    return hashlib.sha256(
        canonical_json({"kind": kind, **payload}).encode("utf-8")
    ).hexdigest()


def digest_id_sequence(doc_ids) -> str:
    """按给定顺序把档号串成稳定摘要。

    写成 ``doc_id.encode('utf-8') + 0x1f`` 的重复；由于调用方按
    UTF-8 字节序排序，SQLite 行顺序、查询计划与输入文件排列都不影响结果。
    """
    h = hashlib.sha256()
    for doc_id in doc_ids:
        h.update(doc_id.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def parse_int(value, field: str, *, minimum: Optional[int] = None,
              maximum: Optional[int] = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise AuditError("BAD_REQUEST", f"{field} 必须是整数")
    if minimum is not None and value < minimum:
        raise AuditError("BAD_REQUEST",
                         f"{field} 必须 >= {minimum}，实际 {value}")
    if maximum is not None and value > maximum:
        raise AuditError("BAD_REQUEST",
                         f"{field} 必须 <= {maximum}，实际 {value}")
    return value


def parse_str(value, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise AuditError("BAD_REQUEST", f"{field} 必须是非空字符串")
    return value


def require_request_id(request: dict) -> str:
    if not isinstance(request, dict):
        raise AuditError("BAD_REQUEST", "请求必须是 JSON 对象")
    rid = request.get("request_id")
    if not isinstance(rid, str) or not rid:
        raise AuditError("BAD_REQUEST", "request_id 必须是非空字符串")
    return rid


def find_request(conn: sqlite3.Connection, request_id: str):
    return conn.execute(
        "SELECT * FROM aud_request WHERE request_id=?", (request_id,)
    ).fetchone()
