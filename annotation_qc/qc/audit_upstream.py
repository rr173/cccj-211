# -*- coding: utf-8 -*-
"""盲审只读上游：已登记的发布与公文身份。

审计只读取发布快照，不读取、也不以当前审定稿（HEAD）代替：

- ``release``          发布 ID -> 审定版本 rev_id / rev_seq / 公文集合；
- ``release_fragment`` 发布下的正式片段（含 seen_text 与 text_sha256）；
- ``release_relation`` 发布下存在的有向引用（有序端点）；
- ``source_doc``       公文身份（整篇底稿摘要、字节长与相对路径）。

上游以 ``mode=ro`` 打开；审计的所有写入都在自己的审计库里。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Iterator, Optional

from .audit_common import AuditError


class AuditUpstream:
    """只读上游句柄；每次调用点查/游标流式读取，不缓存整库。"""

    def __init__(self, workspace: str):
        path = os.path.join(os.path.abspath(workspace), "workspace.db")
        if not os.path.isfile(path):
            raise AuditError(
                "UPSTREAM_NOT_FOUND",
                f"adjudication workspace not found: {path}")
        uri = "file:" + path + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.row_factory = sqlite3.Row
        self.workspace = workspace
        self._check_schema()

    def _check_schema(self) -> None:
        needed = ("release", "release_fragment", "release_relation",
                  "source_doc")
        have = {
            r["name"] for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = [t for t in needed if t not in have]
        if missing:
            raise AuditError(
                "UPSTREAM_INCOMPATIBLE",
                "workspace lacks release tables required by blind audit",
                {"missing_tables": missing})

    def close(self) -> None:
        self.conn.close()

    # -- 发布 ---------------------------------------------------------------
    def release_info(self, release_id: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT release_id, rev_id, rev_seq, doc_ids_json"
            " FROM release WHERE release_id=?",
            (release_id,)).fetchone()
        if row is None:
            return None
        return {
            "release_id": row["release_id"],
            "rev_id": row["rev_id"],
            "rev_seq": row["rev_seq"],
            "doc_ids": json.loads(row["doc_ids_json"]),
        }

    # -- 公文身份（整篇底稿摘要） ------------------------------------------
    def doc_identity(self, doc_id: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT doc_id, sha256, byte_length, rel_path, abs_path"
            " FROM source_doc WHERE doc_id=?",
            (doc_id,)).fetchone()
        if row is None:
            return None
        return {
            "doc_id": row["doc_id"],
            "sha256": row["sha256"],
            "byte_length": row["byte_length"],
            "rel_path": row["rel_path"],
            "abs_path": row["abs_path"],
        }

    # -- 抽样总体：发布内至少一条正式片段或引用的不同公文 ------------------
    def population_cursor(self, release_id: str) -> sqlite3.Cursor:
        """服务端游标：排序去重后的候选档号（不整表载入 Python）。"""
        return self.conn.execute(
            "SELECT doc_id FROM release_fragment WHERE release_id=?"
            " UNION"
            " SELECT doc_id FROM release_relation WHERE release_id=?"
            " ORDER BY doc_id",
            (release_id, release_id))

    # -- 发布下的正式片段（流式，按稳定序） --------------------------------
    def iter_fragments_for_doc(self, release_id: str,
                               doc_id: str) -> Iterator[sqlite3.Row]:
        return iter(self.conn.execute(
            "SELECT fragment_uid, doc_id, layer, label, start_byte, end_byte,"
            " text_sha256, seen_text FROM release_fragment"
            " WHERE release_id=? AND doc_id=?"
            " ORDER BY layer, start_byte, end_byte, label, fragment_uid",
            (release_id, doc_id)))

    # -- 发布下的正式引用（流式，按稳定序） --------------------------------
    def iter_relations_for_doc(self, release_id: str,
                               doc_id: str) -> Iterator[sqlite3.Row]:
        return iter(self.conn.execute(
            "SELECT doc_id, rel_type, from_fragment, to_fragment"
            " FROM release_relation WHERE release_id=? AND doc_id=?"
            " ORDER BY rel_type, from_fragment, to_fragment",
            (release_id, doc_id)))

    def fragment_doc(self, release_id: str,
                     fragment_uid: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT doc_id FROM release_fragment"
            " WHERE release_id=? AND fragment_uid=?",
            (release_id, fragment_uid)).fetchone()
        return row["doc_id"] if row else None
