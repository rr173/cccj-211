# -*- coding: utf-8 -*-
"""裁决工作区（只读上游）存取。

台账只读上游的三张表：

- ``release``          已登记发布（发布 ID -> 审定版本 rev_id / 公文集合）；
- ``release_fragment`` 发布下的审定片段（含 seen_text 与 text_sha256）；
- ``source_doc``       公文身份（整篇底稿的登记摘要与字节长）。

选入公文时按「档号 + 发布 ID」读取该发布下的审定片段与公文身份，
不读取、也不以当前审定稿（HEAD 版本状态）代替。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import List, Optional

from .ledger_common import LedgerError


class Upstream:
    """只读上游句柄；每次调用点查，不缓存整库。"""

    def __init__(self, workspace: str):
        path = os.path.join(os.path.abspath(workspace), "workspace.db")
        if not os.path.isfile(path):
            raise LedgerError(
                "UPSTREAM_NOT_FOUND",
                f"adjudication workspace not found: {path}")
        uri = "file:" + path + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.row_factory = sqlite3.Row
        self.workspace = workspace

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
            "SELECT doc_id, sha256, byte_length FROM source_doc WHERE doc_id=?",
            (doc_id,)).fetchone()
        if row is None:
            return None
        return {
            "doc_id": row["doc_id"],
            "sha256": row["sha256"],
            "byte_length": row["byte_length"],
        }

    # -- 发布下的审定片段（按建账选定的标注层） -----------------------------
    def release_fragments(self, release_id: str, doc_id: str,
                          layer: str) -> List[dict]:
        rows = self.conn.execute(
            "SELECT fragment_uid, label, start_byte, end_byte,"
            "       text_sha256, seen_text"
            " FROM release_fragment"
            " WHERE release_id=? AND doc_id=? AND layer=?"
            " ORDER BY start_byte, end_byte, fragment_uid",
            (release_id, doc_id, layer)).fetchall()
        return [
            {
                "fragment_uid": r["fragment_uid"],
                "label": r["label"],
                "start_byte": r["start_byte"],
                "end_byte": r["end_byte"],
                "text_sha256": r["text_sha256"],
                "seen_text": r["seen_text"],
            }
            for r in rows
        ]
