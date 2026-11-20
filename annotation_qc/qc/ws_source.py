# -*- coding: utf-8 -*-
"""按库中摘要核对的原文存取。

人工划出的片段坐标由调用者提供，但**不能**接受工单自报的切片：
工作区按 ``doc_id`` 记录调用者提供的原文路径，实际打开文件后
- 先核对 SHA-256 与工作区登记的摘要一致；
- 再以字节坐标校验区间边界落在 UTF-8 字符边界；
- 最后从原文取出 ``seen_text``（工单里的同名字段不采用）。

原文始终以只读方式打开。大文件分块哈希，不整体载入内存。
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional

from .source import SourceStore
from .ws_common import WsError


class WorkspaceSources:
    def __init__(self, conn, manifest_path: Optional[str] = None,
                 source_root: Optional[str] = None):
        self.conn = conn
        self._store: Optional[SourceStore] = None
        self._cache: dict[str, tuple[bool, str]] = {}
        if manifest_path is not None:
            self._store = SourceStore(manifest_path)
        self._root = os.path.abspath(source_root) if source_root else None

    # ------------------------------------------------------------------
    # 路径绑定与校验
    # ------------------------------------------------------------------

    def bind_from_manifest(self, manifest_path: str) -> None:
        self._store = SourceStore(manifest_path)
        self._root = None

    def doc_row(self, doc_id: str):
        row = self.conn.execute(
            "SELECT * FROM source_doc WHERE doc_id=?", (doc_id,)
        ).fetchone()
        if row is None:
            raise WsError("UNKNOWN_DOC", f"document not in workspace: {doc_id}")
        return row

    def _abs_path(self, doc_id: str) -> str:
        row = self.doc_row(doc_id)
        if row["abs_path"]:
            return row["abs_path"]
        if self._store is not None and doc_id in self._store:
            return self._store.get(doc_id).abs_path
        if self._root is not None and row["rel_path"]:
            return os.path.join(self._root, row["rel_path"])
        raise WsError(
            "SOURCE_UNAVAILABLE",
            f"no source path bound for doc {doc_id}; provide --manifest"
            " or --source-root",
        )

    def ensure_verified(self, doc_id: str) -> str:
        """核对原文摘要；返回实算摘要。核对失败的文档不允许人工划片段。"""
        if doc_id in self._cache:
            ok, digest = self._cache[doc_id]
            if not ok:
                raise WsError(
                    "SOURCE_DIGEST_MISMATCH",
                    f"doc {doc_id} source digest does not match workspace",
                )
            return digest
        row = self.doc_row(doc_id)
        path = self._abs_path(doc_id)
        if not os.path.isfile(path):
            raise WsError("SOURCE_UNAVAILABLE", f"source file missing: {path}")
        actual, size = _digest_file(path)
        ok = actual == row["sha256"] and size == row["byte_length"]
        self._cache[doc_id] = (ok, actual)
        self.conn.execute(
            "UPDATE source_doc SET abs_path=?, verified_sha=?, verified=?"
            " WHERE doc_id=?",
            (path, actual, 1 if ok else 0, doc_id),
        )
        if not ok:
            raise WsError(
                "SOURCE_DIGEST_MISMATCH",
                f"doc {doc_id}: expected {row['sha256']}, actual {actual}",
            )
        return actual

    # ------------------------------------------------------------------
    # 区间核验与取原文
    # ------------------------------------------------------------------

    def check_span(self, doc_id: str, start: int, end: int) -> tuple[bool, Optional[str]]:
        row = self.doc_row(doc_id)
        path = self._abs_path(doc_id)
        self.ensure_verified(doc_id)
        size = row["byte_length"]
        if not (0 <= start < end <= size):
            return False, "range out of bounds or empty"
        with open(path, "rb", opener=_ro) as fh:
            left_ok = True
            if start > 0:
                fh.seek(start - 1)
                left_ok = (fh.read(1)[0] & 0xC0) != 0x80
            right_ok = True
            if end < size:
                fh.seek(end)
                right_ok = (fh.read(1)[0] & 0xC0) != 0x80
        if not left_ok or not right_ok:
            return False, "range endpoint splits a UTF-8 character"
        return True, None

    def read_span(self, doc_id: str, start: int, end: int) -> bytes:
        self.ensure_verified(doc_id)
        path = self._abs_path(doc_id)
        with open(path, "rb", opener=_ro) as fh:
            fh.seek(start)
            return fh.read(end - start)

    def text_for(self, doc_id: str, start: int, end: int) -> str:
        raw = self.read_span(doc_id, start, end)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:  # pragma: no cover - 边界已校验
            raise WsError("BAD_SOURCE_TEXT", f"span is not valid UTF-8: {exc}")


def _ro(path, flags):
    return os.open(path, os.O_RDONLY)


def _digest_file(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb", opener=_ro) as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size
