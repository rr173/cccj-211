# -*- coding: utf-8 -*-
"""盲审证据的原文存取：按批次绑定的底稿摘要核对，不接受调用者自报切片。

审核证据由调用者给出 ``(doc_id, start, end, text)``。审计库在创建批次时
已把发布绑定的公文身份（``source_doc.sha256`` 与 ``byte_length``）固定到
抽样名单，这里：

- 先按批次登记的摘要核对实际文件（分块 SHA-256，不整体载入内存）；
- 再校验字节区间非空、不越界、两端落在 UTF-8 字符边界；
- 最后只读取出原文，要求证据文本与之逐字节相同（不做归一化）。

原文始终以只读方式打开。文件位置按以下顺序解析：
调用方 ``--manifest``（SourceStore）→ 批次快照里的绝对路径 →
调用方 ``--source-root`` 拼上构建时记录的相对路径。
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional

from .audit_common import AuditError
from .source import SourceStore


def _readonly_opener(path, flags):
    return os.open(path, os.O_RDONLY)


def _digest_file(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb", opener=_readonly_opener) as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


class AuditSources:
    """单个批次的原文存取器（按审计库登记的底稿摘要核对）。"""

    def __init__(self, conn, batch_id: str,
                 manifest_path: Optional[str] = None,
                 source_root: Optional[str] = None):
        self.conn = conn
        self.batch_id = batch_id
        self._store: Optional[SourceStore] = None
        self._root = os.path.abspath(source_root) if source_root else None
        self._docs: dict[str, dict] = {}
        self._digest_cache: dict[str, tuple[bool, str]] = {}
        if manifest_path is not None:
            self._store = SourceStore(manifest_path)
        for row in conn.execute(
            "SELECT doc_id, source_sha256, byte_length, rel_path, abs_path"
            " FROM aud_sample_doc WHERE batch_id=?", (batch_id,)
        ):
            self._docs[row["doc_id"]] = dict(row)

    # ------------------------------------------------------------------
    def doc_ids(self) -> list[str]:
        return sorted(self._docs)

    def doc_row(self, doc_id: str) -> dict:
        try:
            return self._docs[doc_id]
        except KeyError:
            raise AuditError(
                "EVIDENCE_DOC_NOT_IN_BATCH",
                f"document {doc_id!r} is not in batch {self.batch_id}")

    def _abs_path(self, doc_id: str) -> str:
        row = self.doc_row(doc_id)
        if row["abs_path"]:
            return row["abs_path"]
        if self._store is not None and doc_id in self._store:
            return self._store.get(doc_id).abs_path
        if self._root is not None and row["rel_path"]:
            return os.path.join(self._root, row["rel_path"])
        # 最后尝试把构建时相对路径当作相对当前目录
        if row["rel_path"] and os.path.isfile(row["rel_path"]):
            return os.path.abspath(row["rel_path"])
        raise AuditError(
            "SOURCE_UNAVAILABLE",
            f"no source path bound for doc {doc_id}; provide --manifest"
            " or --source-root",
            {"doc_id": doc_id})

    def ensure_verified(self, doc_id: str) -> str:
        if doc_id in self._digest_cache:
            ok, digest = self._digest_cache[doc_id]
            if not ok:
                raise AuditError(
                    "SOURCE_DIGEST_MISMATCH",
                    f"doc {doc_id} source digest does not match the release"
                    " binding", {"doc_id": doc_id})
            return digest
        row = self.doc_row(doc_id)
        path = self._abs_path(doc_id)
        if not os.path.isfile(path):
            raise AuditError("SOURCE_UNAVAILABLE", f"source file missing: {path}",
                             {"doc_id": doc_id})
        actual, size = _digest_file(path)
        ok = actual == row["source_sha256"] and size == row["byte_length"]
        self._digest_cache[doc_id] = (ok, actual)
        if not ok:
            raise AuditError(
                "SOURCE_DIGEST_MISMATCH",
                f"doc {doc_id}: expected {row['source_sha256']}, actual"
                f" {actual} (size {size} vs {row['byte_length']})",
                {"doc_id": doc_id, "expected": row["source_sha256"],
                 "actual": actual})
        return actual

    # ------------------------------------------------------------------
    def check_span(self, doc_id: str, start: int, end: int) -> Optional[str]:
        """返回错误说明；合法返回 None。

        半开区间 [start, end) 的两端都必须是 UTF-8 字符边界：端点处字节
        不能是续字节（0x80..0xBF），文件尾除外。只检查端点处字节是判定
        边界的充分必要条件，而不是检查相邻字节。
        """
        row = self.doc_row(doc_id)
        size = row["byte_length"]
        if not (0 <= start < end <= size):
            return "range out of bounds or empty"
        self.ensure_verified(doc_id)
        path = self._abs_path(doc_id)
        with open(path, "rb", opener=_readonly_opener) as fh:
            fh.seek(start)
            at_start = fh.read(1)
            start_ok = bool(at_start) and (at_start[0] & 0xC0) != 0x80
            end_ok = True
            if end < size:
                fh.seek(end)
                end_ok = (fh.read(1)[0] & 0xC0) != 0x80
        if not start_ok or not end_ok:
            return "range endpoint splits a UTF-8 character"
        return None

    def read_span(self, doc_id: str, start: int, end: int) -> bytes:
        self.ensure_verified(doc_id)
        path = self._abs_path(doc_id)
        with open(path, "rb", opener=_readonly_opener) as fh:
            fh.seek(start)
            return fh.read(end - start)

    def read_text(self, doc_id: str) -> str:
        """分块读取整篇原文用于盲审导出（大文件不整体驻留）。"""
        self.ensure_verified(doc_id)
        path = self._abs_path(doc_id)
        parts = []
        with open(path, "rb", opener=_readonly_opener) as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                parts.append(chunk)
        try:
            return b"".join(parts).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AuditError("BAD_SOURCE_TEXT",
                             f"source is not valid UTF-8: {exc}",
                             {"doc_id": doc_id})
