# -*- coding: utf-8 -*-
"""原文清单与字节级原文存取。

原文按字节原样读取（严格 UTF-8，不做归一化）。构建时会实际核对：
- SHA-256 与清单一致；
- 区间两端落在 UTF-8 字符边界；
- 片段所见原文与对应字节逐字节相同。

读取使用只读方式打开；大文件分块流式哈希，避免整体载入内存。
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Iterator

from .util import StructError

CHUNK = 1024 * 1024


@dataclass(frozen=True)
class SourceDoc:
    doc_id: str
    rel_path: str
    sha256: str
    abs_path: str
    size: int


class SourceStore:
    """原文仓库：持有清单，按需只读打开文件，缓存尺寸与已验证摘要。"""

    def __init__(self, manifest_path: str):
        self.manifest_path = os.path.abspath(manifest_path)
        self.base_dir = os.path.dirname(self.manifest_path)
        with open(manifest_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        docs = raw.get("documents")
        if not isinstance(docs, list) or not docs:
            raise ValueError("manifest must contain a non-empty 'documents' list")
        self._docs: dict[str, SourceDoc] = {}
        for entry in docs:
            doc_id = entry.get("doc_id")
            rel_path = entry.get("path")
            digest = entry.get("sha256")
            if not isinstance(doc_id, str) or not doc_id:
                raise ValueError("manifest entry missing doc_id")
            if not isinstance(rel_path, str) or not rel_path:
                raise ValueError(f"manifest {doc_id}: missing path")
            if not isinstance(digest, str) or len(digest) != 64:
                raise ValueError(f"manifest {doc_id}: bad sha256")
            if os.path.isabs(rel_path) or ".." in rel_path.replace("\\", "/").split("/"):
                raise ValueError(f"manifest {doc_id}: path must be relative and contained")
            abs_path = os.path.abspath(os.path.join(self.base_dir, rel_path))
            if not abs_path.startswith(self.base_dir + os.sep):
                raise ValueError(f"manifest {doc_id}: path escapes manifest directory")
            if not os.path.isfile(abs_path):
                raise ValueError(f"manifest {doc_id}: source file not found: {rel_path}")
            if doc_id in self._docs:
                raise ValueError(f"manifest duplicate doc_id: {doc_id}")
            self._docs[doc_id] = SourceDoc(
                doc_id=doc_id,
                rel_path=rel_path,
                sha256=digest.lower(),
                abs_path=abs_path,
                size=os.path.getsize(abs_path),
            )

    def __contains__(self, doc_id: str) -> bool:
        return doc_id in self._docs

    def get(self, doc_id: str) -> SourceDoc:
        return self._docs[doc_id]

    def doc_ids(self) -> list[str]:
        return sorted(self._docs)

    def stream_bytes(self, doc_id: str) -> Iterator[bytes]:
        """分块流式产出原文字节（只读打开）。"""
        doc = self._docs[doc_id]
        with open(doc.abs_path, "rb", opener=_readonly_opener) as fh:
            while True:
                chunk = fh.read(CHUNK)
                if not chunk:
                    break
                yield chunk

    def verify_digest(self, doc_id: str) -> tuple[bool, str, int]:
        """实际重算 SHA-256 并与清单比对，返回 (是否一致, 实算摘要, 字节数)。"""
        h = hashlib.sha256()
        size = 0
        for chunk in self.stream_bytes(doc_id):
            h.update(chunk)
            size += len(chunk)
        actual = h.hexdigest()
        return actual == self._docs[doc_id].sha256, actual, size

    def read_span(self, doc_id: str, start: int, end: int) -> bytes:
        """只读 seek 读取半开区间字节，不做解码/归一化。"""
        doc = self._docs[doc_id]
        with open(doc.abs_path, "rb", opener=_readonly_opener) as fh:
            fh.seek(start)
            return fh.read(end - start)

    def check_span(
        self, doc_id: str, start: int, end: int
    ) -> tuple[bool, str | None]:
        """区间边界检查：非空、不越界、两端为 UTF-8 字符边界。

        半开区间 [start, end)：
        - start 指向某字符首字节：start==0 或前一字节不是续字节；
        - end 指向下一字符首字节（或文件尾）：end==len 或该位置不是续字节。
        """
        doc = self._docs[doc_id]
        if not (0 <= start < end <= doc.size):
            return False, "range out of bounds or empty"
        with open(doc.abs_path, "rb", opener=_readonly_opener) as fh:
            if start == 0:
                left_ok = True
            else:
                fh.seek(start - 1)
                left_ok = (fh.read(1)[0] & 0xC0) != 0x80
            right_ok = True
            if end < doc.size:
                fh.seek(end)
                right_ok = (fh.read(1)[0] & 0xC0) != 0x80
        if not left_ok or not right_ok:
            return False, "range endpoint splits a UTF-8 character"
        return True, None

    def verify_seen_text(
        self, doc_id: str, start: int, end: int, seen_text: str
    ) -> tuple[bool, str]:
        """所见原文必须与对应字节完全一致（不做 Unicode 归一化）。"""
        actual = self.read_span(doc_id, start, end)
        try:
            seen_bytes = seen_text.encode("utf-8")
        except (UnicodeEncodeError, AttributeError):
            return False, "seen_text is not valid text"
        if seen_bytes != actual:
            return False, "seen_text does not byte-match the source span"
        return True, ""


def _readonly_opener(path, flags):
    # 强制只读打开
    return os.open(path, os.O_RDONLY)


def digest_error(doc_id: str, expected: str, actual: str) -> StructError:
    return StructError(
        code="SOURCE_DIGEST_MISMATCH",
        message=(
            f"doc {doc_id}: sha256 mismatch, manifest={expected}, actual={actual}"
        ),
        doc_id=doc_id,
    )
