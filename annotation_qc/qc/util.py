# -*- coding: utf-8 -*-
"""通用辅助：稳定标识、规范化 JSON、结构错误对象与常量。

所有标识都基于内容的 SHA-256，而不是数据库自增号或团队局部 ID。
合法记录重排不会改变这些标识。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional

# 记录种类（标注包内 record["kind"]）
KIND_REVIEW = "review"
KIND_FRAGMENT = "fragment"
KIND_RELATION = "relation"

# 关系表态
SIGN_FOR = "for"
SIGN_AGAINST = "against"
VALID_SIGNS = (SIGN_FOR, SIGN_AGAINST)

# 区域内候选/票的语义
CANDIDATE_NONE = "\u0000NO_FRAGMENT"  # 「无片段」候选的内部标签
VOTE_CANDIDATE = "candidate"
VOTE_NONE = "no_fragment"
VOTE_CONFLICT = "self_contradiction"

# 复核项种类
REVIEW_REGION = "region"
REVIEW_RELATION = "relation"

# 关系端点被阻断的原因
BLOCK_PENDING = "endpoint_region_pending"
BLOCK_NONE_WON = "endpoint_no_fragment_confirmed"
BLOCK_OTHER_WON = "endpoint_other_candidate_won"

# JSONL 单行字节上限（16 MiB），防止畸形输入吃光内存
MAX_LINE_BYTES = 16 * 1024 * 1024


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    """生成确定性标识：prefix + 短哈希。

    parts 以可复现的规范化 JSON 拼接；重排输入只要逻辑内容不变，
    生成的标识就不变。
    """
    h = hashlib.sha256()
    for part in parts:
        h.update(canonical_json(part).encode("utf-8"))
        h.update(b"\x1f")
    return f"{prefix}_{h.hexdigest()[:32]}"


def canonical_json(obj: Any) -> str:
    """键排序、无多余空白的 JSON，用于哈希与规范化。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


@dataclass
class StructError:
    """结构错误：可定位到具体包/文件/记录位置。"""

    code: str
    message: str
    package_id: Optional[str] = None
    doc_id: Optional[str] = None
    team_id: Optional[str] = None
    file_name: Optional[str] = None
    line_no: Optional[int] = None
    record_index: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "package_id": self.package_id,
            "doc_id": self.doc_id,
            "team_id": self.team_id,
            "file": self.file_name,
            "line": self.line_no,
            "record_index": self.record_index,
        }


class RejectBuild(Exception):
    """构建被拒收：携带全部已收集的结构错误。"""

    def __init__(self, errors: Iterable[StructError]):
        self.errors = list(errors)
        super().__init__(f"build rejected with {len(self.errors)} structural error(s)")


def is_utf8_boundary(data: bytes, offset: int) -> bool:
    """判断 offset 是否为 UTF-8 字符边界（含 0 与 len(data)）。

    UTF-8 续字节范围为 0x80..0xBF；边界位置要么是 0，
    要么前一个字节不是续字节。
    """
    if offset < 0 or offset > len(data):
        return False
    if offset == 0 or offset == len(data):
        return True
    return (data[offset - 1] & 0xC0) != 0x80
