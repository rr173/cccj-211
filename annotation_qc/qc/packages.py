# -*- coding: utf-8 -*-
"""标注包发现与流式读取（JSONL，可分包）。

两种物理形态：
1. 单个 ``*.jsonl`` 文件：首行为包头，其余为记录；
2. 目录（bundle）：含一个或多个 ``*.jsonl`` 文件，按相对名排序后，
   包头必须位于第一个文件的首行；其余文件不得再出现包头。

关系记录允许前向引用（端点片段在后面的文件/行才出现），解析在构建阶段做。
读取全程逐行流式，单行有字节上限，避免畸形输入撑爆内存。

逻辑标识与合法记录重排无关：
- 包标识只取 (doc_id, team_id, package_id)；
- 记录标识取规范化后的记录内容（同内容多次出现按出现序号区分，
  重排后标识集合不变）；
- 片段标识取包内局部 ID（包内唯一性在校验阶段强制）。
物理内容哈希与文件/行号仅作证据保留。
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Iterator, Optional

from .util import (
    KIND_FRAGMENT,
    KIND_RELATION,
    KIND_REVIEW,
    MAX_LINE_BYTES,
    StructError,
    VALID_SIGNS,
    stable_id,
)

HEADER_KIND = "package"
JSONL_SUFFIX = ".jsonl"


class PackageFormatError(Exception):
    """JSONL 物理格式错误，携带可定位信息。"""

    def __init__(self, message: str, file_name: Optional[str] = None,
                 line_no: Optional[int] = None):
        super().__init__(message)
        self.file_name = file_name
        self.line_no = line_no


@dataclass(frozen=True)
class StreamedRecord:
    payload: dict
    file_name: str
    line_no: int
    record_index: int  # 包内记录序号（不含包头），从 0 起


@dataclass(frozen=True)
class DiscoveredPackage:
    package_uid: str
    package_id: str
    doc_id: str
    team_id: str
    source_digest: Optional[str]
    root_path: str          # 目录或单文件绝对路径
    is_bundle: bool
    jsonl_files: tuple      # (相对名, 绝对路径) 排序后
    content_hash: str       # 物理字节证据（与逻辑标识无关）


def discover_packages(input_root: str) -> list[DiscoveredPackage]:
    """确定性地发现输入根下的全部标注包，按路径排序。"""
    root = os.path.abspath(input_root)
    if not os.path.exists(root):
        raise FileNotFoundError(input_root)
    found: list[DiscoveredPackage] = []
    if os.path.isfile(root):
        if not root.endswith(JSONL_SUFFIX):
            raise PackageFormatError(f"single-file package must end with {JSONL_SUFFIX}")
        found.append(_load_single(root))
        return found

    entries = sorted(os.listdir(root))
    for name in entries:
        abs_path = os.path.join(root, name)
        if os.path.isfile(abs_path) and name.endswith(JSONL_SUFFIX):
            found.append(_load_single(abs_path))
        elif os.path.isdir(abs_path):
            jsonls = _collect_jsonl(abs_path)
            if jsonls:
                found.append(_load_bundle(abs_path, jsonls))
    if not found:
        raise PackageFormatError(f"no annotation package found under {root}")
    return found


def _collect_jsonl(bundle_dir: str) -> tuple:
    out = []
    for dirpath, dirnames, filenames in os.walk(bundle_dir):
        dirnames.sort()
        for fn in sorted(filenames):
            if fn.endswith(JSONL_SUFFIX):
                abs_path = os.path.join(dirpath, fn)
                rel = os.path.relpath(abs_path, bundle_dir).replace(os.sep, "/")
                out.append((rel, abs_path))
    return tuple(sorted(out, key=lambda x: x[0]))


def _load_single(path: str) -> DiscoveredPackage:
    header = _read_header(path, os.path.basename(path))
    content_hash, _ = _hash_files(((os.path.basename(path), path),))
    uid = stable_id("pkg", header["doc_id"], header["team_id"], header["package_id"])
    return DiscoveredPackage(
        package_uid=uid,
        package_id=header["package_id"],
        doc_id=header["doc_id"],
        team_id=header["team_id"],
        source_digest=header.get("source_digest"),
        root_path=path,
        is_bundle=False,
        jsonl_files=((os.path.basename(path), path),),
        content_hash=content_hash,
    )


def _load_bundle(bundle_dir: str, jsonl_files: tuple) -> DiscoveredPackage:
    first_rel, first_abs = jsonl_files[0]
    header = _read_header(first_abs, first_rel)
    # 其余文件禁止再出现包头
    for rel, abs_path in jsonl_files[1:]:
        with open(abs_path, "rb") as fh:
            first = fh.readline()
        if first.strip():
            obj = _decode_line(first, rel, 1)
            if obj.get("kind") == HEADER_KIND:
                raise PackageFormatError("duplicate package header in bundle", rel, 1)
    content_hash, _ = _hash_files(jsonl_files)
    uid = stable_id("pkg", header["doc_id"], header["team_id"], header["package_id"])
    return DiscoveredPackage(
        package_uid=uid,
        package_id=header["package_id"],
        doc_id=header["doc_id"],
        team_id=header["team_id"],
        source_digest=header.get("source_digest"),
        root_path=bundle_dir,
        is_bundle=True,
        jsonl_files=jsonl_files,
        content_hash=content_hash,
    )


def _hash_files(jsonl_files: tuple) -> tuple[str, int]:
    h = hashlib.sha256()
    total = 0
    for rel, abs_path in jsonl_files:
        h.update(rel.encode("utf-8"))
        h.update(b"\x00")
        with open(abs_path, "rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
                total += len(chunk)
        h.update(b"\x00")
    return h.hexdigest(), total


def _read_header(path: str, rel_name: str) -> dict:
    with open(path, "rb") as fh:
        line = fh.readline()
    if not line:
        raise PackageFormatError("empty jsonl file (missing package header)", rel_name, 1)
    obj = _decode_line(line, rel_name, 1)
    if obj.get("kind") != HEADER_KIND:
        raise PackageFormatError(
            "first record must be a package header", rel_name, 1
        )
    for field in ("package_id", "doc_id", "team_id"):
        if not isinstance(obj.get(field), str) or not obj[field]:
            raise PackageFormatError(
                f"header missing non-empty string field: {field}", rel_name, 1
            )
    digest = obj.get("source_digest")
    if digest is not None and (not isinstance(digest, str) or len(digest) != 64):
        raise PackageFormatError("header source_digest must be 64-char hex", rel_name, 1)
    return obj


def _decode_line(line: bytes, rel_name: str, line_no: int) -> dict:
    if len(line) > MAX_LINE_BYTES:
        raise PackageFormatError(
            f"line exceeds {MAX_LINE_BYTES} bytes", rel_name, line_no
        )
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PackageFormatError(f"invalid UTF-8: {exc}", rel_name, line_no)
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PackageFormatError(f"invalid JSON: {exc.msg}", rel_name, line_no)
    if not isinstance(obj, dict):
        raise PackageFormatError("record must be a JSON object", rel_name, line_no)
    return obj


def stream_package(pkg: DiscoveredPackage) -> Iterator[StreamedRecord]:
    """按确定性文件顺序逐行流式产出记录（不含包头）。"""
    record_index = 0
    for idx, (rel, abs_path) in enumerate(pkg.jsonl_files):
        line_no = 0
        with open(abs_path, "rb") as fh:
            for raw in fh:
                line_no += 1
                if idx == 0 and line_no == 1:
                    continue  # 包头
                if not raw.strip():
                    continue  # 允许空行
                obj = _decode_line(raw, rel, line_no)
                kind = obj.get("kind")
                if kind == HEADER_KIND:
                    raise PackageFormatError(
                        "package header only allowed as first record", rel, line_no
                    )
                if kind not in (KIND_REVIEW, KIND_FRAGMENT, KIND_RELATION):
                    raise PackageFormatError(
                        f"unknown record kind: {kind!r}", rel, line_no
                    )
                yield StreamedRecord(obj, rel, line_no, record_index)
                record_index += 1


# ---------------------------------------------------------------------------
# 记录级结构校验（与策略/原文相关的深度校验在 build 阶段完成）
# ---------------------------------------------------------------------------

def check_record_shape(rec: StreamedRecord) -> Optional[StructError]:
    """只做形状/类型级校验，返回结构错误或 None。"""
    p = rec.payload
    kind = p["kind"]
    loc = dict(file_name=rec.file_name, line_no=rec.line_no,
               record_index=rec.record_index)

    def err(code: str, msg: str) -> StructError:
        return StructError(code=code, message=msg, **loc)

    if kind == KIND_REVIEW:
        ranges = p.get("ranges")
        if not isinstance(ranges, list) or not ranges:
            return err("BAD_REVIEW", "review.ranges must be a non-empty list")
        for rng in ranges:
            if (not isinstance(rng, list) or len(rng) != 2
                    or not all(isinstance(x, int) and not isinstance(x, bool)
                               for x in rng)):
                return err("BAD_REVIEW", "review range must be [int, int]")
            if rng[0] >= rng[1]:
                return err("BAD_REVIEW", "review range must be non-empty half-open")
        if not isinstance(p.get("layer"), str) or not p["layer"]:
            return err("BAD_REVIEW", "review.layer missing")
        return None

    if kind == KIND_FRAGMENT:
        for field_name in ("id", "layer", "label", "seen_text"):
            if not isinstance(p.get(field_name), str) or not p[field_name]:
                return err("BAD_FRAGMENT", f"fragment.{field_name} missing/empty")
        for field_name in ("start", "end"):
            v = p.get(field_name)
            if not isinstance(v, int) or isinstance(v, bool):
                return err("BAD_FRAGMENT", f"fragment.{field_name} must be int")
        if p["start"] >= p["end"]:
            return err("BAD_FRAGMENT", "fragment range must be non-empty half-open")
        return None

    # relation
    for field_name in ("type", "from_id", "to_id"):
        if not isinstance(p.get(field_name), str) or not p[field_name]:
            return err("BAD_RELATION", f"relation.{field_name} missing/empty")
    if p.get("sign") not in VALID_SIGNS:
        return err("BAD_RELATION", "relation.sign must be 'for' or 'against'")
    return None


def record_logical_uid(package_uid: str, payload: dict, occurrence: int) -> str:
    """内容派生的记录标识；重排合法记录不改变标识集合。"""
    return stable_id("rec", package_uid, payload, occurrence)


def proposal_uid(package_uid: str, local_id: str) -> str:
    return stable_id("prop", package_uid, local_id)
