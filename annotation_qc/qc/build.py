# -*- coding: utf-8 -*-
"""构建阶段：流式校验 -> SQLite 暂存 -> 归组/覆盖/计票 -> 原子发布。

内存边界：
- 标注包逐行流式读取，原始记录直接落库，不把整包/整文档驻留内存；
- 归组采用按 (doc, layer, start, end) 排序后的单遍扫描线，
  显式不枚举提议两两交叠（不生成平方级交叠边）；
- 覆盖资格以包为单位按层排序取出审核区间，合并后用双指针扫区域，
  不为每个区域反复全表扫描审核记录；
- 计票与引用解析全部走带索引的 SQLite 流式查询，批次落盘。

失败语义：
- 任何结构错误都不发布；正式输出路径上的既有完整结果不被替换；
- 暂存库与 WAL 位于输出旁的临时文件，失败即清理；
- 拒收报告（逐项结构错误，含文件/行/记录序号）写到独立文件。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from .packages import (
    DiscoveredPackage,
    PackageFormatError,
    check_record_shape,
    discover_packages,
    proposal_uid,
    record_logical_uid,
    stream_package,
)
from .policy import Policy
from .source import SourceStore
from .util import (
    BLOCK_NONE_WON,
    BLOCK_OTHER_WON,
    BLOCK_PENDING,
    CANDIDATE_NONE,
    KIND_FRAGMENT,
    KIND_RELATION,
    KIND_REVIEW,
    REVIEW_REGION,
    REVIEW_RELATION,
    SIGN_AGAINST,
    SIGN_FOR,
    StructError,
    VOTE_CANDIDATE,
    VOTE_CONFLICT,
    VOTE_NONE,
    canonical_json,
    sha256_hex,
    stable_id,
)

MAX_ERRORS = 1000
INSERT_BATCH = 2000


@dataclass
class BuildResult:
    output_db: str
    reject_report: Optional[str]
    errors: list
    counters: dict


class _ErrorCollector:
    def __init__(self):
        self.errors: list[StructError] = []
        self.truncated = 0

    def add(self, err: StructError) -> bool:
        if len(self.errors) < MAX_ERRORS:
            self.errors.append(err)
            return True
        self.truncated += 1
        return False

    def __bool__(self):
        return bool(self.errors)


def build(
    manifest_path: str,
    packages_root: str,
    output_db: str,
    policy: Policy,
    reject_report: Optional[str] = None,
) -> BuildResult:
    output_db = os.path.abspath(output_db)
    sources = SourceStore(manifest_path)
    errors = _ErrorCollector()
    counters = defaultdict(int)
    _BOUNDARY_SOURCES["store"] = sources
    _POLICY_HOLDER["policy"] = policy

    staging_path = _staging_path(output_db)
    if os.path.exists(staging_path):
        os.remove(staging_path)
    conn = sqlite3.connect(staging_path)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    _init_schema(conn)

    try:
        _stage_sources(conn, sources, errors)
        packages = _discover(packages_root, errors)
        if packages is not None:
            _stage_packages(conn, sources, policy, packages, errors, counters)
            # 即使已有片段级错误也继续做端点校验，尽量一次报全所有结构问题
            _validate_relation_endpoints(conn, errors)
        if errors:
            conn.close()
            _cleanup_staging(staging_path)
            report = _write_reject_report(
                errors, reject_report, output_db, staging_path
            )
            return BuildResult(output_db, report, errors.errors, dict(counters))

        _group_regions(conn, counters)
        _compute_eligibility(conn, counters)
        _tally_regions(conn, policy, counters)
        _resolve_and_tally_relations(conn, policy, counters)
        conn.executemany(
            "INSERT INTO run_counter(name, value) VALUES (?,?)",
            sorted((k, v) for k, v in counters.items()),
        )
        _finalize_meta(conn, sources, packages, policy)
        conn.commit()
        _checkpoint(conn)
        conn.close()
        _publish(staging_path, output_db)
        return BuildResult(output_db, None, [], dict(counters))
    except PackageFormatError as exc:
        # 流式读取中的物理格式错误：同样按拒收处理，绝不发布半份结果
        conn.close()
        _cleanup_staging(staging_path)
        err = StructError(
            code="PACKAGE_FORMAT",
            message=str(exc),
            file_name=exc.file_name,
            line_no=exc.line_no,
        )
        errors.add(err)
        report = _write_reject_report(
            errors, reject_report, output_db, staging_path
        )
        return BuildResult(output_db, report, errors.errors, dict(counters))


# ---------------------------------------------------------------------------
# 暂存与发布
# ---------------------------------------------------------------------------

def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")  # ~20MB 页缓存上限


def _init_schema(conn: sqlite3.Connection) -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())


def _staging_path(output_db: str) -> str:
    out_dir = os.path.dirname(output_db) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, path = tempfile.mkstemp(
        prefix=".qc-staging-", suffix=".db", dir=out_dir
    )
    os.close(fd)
    os.remove(path)  # 让 sqlite 自己创建
    return path


def _cleanup_staging(staging_path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = staging_path + suffix
        if os.path.exists(p):
            os.remove(p)


def _checkpoint(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=DELETE")


def _publish(staging_path: str, output_db: str) -> None:
    """原子替换：先落到同目录唯一临时名，再 os.replace。"""
    out_dir = os.path.dirname(output_db)
    fd, final_tmp = tempfile.mkstemp(prefix=".qc-publish-", suffix=".db", dir=out_dir)
    os.close(fd)
    os.remove(final_tmp)
    # 复制为独立文件（暂存库已 checkpoint 为单一 db 文件）
    with open(staging_path, "rb") as src, open(final_tmp, "wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(final_tmp, output_db)  # 原子替换；失败时旧库仍在
    _cleanup_staging(staging_path)


def _write_reject_report(
    errors: _ErrorCollector,
    reject_report: Optional[str],
    output_db: str,
    staging_path: str,
) -> str:
    if reject_report is None:
        reject_report = output_db + ".reject.json"
    payload = {
        "published": False,
        "error_count": len(errors.errors),
        "truncated": errors.truncated,
        "errors": [e.to_dict() for e in errors.errors],
    }
    with open(reject_report, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
    return reject_report


# ---------------------------------------------------------------------------
# 原文核对
# ---------------------------------------------------------------------------

def _stage_sources(
    conn: sqlite3.Connection, sources: SourceStore, errors: _ErrorCollector
) -> None:
    rows = []
    for doc_id in sources.doc_ids():
        doc = sources.get(doc_id)
        ok, actual, size = sources.verify_digest(doc_id)
        if not ok:
            errors.add(
                StructError(
                    code="SOURCE_DIGEST_MISMATCH",
                    message=(
                        f"doc {doc_id}: manifest={doc.sha256} actual={actual}"
                    ),
                    doc_id=doc_id,
                )
            )
        rows.append((doc_id, doc.rel_path, doc.sha256, size, actual, 1 if ok else 0))
    conn.executemany(
        "INSERT INTO source_doc(doc_id, rel_path, sha256, byte_length,"
        " verified_sha, verified) VALUES (?,?,?,?,?,?)",
        rows,
    )


def _discover(packages_root: str, errors: _ErrorCollector):
    try:
        return discover_packages(packages_root)
    except PackageFormatError as exc:
        errors.add(
            StructError(
                code="PACKAGE_FORMAT",
                message=str(exc),
                file_name=exc.file_name,
                line_no=exc.line_no,
            )
        )
        return None
    except FileNotFoundError as exc:
        errors.add(StructError(code="PACKAGE_NOT_FOUND", message=str(exc)))
        return None


# ---------------------------------------------------------------------------
# 包流式校验与落库
# ---------------------------------------------------------------------------

def _stage_packages(
    conn: sqlite3.Connection,
    sources: SourceStore,
    policy: Policy,
    packages: list[DiscoveredPackage],
    errors: _ErrorCollector,
    counters: dict,
) -> None:
    seen_doc_team: set[tuple] = set()
    for pkg in packages:
        if pkg.doc_id not in sources:
            errors.add(
                StructError(
                    code="UNKNOWN_DOC",
                    message=f"package {pkg.package_id} references unknown doc"
                    f" {pkg.doc_id}",
                    package_id=pkg.package_id,
                )
            )
            continue
        doc = sources.get(pkg.doc_id)
        if (pkg.doc_id, pkg.team_id) in seen_doc_team:
            errors.add(
                StructError(
                    code="DUPLICATE_PACKAGE",
                    message=(
                        f"duplicate package for doc={pkg.doc_id}"
                        f" team={pkg.team_id}"
                    ),
                    package_id=pkg.package_id,
                    doc_id=pkg.doc_id,
                    team_id=pkg.team_id,
                )
            )
            continue
        if pkg.source_digest is not None and pkg.source_digest.lower() != doc.sha256:
            errors.add(
                StructError(
                    code="WRONG_SOURCE_DIGEST",
                    message=(
                        f"package declares digest {pkg.source_digest} but source"
                        f" digest is {doc.sha256}"
                    ),
                    package_id=pkg.package_id,
                    doc_id=pkg.doc_id,
                    team_id=pkg.team_id,
                )
            )
            continue
        seen_doc_team.add((pkg.doc_id, pkg.team_id))
        conn.execute(
            "INSERT INTO package(package_uid, package_id, doc_id, team_id,"
            " source_digest, root_kind, content_hash) VALUES (?,?,?,?,?,?,?)",
            (
                pkg.package_uid,
                pkg.package_id,
                pkg.doc_id,
                pkg.team_id,
                pkg.source_digest,
                "bundle" if pkg.is_bundle else "jsonl",
                pkg.content_hash,
            ),
        )
        _stream_and_validate(conn, sources, policy, pkg, errors, counters)
        counters["packages_loaded"] += 1
        conn.commit()  # 每包一次提交，控制 WAL 体积


def _stream_and_validate(
    conn: sqlite3.Connection,
    sources: SourceStore,
    policy: Policy,
    pkg: DiscoveredPackage,
    errors: _ErrorCollector,
    counters: dict,
) -> None:
    """两遍流式处理，记录物理顺序不影响校验结果。

    第一遍：形状校验、重复记录检测，按种类缓存轻量元组（含文件/行位置），
    不在此阶段依赖记录间先后关系；
    第二遍：先合并全部审核区间，再校验片段（含越出审核范围）与关系，
    因此引用或片段先于审核/被引片段出现都能正确处理。

    仅缓存当前包，且记录以紧凑元组暂存；包边界即释放，工作内存有界。
    """
    payload_occurrences: dict[str, int] = defaultdict(int)
    reviews: list[tuple] = []   # (StreamedRecord, payload, rec_uid)
    fragments: list[tuple] = []
    relations: list[tuple] = []

    # ---- 第一遍：流式读取 + 形状/重复校验 ----
    for rec in stream_package(pkg):
        shape_err = check_record_shape(rec)
        if shape_err is not None:
            shape_err.package_id = pkg.package_id
            shape_err.doc_id = pkg.doc_id
            shape_err.team_id = pkg.team_id
            errors.add(shape_err)
            continue
        payload = rec.payload
        canonical = canonical_json(payload)
        occurrence = payload_occurrences[canonical]
        payload_occurrences[canonical] += 1
        if occurrence > 0:
            errors.add(
                StructError(
                    code="DUPLICATE_RECORD",
                    message="identical record appears more than once in package",
                    package_id=pkg.package_id,
                    doc_id=pkg.doc_id,
                    team_id=pkg.team_id,
                    file_name=rec.file_name,
                    line_no=rec.line_no,
                    record_index=rec.record_index,
                )
            )
            continue
        rec_uid = record_logical_uid(pkg.package_uid, payload, occurrence)
        counters["records_loaded"] += 1
        item = (rec, payload, rec_uid)
        if payload["kind"] == KIND_REVIEW:
            reviews.append(item)
        elif payload["kind"] == KIND_FRAGMENT:
            fragments.append(item)
        else:
            relations.append(item)

    record_rows = []
    review_rows = []
    proposal_rows = []
    relation_rows = []

    def flush():
        if record_rows:
            conn.executemany(
                "INSERT INTO record(record_uid, package_uid, doc_id, team_id,"
                " kind, file_name, line_no, record_index, payload_json)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                record_rows,
            )
            record_rows.clear()
        if review_rows:
            conn.executemany(
                "INSERT INTO review_range(record_uid, package_uid, doc_id,"
                " team_id, layer, start_byte, end_byte)"
                " VALUES (?,?,?,?,?,?,?)",
                review_rows,
            )
            review_rows.clear()
        if proposal_rows:
            conn.executemany(
                "INSERT INTO proposal(proposal_uid, record_uid, package_uid,"
                " doc_id, team_id, layer, local_id, label, start_byte, end_byte,"
                " seen_text) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                proposal_rows,
            )
            proposal_rows.clear()
        if relation_rows:
            conn.executemany(
                "INSERT INTO raw_relation(record_uid, package_uid, doc_id,"
                " team_id, rel_type, from_local, to_local, sign)"
                " VALUES (?,?,?,?,?,?,?,?)",
                relation_rows,
            )
            relation_rows.clear()

    def stage(rec, payload, rec_uid):
        record_rows.append(
            (
                rec_uid,
                pkg.package_uid,
                pkg.doc_id,
                pkg.team_id,
                payload["kind"],
                rec.file_name,
                rec.line_no,
                rec.record_index,
                canonical_json(payload),
            )
        )

    # ---- 第二遍 A：审核区间（先建立完整覆盖图） ----
    review_by_layer: dict[str, list[list[int]]] = defaultdict(list)
    for rec, payload, rec_uid in reviews:
        stage(rec, payload, rec_uid)
        _handle_review(
            conn, policy, pkg, rec, payload, rec_uid, review_by_layer,
            review_rows, errors,
        )
        if len(record_rows) >= INSERT_BATCH:
            flush()

    # ---- 第二遍 B：片段（覆盖图已完整，顺序无关） ----
    local_fragment_ids: set[str] = set()
    for rec, payload, rec_uid in fragments:
        stage(rec, payload, rec_uid)
        _handle_fragment(
            sources, policy, pkg, rec, payload, rec_uid,
            sources.get(pkg.doc_id).size,
            local_fragment_ids, review_by_layer, proposal_rows, errors,
        )
        if len(record_rows) >= INSERT_BATCH:
            flush()

    # ---- 第二遍 C：关系（端点存在性在构建后期统一校验，允许前向引用） ----
    for rec, payload, rec_uid in relations:
        stage(rec, payload, rec_uid)
        _handle_relation(policy, pkg, rec, payload, rec_uid, relation_rows, errors)
        if len(record_rows) >= INSERT_BATCH:
            flush()
    flush()


def _handle_review(
    conn, policy, pkg, rec, payload, rec_uid, review_by_layer, review_rows, errors
):
    layer = payload["layer"]
    if layer not in policy.layers:
        errors.add(
            StructError(
                code="UNKNOWN_LAYER",
                message=f"review uses undeclared layer {layer!r}",
                package_id=pkg.package_id,
                doc_id=pkg.doc_id,
                team_id=pkg.team_id,
                file_name=rec.file_name,
                line_no=rec.line_no,
                record_index=rec.record_index,
            )
        )
        return
    for start, end in payload["ranges"]:
        # 审核区间同样必须是字符边界
        ok, why = _check_boundary(conn, pkg.doc_id, start, end)
        if not ok:
            errors.add(
                StructError(
                    code="BAD_RANGE",
                    message=f"review range [{start},{end}): {why}",
                    package_id=pkg.package_id,
                    doc_id=pkg.doc_id,
                    team_id=pkg.team_id,
                    file_name=rec.file_name,
                    line_no=rec.line_no,
                    record_index=rec.record_index,
                )
            )
            continue
        review_rows.append(
            (
                rec_uid,
                pkg.package_uid,
                pkg.doc_id,
                pkg.team_id,
                layer,
                start,
                end,
            )
        )
        _merge_interval(review_by_layer[layer], start, end)


def _handle_fragment(
    sources, policy, pkg, rec, payload, rec_uid, doc_size,
    local_fragment_ids, review_by_layer, proposal_rows, errors
):
    layer = payload["layer"]
    start, end = payload["start"], payload["end"]
    base = dict(
        package_id=pkg.package_id,
        doc_id=pkg.doc_id,
        team_id=pkg.team_id,
        file_name=rec.file_name,
        line_no=rec.line_no,
        record_index=rec.record_index,
    )
    if layer not in policy.layers:
        errors.add(StructError(code="UNKNOWN_LAYER",
                               message=f"fragment uses undeclared layer {layer!r}",
                               **base))
        return
    if payload["label"] not in policy.layer(layer).labels:
        errors.add(
            StructError(
                code="UNKNOWN_LABEL",
                message=(
                    f"label {payload['label']!r} not allowed on layer {layer!r}"
                ),
                **base,
            )
        )
        return
    if payload["id"] in local_fragment_ids:
        errors.add(
            StructError(
                code="DUPLICATE_LOCAL_ID",
                message=f"duplicate fragment local id {payload['id']!r} in package",
                **base,
            )
        )
        return
    ok, why = _check_boundary(None, pkg.doc_id, start, end, sources=sources)
    if not ok:
        errors.add(StructError(code="BAD_RANGE",
                               message=f"fragment range: {why}", **base))
        return
    ok, why = sources.verify_seen_text(pkg.doc_id, start, end, payload["seen_text"])
    if not ok:
        errors.add(StructError(code="SEEN_TEXT_MISMATCH", message=why, **base))
        return
    if not _covered_by(review_by_layer.get(layer, []), start, end):
        errors.add(
            StructError(
                code="OUTSIDE_REVIEWED_SPAN",
                message=(
                    f"fragment {payload['id']!r} [{start},{end}) is not fully"
                    " inside this team's reviewed span"
                ),
                **base,
            )
        )
        return
    local_fragment_ids.add(payload["id"])
    proposal_rows.append(
        (
            proposal_uid(pkg.package_uid, payload["id"]),
            rec_uid,
            pkg.package_uid,
            pkg.doc_id,
            pkg.team_id,
            layer,
            payload["id"],
            payload["label"],
            start,
            end,
            payload["seen_text"],
        )
    )


def _handle_relation(policy, pkg, rec, payload, rec_uid, relation_rows, errors):
    rel_type = payload["type"]
    base = dict(
        package_id=pkg.package_id,
        doc_id=pkg.doc_id,
        team_id=pkg.team_id,
        file_name=rec.file_name,
        line_no=rec.line_no,
        record_index=rec.record_index,
    )
    if rel_type not in policy.relation_types:
        errors.add(
            StructError(
                code="UNKNOWN_RELATION_TYPE",
                message=f"relation type {rel_type!r} not declared in policy",
                **base,
            )
        )
        return
    relation_rows.append(
        (
            rec_uid,
            pkg.package_uid,
            pkg.doc_id,
            pkg.team_id,
            rel_type,
            payload["from_id"],
            payload["to_id"],
            payload["sign"],
        )
    )


def _check_boundary(conn, doc_id, start, end, sources: Optional[SourceStore] = None):
    """委托 SourceStore 做字符边界/越界检查（不在这里持有原文）。"""
    store = sources or _BOUNDARY_SOURCES["store"]
    ok, why = store.check_span(doc_id, start, end)
    return ok, why


# 由 build() 注入，避免把 SourceStore 透传到每个小函数
_BOUNDARY_SOURCES: dict = {}


def _merge_interval(intervals: list[list[int]], start: int, end: int) -> None:
    """把半开区间并入有序不相交列表；紧邻（首尾相碰）也接上。"""
    if not intervals or start > intervals[-1][1]:
        intervals.append([start, end])
        return
    # 流式包内区间通常有序；通用做法是找到最后一个可能相交的
    last = intervals[-1]
    if start <= last[1]:  # 相碰或交叠
        last[1] = max(last[1], end)
        last[0] = min(last[0], start)
    else:
        intervals.append([start, end])


def _covered_by(intervals: list[list[int]], start: int, end: int) -> bool:
    for lo, hi in intervals:
        if lo <= start and end <= hi:
            return True
        if lo > end:
            break
    return False


# ---------------------------------------------------------------------------
# 步骤 1：悬空关系校验（关系只能指向本包实际提议，允许前向引用）
# ---------------------------------------------------------------------------

def _validate_relation_endpoints(conn: sqlite3.Connection, errors: _ErrorCollector) -> None:
    rel_cur = conn.execute(
        "SELECT r.record_uid, r.package_uid, r.rel_type, r.from_local, r.to_local,"
        " p.package_id, p.doc_id, p.team_id, rec.file_name, rec.line_no,"
        " rec.record_index"
        " FROM raw_relation r"
        " JOIN package p ON p.package_uid = r.package_uid"
        " JOIN record rec ON rec.record_uid = r.record_uid"
        " ORDER BY r.doc_id, r.package_uid, rec.record_index"
    )
    for row in rel_cur:
        base = dict(
            package_id=row["package_id"],
            doc_id=row["doc_id"],
            team_id=row["team_id"],
            file_name=row["file_name"],
            line_no=row["line_no"],
            record_index=row["record_index"],
        )
        for local_id, side in ((row["from_local"], "from_id"),
                               (row["to_local"], "to_id")):
            hit = conn.execute(
                "SELECT 1 FROM proposal WHERE package_uid=? AND local_id=?",
                (row["package_uid"], local_id),
            ).fetchone()
            if hit is None:
                errors.add(
                    StructError(
                        code="DANGLING_RELATION",
                        message=(
                            f"relation {row['rel_type']!r} {side}={local_id!r}"
                            " has no matching fragment proposal in this package"
                        ),
                        **base,
                    )
                )
        # 端点层约束：关系类型的起/终点层必须与提议层一致
        fp = conn.execute(
            "SELECT layer FROM proposal WHERE package_uid=? AND local_id=?",
            (row["package_uid"], row["from_local"]),
        ).fetchone()
        tp = conn.execute(
            "SELECT layer FROM proposal WHERE package_uid=? AND local_id=?",
            (row["package_uid"], row["to_local"]),
        ).fetchone()
        pol = _POLICY_HOLDER["policy"].relation(row["rel_type"])
        if fp is not None and fp["layer"] != pol.from_layer:
            errors.add(
                StructError(
                    code="RELATION_LAYER_MISMATCH",
                    message=(
                        f"relation {row['rel_type']!r} from endpoint layer"
                        f" {fp['layer']!r}, policy requires {pol.from_layer!r}"
                    ),
                    **base,
                )
            )
        if tp is not None and tp["layer"] != pol.to_layer:
            errors.add(
                StructError(
                    code="RELATION_LAYER_MISMATCH",
                    message=(
                        f"relation {row['rel_type']!r} to endpoint layer"
                        f" {tp['layer']!r}, policy requires {pol.to_layer!r}"
                    ),
                    **base,
                )
            )


_POLICY_HOLDER: dict = {}


# ---------------------------------------------------------------------------
# 步骤 2：同层争议区域归组（排序 + 单遍扫描线，不枚举两两交叠）
# ---------------------------------------------------------------------------

def _group_regions(conn: sqlite3.Connection, counters: dict) -> None:
    """扫描线归组：按 doc, layer, start, end 排序后单遍处理。

    正长度交叠（a.start < b.end 且 b.start < a.end）传递合并；
    首尾相碰（prev_end == next_start）不算重叠，开新组。
    工作内存只保留当前 (doc, layer) 上同时活跃的组，与输入规模次线性。
    """
    cur = conn.execute(
        "SELECT proposal_uid, doc_id, layer, start_byte, end_byte"
        " FROM proposal"
        " ORDER BY doc_id, layer, start_byte, end_byte, proposal_uid"
    )
    current_doc = None
    current_layer = None
    active: list[dict] = []  # [{uids:[...], start, end}]
    region_rows = []
    link_rows = []

    def flush_group(group):
        region_uid = stable_id(
            "region",
            current_doc,
            current_layer,
            group["start"],
            group["end"],
        )
        region_rows.append(
            (region_uid, current_doc, current_layer, group["start"], group["end"])
        )
        for uid in group["uids"]:
            link_rows.append((region_uid, uid))
        counters["regions"] += 1

    for row in cur:
        if row["doc_id"] != current_doc or row["layer"] != current_layer:
            for g in active:
                flush_group(g)
            active = []
            current_doc = row["doc_id"]
            current_layer = row["layer"]

        s, e = row["start_byte"], row["end_byte"]
        # 移除在本提议开始前已经闭合（end <= s）的组：相碰即闭合
        remaining = []
        touched = []
        for g in active:
            counters["overlap_comparisons"] += 1  # 仅与活跃组比较，非两两枚举
            if g["end"] <= s:
                flush_group(g)
            else:
                remaining.append(g)
                if s < g["end"] and e > g["start"]:  # 正长度交叠
                    touched.append(g)
        active = remaining
        if not touched:
            active.append(
                {"uids": [row["proposal_uid"]], "start": s, "end": e}
            )
        else:
            # 传递合并：所有与当前提议交叠的活跃组合成一个
            base = touched[0]
            base["uids"].append(row["proposal_uid"])
            base["start"] = min(base["start"], s)
            base["end"] = max(base["end"], e)
            for g in touched[1:]:
                base["uids"].extend(g["uids"])
                base["start"] = min(base["start"], g["start"])
                base["end"] = max(base["end"], g["end"])
                active.remove(g)
            # 合并后可能与其他活跃组产生新的交叠（理论上排序保证少见，稳妥处理）
            changed = True
            while changed:
                changed = False
                for g in list(active):
                    if g is base:
                        continue
                    if base["start"] < g["end"] and g["start"] < base["end"]:
                        base["uids"].extend(g["uids"])
                        base["start"] = min(base["start"], g["start"])
                        base["end"] = max(base["end"], g["end"])
                        active.remove(g)
                        changed = True

    for g in active:
        flush_group(g)

    conn.executemany(
        "INSERT INTO region(region_uid, doc_id, layer, start_byte, end_byte)"
        " VALUES (?,?,?,?,?)",
        region_rows,
    )
    conn.executemany(
        "INSERT INTO region_proposal(region_uid, proposal_uid) VALUES (?,?)",
        link_rows,
    )


# ---------------------------------------------------------------------------
# 步骤 3：覆盖资格（合并审核区间，双指针判定完整/部分覆盖与缺口）
# ---------------------------------------------------------------------------

def _merged_reviews(conn, package_uid: str, layer: str) -> list[tuple[int, int]]:
    """取出该包该层的审核区间并做有序合并（紧邻可接上）。"""
    cur = conn.execute(
        "SELECT start_byte, end_byte FROM review_range"
        " WHERE package_uid=? AND layer=?"
        " ORDER BY start_byte, end_byte",
        (package_uid, layer),
    )
    merged: list[list[int]] = []
    for s, e in cur:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            merged[-1][1] = max(merged[-1][1], e)
    return [(s, e) for s, e in merged]


def coverage_gaps(
    merged: list[tuple[int, int]], start: int, end: int
) -> list[list[int]]:
    """计算 [start,end) 相对已合并审核区间的未覆盖缺口。

    中间只差一个字节（如 [0,5)+[6,9) 对 [0,9)）也会显式报告 [[5,6]]。
    """
    gaps: list[list[int]] = []
    cursor = start
    for lo, hi in merged:
        if hi <= cursor:
            continue
        if lo >= end:
            break
        if lo > cursor:
            gaps.append([cursor, min(lo, end)])
        cursor = max(cursor, hi)
        if cursor >= end:
            break
    if cursor < end:
        gaps.append([cursor, end])
    return gaps


def _compute_eligibility(conn: sqlite3.Connection, counters: dict) -> None:
    """对每个区域，找在该层有审核记录的团队逐个判覆盖。

    每包审核区间只排序/合并一次（按层缓存于查询内），
    区域按坐标排序后用前进指针消费区间，避免每区域全表扫描。
    """
    # 参与过该 (doc, layer) 的团队（无论是否提议）
    team_cur = conn.execute(
        "SELECT DISTINCT doc_id, layer, package_uid, team_id FROM review_range"
        " ORDER BY doc_id, layer, package_uid"
    )
    # 结构：(doc, layer) -> [(package_uid, team_id, merged_ranges)]
    current_key = None
    ranges_by_pkg: list[tuple] = []
    elig_rows = []

    def process(key, items):
        if key is None or items is None:
            return
        doc_id, layer = key
        regions = conn.execute(
            "SELECT region_uid, start_byte, end_byte FROM region"
            " WHERE doc_id=? AND layer=?"
            " ORDER BY start_byte, end_byte",
            (doc_id, layer),
        ).fetchall()
        for package_uid, team_id, merged in items:
            for reg in regions:
                gaps = coverage_gaps(merged, reg["start_byte"], reg["end_byte"])
                if not gaps:
                    coverage = "full"
                    counters["full_coverage_checks"] = (
                        counters.get("full_coverage_checks", 0) + 1
                    )
                else:
                    # 只有与区域有实质交集（至少部分覆盖）才落 partial
                    if _any_overlap(merged, reg["start_byte"], reg["end_byte"]):
                        coverage = "partial"
                    else:
                        continue
                elig_rows.append(
                    (reg["region_uid"], package_uid, team_id, coverage)
                )

    for row in team_cur:
        key = (row["doc_id"], row["layer"])
        if key != current_key:
            process(current_key, ranges_by_pkg)
            current_key = key
            ranges_by_pkg = []
        merged = _merged_reviews(conn, row["package_uid"], row["layer"])
        ranges_by_pkg.append((row["package_uid"], row["team_id"], merged))
    process(current_key, ranges_by_pkg)

    # 分批落盘
    for i in range(0, len(elig_rows), INSERT_BATCH):
        conn.executemany(
            "INSERT INTO region_eligibility(region_uid, package_uid, team_id,"
            " coverage) VALUES (?,?,?,?)",
            elig_rows[i : i + INSERT_BATCH],
        )


def _any_overlap(merged: list[tuple[int, int]], start: int, end: int) -> bool:
    for lo, hi in merged:
        if lo < end and start < hi:
            return True
        if lo >= end:
            break
    return False


# ---------------------------------------------------------------------------
# 步骤 4：区域计票（完整覆盖团队进分母；候选含「无片段」）
# ---------------------------------------------------------------------------

def _tally_regions(
    conn: sqlite3.Connection, policy: Policy, counters: dict
) -> None:
    region_cur = conn.execute(
        "SELECT region_uid, doc_id, layer, start_byte, end_byte FROM region"
        " ORDER BY doc_id, layer, start_byte"
    )
    for reg in region_cur:
        rid = reg["region_uid"]
        layer_pol = policy.layer(reg["layer"])
        # 分母：完整覆盖的团队
        full = conn.execute(
            "SELECT package_uid, team_id FROM region_eligibility"
            " WHERE region_uid=? AND coverage='full'"
            " ORDER BY package_uid",
            (rid,),
        ).fetchall()
        denominator = len(full)
        strict_majority = denominator // 2 + 1 if denominator else 1

        # 候选由区域内成员提议的 (start,end,label) 去重产生，另加「无片段」
        cand_cur = conn.execute(
            "SELECT DISTINCT p.start_byte AS s, p.end_byte AS e, p.label AS label"
            " FROM region_proposal rp JOIN proposal p"
            " ON p.proposal_uid = rp.proposal_uid"
            " WHERE rp.region_uid=?"
            " ORDER BY p.start_byte, p.end_byte, p.label",
            (rid,),
        )
        cand_uid_by_key: dict[tuple, str] = {}
        cand_rows = []
        for c in cand_cur:
            key = (c["s"], c["e"], c["label"])
            cuid = stable_id("cand", rid, *key)
            cand_uid_by_key[key] = cuid
            cand_rows.append((cuid, rid, c["s"], c["e"], c["label"], 0))
        none_uid = stable_id("cand", rid, CANDIDATE_NONE)
        cand_rows.append(
            (none_uid, rid, reg["start_byte"], reg["end_byte"], CANDIDATE_NONE, 1)
        )
        conn.executemany(
            "INSERT INTO region_candidate(candidate_uid, region_uid, start_byte,"
            " end_byte, label, is_no_fragment) VALUES (?,?,?,?,?,?)",
            cand_rows,
        )

        # 每个完整覆盖团队在该区域内的不同候选（按起止+标签认同）
        vote_rows = []
        counts: dict[str, int] = defaultdict(int)  # candidate_uid -> 票数
        none_votes = 0
        for team_row in full:
            pkg_uid = team_row["package_uid"]
            my_cands = conn.execute(
                "SELECT DISTINCT p.start_byte AS s, p.end_byte AS e, p.label AS l"
                " FROM region_proposal rp"
                " JOIN proposal p ON p.proposal_uid = rp.proposal_uid"
                " WHERE rp.region_uid=? AND p.package_uid=?"
                " ORDER BY p.start_byte, p.end_byte, p.label",
                (rid, pkg_uid),
            ).fetchall()
            if not my_cands:
                # 完整覆盖却没提议：投「无片段」
                none_votes += 1
                vote_rows.append(
                    (none_uid, rid, pkg_uid, team_row["team_id"], VOTE_NONE)
                )
            elif len(my_cands) == 1:
                key = (my_cands[0]["s"], my_cands[0]["e"], my_cands[0]["l"])
                cuid = cand_uid_by_key[key]
                counts[cuid] += 1
                vote_rows.append(
                    (cuid, rid, pkg_uid, team_row["team_id"], VOTE_CANDIDATE)
                )
            else:
                # 多个不同候选：自相矛盾，不给任何候选加票，但仍在分母
                vote_rows.append(
                    (None, rid, pkg_uid, team_row["team_id"], VOTE_CONFLICT)
                )
        conn.executemany(
            "INSERT INTO region_vote(candidate_uid, region_uid, package_uid,"
            " team_id, vote_kind) VALUES (?,?,?,?,?)",
            vote_rows,
        )

        # 判定：同时达到最少支持团队数与严格多数
        winner_kind = "pending"
        winner_candidate = None
        # 「无片段」也是候选
        tally = list(counts.items()) + [(none_uid, none_votes)]
        winners = [
            cuid
            for cuid, n in tally
            if n >= layer_pol.min_support and n >= strict_majority
        ]
        if len(winners) == 1:
            winner_candidate = winners[0]
            winner_kind = (
                "no_fragment" if winner_candidate == none_uid else "candidate"
            )
        # 0 或 >1 个达标者都算待核（并列无胜出）
        conn.execute(
            "INSERT INTO region_outcome(region_uid, doc_id, layer, denominator,"
            " winner_kind, winner_candidate, min_support, strict_majority)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                rid,
                reg["doc_id"],
                reg["layer"],
                denominator,
                winner_kind,
                winner_candidate,
                layer_pol.min_support,
                strict_majority,
            ),
        )
        counters["region_decisions"] += 1

        if winner_kind == "candidate":
            _write_official_fragment(conn, rid, winner_candidate)
        elif winner_kind == "no_fragment":
            counters["confirmed_no_fragment"] += 1
        else:
            _write_region_review(conn, reg, denominator, strict_majority,
                                 layer_pol.min_support)


def _write_official_fragment(conn, region_uid, candidate_uid):
    cand = conn.execute(
        "SELECT * FROM region_candidate WHERE candidate_uid=?",
        (candidate_uid,),
    ).fetchone()
    reg = conn.execute(
        "SELECT doc_id, layer FROM region WHERE region_uid=?",
        (region_uid,),
    ).fetchone()
    # 正式片段身份：doc + layer + 坐标 + 标签 的内容派生 ID
    fragment_uid = stable_id(
        "frag",
        reg["doc_id"],
        reg["layer"],
        cand["start_byte"],
        cand["end_byte"],
        cand["label"],
    )
    # 取任一支持该候选的提议所见原文（候选坐标相同，字节必然一致，
    # 构建时已逐字节核对过原文）
    seen = conn.execute(
        "SELECT p.seen_text FROM region_proposal rp"
        " JOIN proposal p ON p.proposal_uid = rp.proposal_uid"
        " WHERE rp.region_uid=? AND p.start_byte=? AND p.end_byte=?"
        " AND p.label=? LIMIT 1",
        (region_uid, cand["start_byte"], cand["end_byte"], cand["label"]),
    ).fetchone()
    seen_text = seen["seen_text"]
    conn.execute(
        "INSERT INTO official_fragment(fragment_uid, doc_id, layer, label,"
        " start_byte, end_byte, region_uid, candidate_uid, text_sha256, seen_text)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            fragment_uid,
            reg["doc_id"],
            reg["layer"],
            cand["label"],
            cand["start_byte"],
            cand["end_byte"],
            region_uid,
            candidate_uid,
            sha256_hex(seen_text.encode("utf-8")),
            seen_text,
        ),
    )


def _write_region_review(conn, reg, denominator, strict_majority, min_support):
    rid = reg["region_uid"]
    review_uid = stable_id("rev", "region", rid)
    detail = {
        "reason": "no_candidate_reached_threshold",
        "denominator": denominator,
        "strict_majority": strict_majority,
        "min_support": min_support,
    }
    conn.execute(
        "INSERT INTO review_item(review_uid, item_kind, doc_id, layer, rel_type,"
        " region_uid, rel_group_uid, record_uid, reason_code, detail_json)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            review_uid,
            REVIEW_REGION,
            reg["doc_id"],
            reg["layer"],
            None,
            rid,
            None,
            None,
            "REGION_PENDING",
            canonical_json(detail),
        ),
    )


# ---------------------------------------------------------------------------
# 步骤 5：关系端点解析、阻断、归票与判定
# ---------------------------------------------------------------------------

def _resolve_and_tally_relations(
    conn: sqlite3.Connection, policy: Policy, counters: dict
) -> None:
    rel_cur = conn.execute(
        "SELECT r.*, rec.file_name, rec.line_no, rec.record_index"
        " FROM raw_relation r JOIN record rec ON rec.record_uid = r.record_uid"
        " ORDER BY r.doc_id, r.rel_type, r.package_uid, rec.record_index"
    )
    # 分组暂存：rel_group_key -> {pkg: {"signs":set, "records":[...]}}
    groups: dict[tuple, dict] = {}

    for row in rel_cur:
        from_prop = conn.execute(
            "SELECT proposal_uid, layer, start_byte, end_byte FROM proposal"
            " WHERE package_uid=? AND local_id=?",
            (row["package_uid"], row["from_local"]),
        ).fetchone()
        to_prop = conn.execute(
            "SELECT proposal_uid, layer, start_byte, end_byte FROM proposal"
            " WHERE package_uid=? AND local_id=?",
            (row["package_uid"], row["to_local"]),
        ).fetchone()
        # 悬空已在结构校验阶段拒收；这里兜底
        if from_prop is None or to_prop is None:
            _blocked_relation(
                conn, row, None, None,
                "dangling_endpoint",
                "endpoint proposal missing (should have been rejected)",
            )
            continue

        from_frag, from_block = _resolve_endpoint(conn, from_prop["proposal_uid"])
        to_frag, to_block = _resolve_endpoint(conn, to_prop["proposal_uid"])
        conn.execute(
            "INSERT INTO relation_endpoint(record_uid, package_uid, from_proposal,"
            " to_proposal, resolved, blocked_reason) VALUES (?,?,?,?,?,?)",
            (
                row["record_uid"],
                row["package_uid"],
                from_prop["proposal_uid"],
                to_prop["proposal_uid"],
                1 if (from_frag and to_frag) else 0,
                None if (from_frag and to_frag)
                else (from_block or to_block),
            ),
        )
        if not (from_frag and to_frag):
            _blocked_relation(
                conn, row, from_prop["proposal_uid"], to_prop["proposal_uid"],
                from_block or to_block,
                f"endpoint blocked: {from_block or to_block}",
            )
            counters["relations_blocked"] += 1
            continue

        key = (
            row["doc_id"],
            row["rel_type"],
            from_frag,
            to_frag,
        )  # 反向关系 (to_frag, from_frag) 天然是另一个 key
        bucket = groups.setdefault(key, {})
        team_bucket = bucket.setdefault(
            row["package_uid"],
            {"team_id": row["team_id"], "signs": set(), "records": []},
        )
        team_bucket["signs"].add(row["sign"])
        team_bucket["records"].append(row["record_uid"])

    _finalize_relation_groups(conn, policy, groups, counters)


def _resolve_endpoint(conn, proposal_uid: str) -> tuple[Optional[str], Optional[str]]:
    """把本包提议解析为正式片段；不能解析时给出阻断原因。

    绝不因为两个候选同处一个争议区域就把票转移到胜出候选上：
    只有该提议本身（同坐标同标签）成为正式片段才算解析成功。
    """
    link = conn.execute(
        "SELECT region_uid FROM region_proposal WHERE proposal_uid=?",
        (proposal_uid,),
    ).fetchone()
    if link is None:
        # 理论不会发生：所有提议都属于某个区域
        return None, BLOCK_PENDING
    outcome = conn.execute(
        "SELECT winner_kind, winner_candidate FROM region_outcome"
        " WHERE region_uid=?",
        (link["region_uid"],),
    ).fetchone()
    if outcome is None or outcome["winner_kind"] == "pending":
        return None, BLOCK_PENDING
    if outcome["winner_kind"] == "no_fragment":
        return None, BLOCK_NONE_WON
    prop = conn.execute(
        "SELECT start_byte, end_byte, label FROM proposal WHERE proposal_uid=?",
        (proposal_uid,),
    ).fetchone()
    cand = conn.execute(
        "SELECT candidate_uid FROM region_candidate WHERE region_uid=?"
        " AND start_byte=? AND end_byte=? AND label=? AND is_no_fragment=0",
        (
            link["region_uid"],
            prop["start_byte"],
            prop["end_byte"],
            prop["label"],
        ),
    ).fetchone()
    if cand is None or cand["candidate_uid"] != outcome["winner_candidate"]:
        return None, BLOCK_OTHER_WON
    frag = conn.execute(
        "SELECT fragment_uid FROM official_fragment WHERE region_uid=?"
        " AND candidate_uid=?",
        (link["region_uid"], outcome["winner_candidate"]),
    ).fetchone()
    return frag["fragment_uid"], None


def _blocked_relation(conn, row, from_prop, to_prop, reason_code, message):
    review_uid = stable_id("rev", "relation-blocked", row["record_uid"])
    detail = {
        "reason": reason_code,
        "message": message,
        "rel_type": row["rel_type"],
        "from_local": row["from_local"],
        "to_local": row["to_local"],
        "sign": row["sign"],
        "from_proposal": from_prop,
        "to_proposal": to_prop,
        "file": None,  # 证据在 record 表，查询时联表
    }
    conn.execute(
        "INSERT INTO review_item(review_uid, item_kind, doc_id, layer, rel_type,"
        " region_uid, rel_group_uid, record_uid, reason_code, detail_json)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            review_uid,
            REVIEW_RELATION,
            row["doc_id"],
            None,
            row["rel_type"],
            None,
            None,
            row["record_uid"],
            reason_code,
            canonical_json(detail),
        ),
    )


def _finalize_relation_groups(conn, policy, groups: dict, counters: dict) -> None:
    for (doc_id, rel_type, from_frag, to_frag), bucket in groups.items():
        rel_pol = policy.relation(rel_type)
        group_uid = stable_id(
            "relgroup", doc_id, rel_type, from_frag, to_frag
        )
        denominator = len(bucket)  # 只有明确投票的不同团队；不补反对票
        strict_majority = denominator // 2 + 1 if denominator else 1
        for_votes = against_votes = conflict_votes = 0
        vote_rows = []
        for pkg_uid, info in sorted(bucket.items()):
            if info["signs"] == {SIGN_FOR}:
                sign = SIGN_FOR
                for_votes += 1
            elif info["signs"] == {SIGN_AGAINST}:
                sign = SIGN_AGAINST
                against_votes += 1
            else:
                # 赞成与反对都出现：不计入任一方，但仍进分母
                sign = "conflict"
                conflict_votes += 1
            vote_rows.append(
                (
                    group_uid,
                    pkg_uid,
                    info["team_id"],
                    sign,
                    canonical_json(sorted(info["records"])),
                )
            )
        if for_votes >= rel_pol.min_support and for_votes >= strict_majority:
            decision = "confirmed"
        elif (
            against_votes >= rel_pol.min_support
            and against_votes >= strict_majority
        ):
            decision = "confirmed_absent"
        else:
            decision = "pending"
        conn.execute(
            "INSERT INTO relation_group(rel_group_uid, doc_id, rel_type,"
            " from_fragment, to_fragment, denominator, for_votes, against_votes,"
            " conflict_votes, strict_majority, min_support, decision)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                group_uid,
                doc_id,
                rel_type,
                from_frag,
                to_frag,
                denominator,
                for_votes,
                against_votes,
                conflict_votes,
                strict_majority,
                rel_pol.min_support,
                decision,
            ),
        )
        conn.executemany(
            "INSERT INTO relation_group_vote(rel_group_uid, package_uid, team_id,"
            " sign, record_uids) VALUES (?,?,?,?,?)",
            vote_rows,
        )
        counters["relation_groups"] += 1
        if decision == "confirmed":
            conn.execute(
                "INSERT INTO official_relation(rel_group_uid, doc_id, rel_type,"
                " from_fragment, to_fragment) VALUES (?,?,?,?,?)",
                (group_uid, doc_id, rel_type, from_frag, to_frag),
            )
            counters["official_relations"] += 1
        elif decision == "confirmed_absent":
            counters["confirmed_absent_relations"] += 1
        else:
            review_uid = stable_id("rev", "relation-group", group_uid)
            detail = {
                "reason": "relation_threshold_not_met",
                "denominator": denominator,
                "for": for_votes,
                "against": against_votes,
                "conflict": conflict_votes,
                "strict_majority": strict_majority,
                "min_support": rel_pol.min_support,
            }
            conn.execute(
                "INSERT INTO review_item(review_uid, item_kind, doc_id, layer,"
                " rel_type, region_uid, rel_group_uid, record_uid, reason_code,"
                " detail_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    review_uid,
                    REVIEW_RELATION,
                    doc_id,
                    None,
                    rel_type,
                    None,
                    group_uid,
                    None,
                    "RELATION_PENDING",
                    canonical_json(detail),
                ),
            )


# ---------------------------------------------------------------------------
# 元信息与计数器
# ---------------------------------------------------------------------------

def _finalize_meta(conn, sources: SourceStore, packages, policy: Policy) -> None:
    meta = {
        "format_version": "1",
        "doc_count": str(len(sources.doc_ids())),
        "package_count": str(len(packages) if packages else 0),
        "layers": canonical_json(sorted(policy.layers)),
        "relation_types": canonical_json(sorted(policy.relation_types)),
        "published_at_utc": _utc_now(),
    }
    conn.executemany(
        "INSERT INTO build_meta(key, value) VALUES (?,?)",
        sorted(meta.items()),
    )


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
