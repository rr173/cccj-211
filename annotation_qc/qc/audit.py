# -*- coding: utf-8 -*-
"""发布后独立盲审：抽样、盲审批次快照、审核提交与只读导出。

设计要点（与需求逐条对应）：

- **抽样总体与单位**：总体是某已登记发布中包含至少一条正式片段或正式
  引用的不同公文；抽样单位为公文，按种子派生的确定性伪随机序**不放回**
  抽取恰好 ``sample_size`` 篇。总体不足即整次拒绝，不生成空壳批次。
- **样本稳定**：每篇公文的分值只由 ``(种子, 档号)`` 决定，按
  ``(分值, 档号 UTF-8 字节)`` 排序取前 k；与 SQLite 行顺序、查询计划、
  输入文件排列无关。分值计算与临时排序都交给 SQLite 临时表（可落磁盘），
  Python 侧只保留 O(样本量) 的状态。
- **可重开批次**：创建时把发布绑定（release/rev/公文集合）、抽样参数、
  抽样名单、抽中公文在该发布下的全部正式片段与正式引用快照写入审计库。
  此后导出只读快照，后续合批/新发布不改变旧批次。
- **盲审标识**：每批一个随机密钥；``blind_doc_id`` / ``blind_fragment_id``
  / ``blind_relation_id`` 由 HMAC-SHA256(密钥, 种类+正式身份) 派生，
  只在本批有效，不依赖自增号或明文档号。导出不包含正式身份、团队、
  投票、自动/人工来源、裁决理由或其它批次映射。
- **审核提交**：按本批盲审标识定位主体；问题证据须给出原文坐标，按批次
  绑定的底稿摘要核对字节边界与原文内容。不属于本批、重复、坐标非法
  任一处即整批拒绝，不部分写入。成功提交幂等重放原回执；单事务 + 文件锁
  保证并发/中断/重试不产生半批结果。
- **只读审计视图**：批次总览、抽中范围、未审记录、审计结论与可定位的
  原文证据；审计库独立，已登记发布只以 ``mode=ro`` 打开，绝不回写。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from typing import Iterator, Optional

from .audit_common import (
    AuditError,
    CREATE,
    SUBMIT,
    VERDICT_CONFORMING,
    VERDICT_ISSUE,
    digest_id_sequence,
    exclusive,
    find_request,
    open_audit,
    parse_int,
    parse_str,
    request_hash,
    require_request_id,
    transaction,
    utc_now,
)
from .audit_source import AuditSources
from .audit_upstream import AuditUpstream
from .util import canonical_json, sha256_hex, stable_id

SAMPLE_DOMAIN = b"annotation_qc.publish_audit.sample.v1"
BLIND_DOMAIN = b"annotation_qc.publish_audit.blind.v1"
POP_CHUNK = 2000
INSERT_CHUNK = 1000

KIND_DOC = "document"
KIND_FRAGMENT = "fragment"
KIND_RELATION = "relation"
ALLOWED_SUBJECT_KINDS = (KIND_DOC, KIND_FRAGMENT, KIND_RELATION)


# ---------------------------------------------------------------------------
# 确定性抽样
# ---------------------------------------------------------------------------

def sample_score(seed: int, doc_id: str) -> bytes:
    """公文在给定种子下的确定性伪随机分值（32 字节）。

    只由种子与档号决定；参考实现可按同样规则独立复算::

        sha256(b"annotation_qc.publish_audit.sample.v1" + b"\\x00"
               + str(seed).encode("ascii") + b"\\x00" + doc_id.encode("utf-8"))

    按 ``(分值, 档号 UTF-8 字节)`` 升序取前 k 即为不放回抽样；
    并列分值用档号字节序打破，保证不依赖任何枚举顺序。
    """
    h = hashlib.sha256()
    h.update(SAMPLE_DOMAIN)
    h.update(b"\x00")
    h.update(str(seed).encode("ascii"))
    h.update(b"\x00")
    h.update(doc_id.encode("utf-8"))
    return h.digest()


def _select_sample(conn, upstream: AuditUpstream, release_id: str,
                   sample_size: int,
                   seed: int) -> tuple[list[str], int, str]:
    """返回 (按档号字节序排列的样本, 总体大小, 总体摘要)。"""
    conn.execute("DROP TABLE IF EXISTS temp._aud_pop")
    conn.execute(
        "CREATE TEMP TABLE _aud_pop ("
        " doc_id TEXT PRIMARY KEY,"
        " score BLOB NOT NULL)")
    cur = upstream.population_cursor(release_id)
    chunk: list[tuple] = []
    for row in cur:
        doc_id = row["doc_id"]
        chunk.append((doc_id, sample_score(seed, doc_id)))
        if len(chunk) >= POP_CHUNK:
            conn.executemany(
                "INSERT OR IGNORE INTO _aud_pop(doc_id, score) VALUES (?,?)",
                chunk)
            chunk.clear()
    if chunk:
        conn.executemany(
            "INSERT OR IGNORE INTO _aud_pop(doc_id, score) VALUES (?,?)",
            chunk)
        chunk.clear()

    population_size = conn.execute(
        "SELECT COUNT(*) c FROM _aud_pop").fetchone()["c"]
    if population_size == 0:
        raise AuditError(
            "EMPTY_POPULATION",
            f"release {release_id} has no document with an official fragment"
            " or relation", {"release_id": release_id})
    if sample_size > population_size:
        raise AuditError(
            "SAMPLE_TOO_LARGE",
            f"requested {sample_size} documents but the release population"
            f" has only {population_size}",
            {"sample_size": sample_size,
             "population_size": population_size})

    population_digest = _digest_stream(
        r["doc_id"] for r in conn.execute(
            "SELECT doc_id FROM _aud_pop ORDER BY doc_id"))
    sample = [
        r["doc_id"] for r in conn.execute(
            "SELECT doc_id FROM _aud_pop ORDER BY score, doc_id LIMIT ?",
            (sample_size,))
    ]
    conn.execute("DROP TABLE IF EXISTS temp._aud_pop")
    sample.sort(key=lambda d: d.encode("utf-8"))
    return sample, population_size, population_digest


def _digest_stream(doc_ids) -> str:
    h = hashlib.sha256()
    for doc_id in doc_ids:
        h.update(doc_id.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 盲审标识
# ---------------------------------------------------------------------------

def _blind_id(secret_hex: str, kind: str, real_id: str, prefix: str) -> str:
    mac = hmac.new(
        bytes.fromhex(secret_hex),
        BLIND_DOMAIN + b"\x1f" + kind.encode("ascii") + b"\x1f"
        + real_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{prefix}_{mac[:32]}"


def blind_batch_id(secret_hex: str, batch_id: str) -> str:
    mac = hmac.new(
        bytes.fromhex(secret_hex),
        BLIND_DOMAIN + b"\x1f" + b"batch" + b"\x1f" + batch_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"aub_{mac[:24]}"


def relation_key(rel: dict) -> str:
    """引用在本审计库内的稳定键：有序端点的规范化文本（反向天然不同）。"""
    return canonical_json([rel["doc_id"], rel["rel_type"],
                           rel["from_fragment"], rel["to_fragment"]])


# ---------------------------------------------------------------------------
# 创建批次
# ---------------------------------------------------------------------------

def create_batch(audit_dir: str, conn, workspace: str, request: dict) -> dict:
    """创建（或重开）一个盲审批次。``workspace`` 为裁决工作区目录。"""
    rid = require_request_id(request)
    allowed = {"request_id", "release_id", "sample_size", "seed"}
    unknown = sorted(set(request) - allowed)
    if unknown:
        raise AuditError("BAD_REQUEST", f"未知的创建参数: {unknown}")
    release_id = parse_str(request.get("release_id"), "release_id")
    sample_size = parse_int(request.get("sample_size"), "sample_size",
                            minimum=1)
    seed = parse_int(request.get("seed"), "seed",
                     minimum=-(2 ** 63), maximum=2 ** 63 - 1)
    payload = {"release_id": release_id, "sample_size": sample_size,
               "seed": seed}
    req_hash = request_hash(CREATE, payload)

    with exclusive(audit_dir):
        prior = find_request(conn, rid)
        if prior is not None:
            if prior["request_hash"] != req_hash or prior["kind"] != CREATE:
                raise AuditError(
                    "REQUEST_ID_CONFLICT",
                    f"request_id {rid!r} 已被不同参数或不同种类的请求使用",
                    {"request_id": rid})
            return {**json.loads(prior["receipt_json"]), "replayed": True,
                    "created": False}

        upstream = AuditUpstream(workspace)
        try:
            with transaction(conn):
                again = find_request(conn, rid)
                if again is not None:
                    if (again["request_hash"] != req_hash
                            or again["kind"] != CREATE):
                        raise AuditError(
                            "REQUEST_ID_CONFLICT",
                            f"request_id {rid!r} 已被不同参数使用",
                            {"request_id": rid})
                    return {**json.loads(again["receipt_json"]),
                            "replayed": True, "created": False}

                info = upstream.release_info(release_id)
                if info is None:
                    raise AuditError(
                        "RELEASE_NOT_FOUND", f"unknown release: {release_id}",
                        {"release_id": release_id})
                sample, population_size, pop_digest = _select_sample(
                    conn, upstream, release_id, sample_size, seed)
                sample_digest = digest_id_sequence(sample)
                batch_id = stable_id(
                    "aub", release_id, info["rev_id"], sample_size, seed,
                    population_size, pop_digest, sample_digest)

                existing = conn.execute(
                    "SELECT * FROM aud_batch WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if existing is not None:
                    # 相同发布/样本量/种子：同一批次，重开而不重抽
                    receipt = _create_receipt(conn, existing, rid,
                                              replayed=True)
                    conn.execute(
                        "INSERT INTO aud_request(request_id, request_hash,"
                        " kind, batch_id, receipt_json, created_at)"
                        " VALUES (?,?,?,?,?,?)",
                        (rid, req_hash, CREATE, batch_id,
                         canonical_json(receipt), utc_now()))
                    return receipt

                secret = secrets.token_hex(32)
                ts = utc_now()
                conn.execute(
                    "INSERT INTO aud_batch(batch_id, created_request_id,"
                    " release_id, rev_id, rev_seq, release_doc_ids_json,"
                    " release_digest, sample_size, seed, population_size,"
                    " population_digest, sample_digest, blind_secret,"
                    " blind_batch_id, fragment_count, relation_count,"
                    " created_at, request_json)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, rid, release_id, info["rev_id"],
                     info["rev_seq"], canonical_json(sorted(info["doc_ids"])),
                     "", sample_size, seed, population_size, pop_digest,
                     sample_digest, secret,
                     blind_batch_id(secret, batch_id), 0, 0, ts,
                     canonical_json(payload)))

                snapshot = hashlib.sha256()
                fragment_count = 0
                relation_count = 0
                for rank, doc_id in enumerate(sample, start=1):
                    ident = upstream.doc_identity(doc_id)
                    if ident is None:
                        raise AuditError(
                            "UPSTREAM_INCONSISTENT",
                            f"release document {doc_id!r} has no source_doc"
                            " identity", {"doc_id": doc_id})
                    bdoc = _blind_id(secret, KIND_DOC, doc_id, "bd")
                    conn.execute(
                        "INSERT INTO aud_sample_doc(batch_id, doc_id,"
                        " blind_doc_id, sample_rank, source_sha256,"
                        " byte_length, rel_path, abs_path)"
                        " VALUES (?,?,?,?,?,?,?,?)",
                        (batch_id, doc_id, bdoc, rank, ident["sha256"],
                         ident["byte_length"], ident["rel_path"],
                         ident["abs_path"]))
                    conn.execute(
                        "INSERT INTO aud_blind_map(batch_id, kind, blind_id,"
                        " real_id, doc_id) VALUES (?,?,?,?,?)",
                        (batch_id, KIND_DOC, bdoc, doc_id, doc_id))

                    fragment_count += _snapshot_fragments(
                        conn, upstream, secret, batch_id, release_id, doc_id,
                        bdoc, snapshot)
                    relation_count += _snapshot_relations(
                        conn, upstream, secret, batch_id, release_id, doc_id,
                        bdoc, snapshot)

                conn.execute(
                    "UPDATE aud_batch SET release_digest=?, fragment_count=?,"
                    " relation_count=? WHERE batch_id=?",
                    (snapshot.hexdigest(), fragment_count, relation_count,
                     batch_id))
                batch = conn.execute(
                    "SELECT * FROM aud_batch WHERE batch_id=?", (batch_id,)
                ).fetchone()
                receipt = _create_receipt(conn, batch, rid, replayed=False)
                conn.execute(
                    "INSERT INTO aud_request(request_id, request_hash, kind,"
                    " batch_id, receipt_json, created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (rid, req_hash, CREATE, batch_id,
                     canonical_json(receipt), utc_now()))
                return receipt
        finally:
            upstream.close()


def _snapshot_fragments(conn, upstream, secret, batch_id, release_id,
                        doc_id, bdoc, snapshot) -> int:
    rows: list[tuple] = []
    count = 0
    for frag in upstream.iter_fragments_for_doc(release_id, doc_id):
        bfrag = _blind_id(secret, KIND_FRAGMENT, frag["fragment_uid"], "bf")
        rows.append((
            batch_id, frag["fragment_uid"], bfrag, doc_id, bdoc,
            frag["layer"], frag["label"], frag["start_byte"],
            frag["end_byte"], frag["text_sha256"], frag["seen_text"],
        ))
        conn.execute(
            "INSERT INTO aud_blind_map(batch_id, kind, blind_id, real_id,"
            " doc_id) VALUES (?,?,?,?,?)",
            (batch_id, KIND_FRAGMENT, bfrag, frag["fragment_uid"], doc_id))
        snapshot.update(canonical_json([
            "fragment", doc_id, frag["fragment_uid"], frag["layer"],
            frag["label"], frag["start_byte"], frag["end_byte"],
            frag["text_sha256"], frag["seen_text"]]).encode("utf-8"))
        snapshot.update(b"\x1e")
        count += 1
        if len(rows) >= INSERT_CHUNK:
            conn.executemany(
                "INSERT INTO aud_fragment(batch_id, fragment_uid,"
                " blind_fragment_id, doc_id, blind_doc_id, layer, label,"
                " start_byte, end_byte, text_sha256, seen_text)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            rows.clear()
    if rows:
        conn.executemany(
            "INSERT INTO aud_fragment(batch_id, fragment_uid,"
            " blind_fragment_id, doc_id, blind_doc_id, layer, label,"
            " start_byte, end_byte, text_sha256, seen_text)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    return count


def _snapshot_relations(conn, upstream, secret, batch_id, release_id,
                        doc_id, bdoc, snapshot) -> int:
    count = 0
    for rel in upstream.iter_relations_for_doc(release_id, doc_id):
        bfrm = _blind_fragment_id(conn, upstream, secret, batch_id,
                                  release_id, rel["from_fragment"], doc_id)
        bto = _blind_fragment_id(conn, upstream, secret, batch_id,
                                 release_id, rel["to_fragment"], doc_id)
        key = relation_key(rel)
        brel = _blind_id(secret, KIND_RELATION, key, "br")
        conn.execute(
            "INSERT INTO aud_relation(batch_id, blind_relation_id, doc_id,"
            " blind_doc_id, rel_type, from_fragment, to_fragment, blind_from,"
            " blind_to) VALUES (?,?,?,?,?,?,?,?,?)",
            (batch_id, brel, doc_id, bdoc, rel["rel_type"],
             rel["from_fragment"], rel["to_fragment"], bfrm, bto))
        conn.execute(
            "INSERT INTO aud_blind_map(batch_id, kind, blind_id, real_id,"
            " doc_id) VALUES (?,?,?,?,?)",
            (batch_id, KIND_RELATION, brel, key, doc_id))
        snapshot.update(canonical_json([
            "relation", doc_id, rel["rel_type"], rel["from_fragment"],
            rel["to_fragment"]]).encode("utf-8"))
        snapshot.update(b"\x1e")
        count += 1
    return count


def _blind_fragment_id(conn, upstream, secret, batch_id, release_id,
                       fragment_uid: str, fallback_doc: str) -> str:
    row = conn.execute(
        "SELECT blind_id FROM aud_blind_map WHERE batch_id=? AND kind=?"
        " AND real_id=?",
        (batch_id, KIND_FRAGMENT, fragment_uid)).fetchone()
    if row is not None:
        return row["blind_id"]
    # 端点片段不在已抽中片段快照里（例如发布数据不自洽）：仍生成可回查
    # 本批盲审内部的标识，并尽量记录所属公文，绝不静默丢弃这条引用。
    doc_id = upstream.fragment_doc(release_id, fragment_uid) or fallback_doc
    blind = _blind_id(secret, KIND_FRAGMENT, fragment_uid, "bf")
    conn.execute(
        "INSERT INTO aud_blind_map(batch_id, kind, blind_id, real_id, doc_id)"
        " VALUES (?,?,?,?,?)",
        (batch_id, KIND_FRAGMENT, blind, fragment_uid, doc_id))
    return blind


def _create_receipt(conn, batch, request_id: str, *, replayed: bool) -> dict:
    docs = [
        {"doc_id": r["doc_id"], "blind_doc_id": r["blind_doc_id"],
         "sample_rank": r["sample_rank"]}
        for r in conn.execute(
            "SELECT doc_id, blind_doc_id, sample_rank FROM aud_sample_doc"
            " WHERE batch_id=? ORDER BY sample_rank", (batch["batch_id"],))
    ]
    return {
        "ok": True,
        "kind": CREATE,
        "request_id": request_id,
        "batch_id": batch["batch_id"],
        "blind_batch_id": batch["blind_batch_id"],
        "release_id": batch["release_id"],
        "rev_id": batch["rev_id"],
        "sample_size": batch["sample_size"],
        "seed": batch["seed"],
        "population_size": batch["population_size"],
        "sample_doc_ids": [d["doc_id"] for d in docs],
        "sample_digest": batch["sample_digest"],
        "blind_doc_ids": [d["blind_doc_id"] for d in docs],
        "fragment_count": batch["fragment_count"],
        "relation_count": batch["relation_count"],
        "replayed": replayed,
    }


# ---------------------------------------------------------------------------
# 审核提交
# ---------------------------------------------------------------------------

def resolve_batch(conn, ref: str):
    if not isinstance(ref, str) or not ref:
        raise AuditError("BAD_REQUEST", "batch 必须是非空字符串")
    row = conn.execute(
        "SELECT * FROM aud_batch WHERE batch_id=?", (ref,)).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT * FROM aud_batch WHERE blind_batch_id=?", (ref,)
        ).fetchone()
    if row is None:
        raise AuditError("BATCH_NOT_FOUND", f"unknown audit batch: {ref}")
    return row


def submit_results(audit_dir: str, conn, request: dict, *,
                   manifest_path: Optional[str] = None,
                   source_root: Optional[str] = None) -> dict:
    rid = require_request_id(request)
    allowed = {"request_id", "batch_id", "blind_batch_id", "reviewer",
               "results"}
    unknown = sorted(set(request) - allowed)
    if unknown:
        raise AuditError("BAD_REQUEST", f"未知的提交参数: {unknown}")
    ref = request.get("batch_id") or request.get("blind_batch_id")
    ref = parse_str(ref, "batch_id")
    results = request.get("results")
    if not isinstance(results, list) or not results:
        raise AuditError("BAD_REQUEST", "results 必须是非空数组")
    reviewer = request.get("reviewer")
    if reviewer is not None:
        reviewer = parse_str(reviewer, "reviewer")
    req_hash = request_hash(SUBMIT, {
        "batch": ref, "reviewer": reviewer, "results": results})

    with exclusive(audit_dir):
        prior = find_request(conn, rid)
        if prior is not None:
            if prior["request_hash"] != req_hash or prior["kind"] != SUBMIT:
                raise AuditError(
                    "REQUEST_ID_CONFLICT",
                    f"request_id {rid!r} 已被不同参数或不同种类的请求使用",
                    {"request_id": rid})
            return {**json.loads(prior["receipt_json"]), "replayed": True}

        with transaction(conn):
            again = find_request(conn, rid)
            if again is not None:
                if (again["request_hash"] != req_hash
                        or again["kind"] != SUBMIT):
                    raise AuditError(
                        "REQUEST_ID_CONFLICT",
                        f"request_id {rid!r} 已被不同参数使用",
                        {"request_id": rid})
                return {**json.loads(again["receipt_json"]), "replayed": True}

            batch = resolve_batch(conn, ref)
            batch_id = batch["batch_id"]
            sources = AuditSources(
                conn, batch_id, manifest_path=manifest_path,
                source_root=source_root)
            validated, errors = _validate_results(conn, sources, batch_id,
                                                  results)
            if errors:
                raise AuditError(
                    "SUBMISSION_REJECTED",
                    f"审核提交含 {len(errors)} 处非法，整批不写入",
                    {"batch_id": batch_id}, errors=errors)

            submission_id = stable_id("aus", batch_id, rid)
            ts = utc_now()
            issue_count = sum(1 for r in validated
                              if r["verdict"] == VERDICT_ISSUE)
            conn.execute(
                "INSERT INTO aud_submission(submission_id, batch_id,"
                " request_id, reviewer, result_count, issue_count,"
                " receipt_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (submission_id, batch_id, rid, reviewer, len(validated),
                 issue_count, "", ts))
            conn.executemany(
                "INSERT INTO aud_result(batch_id, subject_kind, blind_id,"
                " real_id, doc_id, verdict, note, evidence_doc_id,"
                " evidence_start, evidence_end, evidence_text_sha256,"
                " evidence_seen_text, submission_id, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (batch_id, r["subject_kind"], r["blind_id"],
                     r["real_id"], r["doc_id"], r["verdict"], r["note"],
                     r["evidence_doc_id"], r["evidence_start"],
                     r["evidence_end"], r["evidence_text_sha256"],
                     r["evidence_seen_text"], submission_id, ts)
                    for r in validated
                ])
            receipt = {
                "ok": True,
                "kind": SUBMIT,
                "request_id": rid,
                "batch_id": batch_id,
                "blind_batch_id": batch["blind_batch_id"],
                "submission_id": submission_id,
                "reviewer": reviewer,
                "result_count": len(validated),
                "issue_count": issue_count,
                "results": [
                    _safe_result_receipt(conn, batch_id, r) for r in validated
                ],
                "created_at": ts,
                "replayed": False,
            }
            conn.execute(
                "UPDATE aud_submission SET receipt_json=? WHERE"
                " submission_id=?",
                (canonical_json(receipt), submission_id))
            conn.execute(
                "INSERT INTO aud_request(request_id, request_hash, kind,"
                " batch_id, receipt_json, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (rid, req_hash, SUBMIT, batch_id, canonical_json(receipt), ts))
            return receipt


def _safe_result_receipt(conn, batch_id: str, result: dict) -> dict:
    """提交回执只含盲审标识与坐标，不含正式身份或出处。"""
    out = {
        "subject_kind": result["subject_kind"],
        "blind_id": result["blind_id"],
        "verdict": result["verdict"],
        "note": result["note"],
    }
    if result["evidence_doc_id"] is not None:
        brow = conn.execute(
            "SELECT blind_doc_id FROM aud_sample_doc WHERE batch_id=?"
            " AND doc_id=?",
            (batch_id, result["evidence_doc_id"])).fetchone()
        out["evidence"] = {
            "blind_doc_id": brow["blind_doc_id"] if brow else None,
            "start": result["evidence_start"],
            "end": result["evidence_end"],
            "text_sha256": result["evidence_text_sha256"],
        }
    return out


def _validate_results(conn, sources: AuditSources, batch_id: str,
                      results: list) -> tuple[list, list]:
    validated: list = []
    errors: list = []
    seen: set = set()
    blind_doc_cache: dict[str, str] = {}

    for index, raw in enumerate(results):
        loc = {"result_index": index}
        if not isinstance(raw, dict):
            errors.append({"code": "RESULT_FORMAT",
                           "message": "结果必须是对象", **loc})
            continue
        subject = raw.get("subject")
        if not isinstance(subject, dict):
            errors.append({"code": "RESULT_FORMAT",
                           "message": "subject 必须是对象", **loc})
            continue
        kind = subject.get("kind")
        blind_id = subject.get("blind_id")
        if kind not in ALLOWED_SUBJECT_KINDS:
            errors.append({"code": "SUBJECT_KIND",
                           "message": f"非法主体种类: {kind!r}", **loc})
            continue
        if not isinstance(blind_id, str) or not blind_id:
            errors.append({"code": "RESULT_FORMAT",
                           "message": "subject.blind_id 必须是非空字符串",
                           **loc})
            continue
        loc = {**loc, "subject_kind": kind, "blind_id": blind_id}
        row = conn.execute(
            "SELECT real_id, doc_id FROM aud_blind_map WHERE batch_id=?"
            " AND kind=? AND blind_id=?",
            (batch_id, kind, blind_id)).fetchone()
        if row is None:
            errors.append({"code": "SUBJECT_NOT_IN_BATCH",
                           "message": "盲审标识不属于本批", **loc})
            continue
        real_id = row["real_id"]
        doc_id = row["doc_id"]
        hint = subject.get("real_id")
        if hint is not None and hint != real_id:
            errors.append({"code": "SUBJECT_ID_MISMATCH",
                           "message": "正式身份与盲审标识不一致", **loc})
            continue
        # 主体必须确实存在于本批该种类快照中（防止映射表残留或伪造）
        if kind == KIND_DOC:
            owner = conn.execute(
                "SELECT 1 FROM aud_sample_doc WHERE batch_id=? AND doc_id=?",
                (batch_id, doc_id)).fetchone()
        elif kind == KIND_FRAGMENT:
            owner = conn.execute(
                "SELECT 1 FROM aud_fragment WHERE batch_id=?"
                " AND fragment_uid=? AND blind_fragment_id=?",
                (batch_id, real_id, blind_id)).fetchone()
        else:
            owner = conn.execute(
                "SELECT 1 FROM aud_relation WHERE batch_id=?"
                " AND blind_relation_id=?",
                (batch_id, blind_id)).fetchone()
        if owner is None:
            errors.append({"code": "SUBJECT_NOT_IN_BATCH",
                           "message": "主体不在本批快照中", **loc})
            continue
        key = (kind, blind_id)
        if key in seen:
            errors.append({"code": "DUPLICATE_RESULT",
                           "message": "同一批结果中重复同一主体", **loc})
            continue
        seen.add(key)
        existing = conn.execute(
            "SELECT 1 FROM aud_result WHERE batch_id=? AND subject_kind=?"
            " AND (blind_id=? OR real_id=?)",
            (batch_id, kind, blind_id, real_id)).fetchone()
        if existing is not None:
            errors.append({"code": "DUPLICATE_RESULT",
                           "message": "该主体已有审核结论", **loc})
            continue

        verdict = raw.get("verdict")
        if verdict not in (VERDICT_CONFORMING, VERDICT_ISSUE):
            errors.append({"code": "BAD_VERDICT",
                           "message": f"非法结论: {verdict!r}", **loc})
            continue
        note = raw.get("note")
        if not isinstance(note, str) or not note.strip():
            errors.append({"code": "NOTE_REQUIRED",
                           "message": "审核说明不能为空", **loc})
            continue

        evidence = raw.get("evidence")
        if evidence is None:
            if verdict == VERDICT_ISSUE:
                errors.append({"code": "EVIDENCE_REQUIRED",
                               "message": "发现问题须给出原文坐标与内容",
                               **loc})
                continue
            ev = _empty_evidence()
        else:
            ev, why = _validate_evidence(
                sources, evidence, loc, blind_doc_cache)
            if why is not None:
                errors.append(why)
                continue

        validated.append({
            "subject_kind": kind,
            "blind_id": blind_id,
            "real_id": real_id,
            "doc_id": doc_id,
            "verdict": verdict,
            "note": note,
            **ev,
        })
    return validated, errors


def _empty_evidence() -> dict:
    return {
        "evidence_doc_id": None, "evidence_start": None,
        "evidence_end": None, "evidence_text_sha256": None,
        "evidence_seen_text": None,
    }


def _validate_evidence(sources: AuditSources, raw, loc: dict,
                       blind_doc_cache: dict) -> tuple[dict, Optional[dict]]:
    if not isinstance(raw, dict):
        return {}, {"code": "EVIDENCE_FORMAT",
                    "message": "evidence 必须是对象", **loc}
    bdoc = raw.get("blind_doc_id")
    real_doc = raw.get("doc_id")
    doc_id = None
    if bdoc is not None:
        if not isinstance(bdoc, str) or not bdoc:
            return {}, {"code": "EVIDENCE_FORMAT",
                        "message": "blind_doc_id 非法", **loc}
        if bdoc not in blind_doc_cache:
            row = sources.conn.execute(
                "SELECT doc_id FROM aud_sample_doc WHERE batch_id=?"
                " AND blind_doc_id=?",
                (sources.batch_id, bdoc)).fetchone()
            if row is None:
                return {}, {"code": "EVIDENCE_DOC_NOT_IN_BATCH",
                            "message": "盲文档不属于本批", **loc}
            blind_doc_cache[bdoc] = row["doc_id"]
        doc_id = blind_doc_cache[bdoc]
    if real_doc is not None:
        if not isinstance(real_doc, str) or not real_doc:
            return {}, {"code": "EVIDENCE_FORMAT",
                        "message": "doc_id 非法", **loc}
        if doc_id is not None and doc_id != real_doc:
            return {}, {"code": "EVIDENCE_DOC_MISMATCH",
                        "message": "盲文档与正式档号不一致", **loc}
        if doc_id is None:
            doc_id = real_doc
    if doc_id is None:
        return {}, {"code": "EVIDENCE_FORMAT",
                    "message": "evidence 须给出 blind_doc_id 或 doc_id",
                    **loc}
    try:
        sources.doc_row(doc_id)
    except AuditError:
        return {}, {"code": "EVIDENCE_DOC_NOT_IN_BATCH",
                    "message": "证据原文不属于本批抽中公文", **loc}
    start = raw.get("start")
    end = raw.get("end")
    if (not isinstance(start, int) or isinstance(start, bool)
            or not isinstance(end, int) or isinstance(end, bool)):
        return {}, {"code": "EVIDENCE_FORMAT",
                    "message": "start/end 必须是整数", **loc}
    text = raw.get("text")
    if not isinstance(text, str):
        return {}, {"code": "EVIDENCE_FORMAT",
                    "message": "text 必须是字符串", **loc}
    try:
        why = sources.check_span(doc_id, start, end)
        if why is not None:
            return {}, {"code": "BAD_EVIDENCE_RANGE", "message": why,
                        "doc_id": doc_id, "start": start, "end": end, **loc}
        actual = sources.read_span(doc_id, start, end)
    except AuditError as exc:
        return {}, {**exc.to_dict(), **loc}
    try:
        expected = text.encode("utf-8")
    except UnicodeEncodeError:
        return {}, {"code": "EVIDENCE_TEXT_MISMATCH",
                    "message": "text 不是合法文本", **loc}
    if actual != expected:
        return {}, {"code": "EVIDENCE_TEXT_MISMATCH",
                    "message": "证据原文与坐标处字节不一致",
                    "doc_id": doc_id, "start": start, "end": end, **loc}
    return {
        "evidence_doc_id": doc_id,
        "evidence_start": start,
        "evidence_end": end,
        "evidence_text_sha256": sha256_hex(actual),
        "evidence_seen_text": actual.decode("utf-8"),
    }, None


# ---------------------------------------------------------------------------
# 只读视图（档案人员）
# ---------------------------------------------------------------------------

def list_batches(conn) -> list[dict]:
    return [_batch_summary(conn, row) for row in conn.execute(
        "SELECT * FROM aud_batch ORDER BY created_at, batch_id")]


def _batch_summary(conn, row) -> dict:
    c = conn.execute(
        "SELECT COUNT(*) c, COALESCE(SUM(CASE WHEN verdict='issue' THEN 1"
        " ELSE 0 END), 0) i FROM aud_result WHERE batch_id=?",
        (row["batch_id"],)).fetchone()
    return {
        "batch_id": row["batch_id"],
        "blind_batch_id": row["blind_batch_id"],
        "release_id": row["release_id"],
        "rev_id": row["rev_id"],
        "sample_size": row["sample_size"],
        "seed": row["seed"],
        "population_size": row["population_size"],
        "sample_digest": row["sample_digest"],
        "created_at": row["created_at"],
        "fragment_count": row["fragment_count"],
        "relation_count": row["relation_count"],
        "result_count": c["c"],
        "issue_count": c["i"],
        "submission_count": conn.execute(
            "SELECT COUNT(*) c FROM aud_submission WHERE batch_id=?",
            (row["batch_id"],)).fetchone()["c"],
    }


def get_batch(conn, ref: str) -> dict:
    row = resolve_batch(conn, ref)
    out = _batch_summary(conn, row)
    out.update({
        "rev_seq": row["rev_seq"],
        "release_doc_ids": json.loads(row["release_doc_ids_json"]),
        "population_digest": row["population_digest"],
        "release_digest": row["release_digest"],
        "created_request_id": row["created_request_id"],
        "documents": [
            {
                "doc_id": r["doc_id"],
                "blind_doc_id": r["blind_doc_id"],
                "sample_rank": r["sample_rank"],
                "source_sha256": r["source_sha256"],
                "byte_length": r["byte_length"],
            }
            for r in conn.execute(
                "SELECT * FROM aud_sample_doc WHERE batch_id=?"
                " ORDER BY sample_rank", (row["batch_id"],))
        ],
        "submissions": [
            {
                "submission_id": r["submission_id"],
                "request_id": r["request_id"],
                "reviewer": r["reviewer"],
                "result_count": r["result_count"],
                "issue_count": r["issue_count"],
                "created_at": r["created_at"],
            }
            for r in conn.execute(
                "SELECT * FROM aud_submission WHERE batch_id=?"
                " ORDER BY created_at, submission_id", (row["batch_id"],))
        ],
    })
    return out


def batch_coverage(conn, ref: str) -> dict:
    """每篇抽中公文的记录覆盖与未审计数。"""
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    docs = []
    totals = {"fragments": 0, "relations": 0, "reviewed_fragments": 0,
              "reviewed_relations": 0, "documents_reviewed": 0,
              "unreviewed": 0}
    for d in conn.execute(
            "SELECT * FROM aud_sample_doc WHERE batch_id=?"
            " ORDER BY sample_rank", (batch_id,)):
        doc_id = d["doc_id"]
        f_total = conn.execute(
            "SELECT COUNT(*) c FROM aud_fragment WHERE batch_id=?"
            " AND doc_id=?", (batch_id, doc_id)).fetchone()["c"]
        r_total = conn.execute(
            "SELECT COUNT(*) c FROM aud_relation WHERE batch_id=?"
            " AND doc_id=?", (batch_id, doc_id)).fetchone()["c"]
        f_rev = conn.execute(
            "SELECT COUNT(*) c FROM aud_result WHERE batch_id=?"
            " AND subject_kind='fragment' AND doc_id=?",
            (batch_id, doc_id)).fetchone()["c"]
        r_rev = conn.execute(
            "SELECT COUNT(*) c FROM aud_result WHERE batch_id=?"
            " AND subject_kind='relation' AND doc_id=?",
            (batch_id, doc_id)).fetchone()["c"]
        doc_rev = conn.execute(
            "SELECT COUNT(*) c FROM aud_result WHERE batch_id=?"
            " AND subject_kind='document' AND doc_id=?",
            (batch_id, doc_id)).fetchone()["c"]
        issues = conn.execute(
            "SELECT COUNT(*) c FROM aud_result WHERE batch_id=?"
            " AND doc_id=? AND verdict='issue'",
            (batch_id, doc_id)).fetchone()["c"]
        unreviewed = ((0 if doc_rev else 1) + (f_total - f_rev)
                      + (r_total - r_rev))
        docs.append({
            "doc_id": doc_id,
            "blind_doc_id": d["blind_doc_id"],
            "sample_rank": d["sample_rank"],
            "document_reviewed": bool(doc_rev),
            "fragments": f_total,
            "fragments_reviewed": f_rev,
            "relations": r_total,
            "relations_reviewed": r_rev,
            "unreviewed": unreviewed,
            "issues": issues,
        })
        totals["fragments"] += f_total
        totals["relations"] += r_total
        totals["reviewed_fragments"] += f_rev
        totals["reviewed_relations"] += r_rev
        totals["documents_reviewed"] += 1 if doc_rev else 0
        totals["unreviewed"] += unreviewed
    totals["documents"] = len(docs)
    totals["issues"] = conn.execute(
        "SELECT COUNT(*) c FROM aud_result WHERE batch_id=?"
        " AND verdict='issue'", (batch_id,)).fetchone()["c"]
    return {
        "batch_id": batch_id,
        "blind_batch_id": batch["blind_batch_id"],
        "sampled_documents": len(docs),
        "totals": totals,
        "documents": docs,
    }


def stream_unreviewed(conn, ref: str) -> Iterator[str]:
    """档案只读：未审记录（含正式身份与盲审标识）。"""
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    sqls = (
        ("document",
         "SELECT d.doc_id, d.blind_doc_id FROM aud_sample_doc d"
         " WHERE d.batch_id=? AND NOT EXISTS ("
         "  SELECT 1 FROM aud_result r WHERE r.batch_id=d.batch_id"
         "  AND r.subject_kind='document' AND r.doc_id=d.doc_id)"
         " ORDER BY d.sample_rank"),
        ("fragment",
         "SELECT f.doc_id, f.blind_doc_id, f.fragment_uid,"
         " f.blind_fragment_id FROM aud_fragment f"
         " WHERE f.batch_id=? AND NOT EXISTS ("
         "  SELECT 1 FROM aud_result r WHERE r.batch_id=f.batch_id"
         "  AND r.subject_kind='fragment' AND r.real_id=f.fragment_uid)"
         " ORDER BY f.doc_id, f.layer, f.start_byte, f.end_byte,"
         " f.fragment_uid"),
        ("relation",
         "SELECT r.doc_id, r.blind_doc_id, r.blind_relation_id, r.rel_type,"
         " r.blind_from, r.blind_to FROM aud_relation r"
         " WHERE r.batch_id=? AND NOT EXISTS ("
         "  SELECT 1 FROM aud_result x WHERE x.batch_id=r.batch_id"
         "  AND x.subject_kind='relation' AND x.blind_id=r.blind_relation_id)"
         " ORDER BY r.doc_id, r.rel_type, r.blind_relation_id"),
    )
    for kind, sql in sqls:
        for row in conn.execute(sql, (batch_id,)):
            payload = {"record_kind": kind, **dict(row)}
            yield json.dumps(payload, ensure_ascii=False,
                             sort_keys=True) + "\n"


def stream_results(conn, ref: str) -> Iterator[str]:
    """档案只读：审核结论与可定位的原文证据（正式身份 + 盲审标识）。"""
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    for row in conn.execute(
            "SELECT r.*, s.reviewer FROM aud_result r"
            " LEFT JOIN aud_submission s ON s.submission_id=r.submission_id"
            " WHERE r.batch_id=?"
            " ORDER BY r.doc_id, r.subject_kind, r.blind_id", (batch_id,)):
        payload = {
            "record_kind": "result",
            "doc_id": row["doc_id"],
            "subject_kind": row["subject_kind"],
            "blind_id": row["blind_id"],
            "real_id": row["real_id"],
            "verdict": row["verdict"],
            "note": row["note"],
            "submission_id": row["submission_id"],
            "reviewer": row["reviewer"],
            "created_at": row["created_at"],
        }
        if row["evidence_doc_id"] is not None:
            payload["evidence"] = {
                "doc_id": row["evidence_doc_id"],
                "start": row["evidence_start"],
                "end": row["evidence_end"],
                "text_sha256": row["evidence_text_sha256"],
                "text": row["evidence_seen_text"],
            }
        yield json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"


def stream_snapshot(conn, ref: str) -> Iterator[str]:
    """档案只读：批次快照（发布绑定、正式身份与盲审标识、全部记录）。"""
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    yield json.dumps({
        "record_kind": "batch",
        "batch_id": batch_id,
        "blind_batch_id": batch["blind_batch_id"],
        "release_id": batch["release_id"],
        "rev_id": batch["rev_id"],
        "rev_seq": batch["rev_seq"],
        "release_doc_ids": json.loads(batch["release_doc_ids_json"]),
        "sample_size": batch["sample_size"],
        "seed": batch["seed"],
        "population_size": batch["population_size"],
        "population_digest": batch["population_digest"],
        "sample_digest": batch["sample_digest"],
        "release_digest": batch["release_digest"],
    }, ensure_ascii=False, sort_keys=True) + "\n"
    for d in conn.execute(
            "SELECT * FROM aud_sample_doc WHERE batch_id=?"
            " ORDER BY sample_rank", (batch_id,)):
        yield json.dumps({
            "record_kind": "document",
            "doc_id": d["doc_id"],
            "blind_doc_id": d["blind_doc_id"],
            "sample_rank": d["sample_rank"],
            "source_sha256": d["source_sha256"],
            "byte_length": d["byte_length"],
        }, ensure_ascii=False, sort_keys=True) + "\n"
    for f in conn.execute(
            "SELECT * FROM aud_fragment WHERE batch_id=?"
            " ORDER BY doc_id, layer, start_byte, end_byte, fragment_uid",
            (batch_id,)):
        yield json.dumps({
            "record_kind": "fragment",
            "doc_id": f["doc_id"],
            "blind_doc_id": f["blind_doc_id"],
            "fragment_uid": f["fragment_uid"],
            "blind_fragment_id": f["blind_fragment_id"],
            "layer": f["layer"],
            "label": f["label"],
            "start": f["start_byte"],
            "end": f["end_byte"],
            "text_sha256": f["text_sha256"],
            "text": f["seen_text"],
        }, ensure_ascii=False, sort_keys=True) + "\n"
    for r in conn.execute(
            "SELECT * FROM aud_relation WHERE batch_id=?"
            " ORDER BY doc_id, rel_type, from_fragment, to_fragment",
            (batch_id,)):
        yield json.dumps({
            "record_kind": "relation",
            "doc_id": r["doc_id"],
            "blind_doc_id": r["blind_doc_id"],
            "blind_relation_id": r["blind_relation_id"],
            "rel_type": r["rel_type"],
            "from_fragment": r["from_fragment"],
            "to_fragment": r["to_fragment"],
            "blind_from": r["blind_from"],
            "blind_to": r["blind_to"],
        }, ensure_ascii=False, sort_keys=True) + "\n"


# ---------------------------------------------------------------------------
# 盲审导出（审核员可见；不含正式身份、团队、投票、来源、理由）
# ---------------------------------------------------------------------------

_BLIND_FORBIDDEN_KEYS = frozenset({
    "batch_id", "release_id", "rev_id", "rev_seq", "doc_id", "fragment_uid",
    "ticket_uid", "reviewer_id", "reviewer", "team_id", "package_uid",
    "origin", "source_kind", "reason", "evidence_json", "request_id",
    "blind_secret", "sample_doc_ids", "blind_doc_ids", "real_id",
    "population_digest", "sample_digest", "release_digest",
    "release_doc_ids", "created_request_id",
})


def stream_blind(conn, ref: str) -> Iterator[str]:
    """审核员盲审导出（JSONL，服务端游标逐行）。

    每行只含盲审标识、标注层/标签、字节坐标、片段原文与片段摘要；
    不输出正式档号、发布/版本、团队、投票、裁决来源或理由。
    """
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    yield json.dumps({
        "record_kind": "batch",
        "blind_batch_id": batch["blind_batch_id"],
        "sample_size": batch["sample_size"],
        "population_size": batch["population_size"],
        "seed": batch["seed"],
    }, ensure_ascii=False, sort_keys=True) + "\n"
    for d in conn.execute(
            "SELECT * FROM aud_sample_doc WHERE batch_id=?"
            " ORDER BY sample_rank", (batch_id,)):
        yield json.dumps({
            "record_kind": "document",
            "blind_doc_id": d["blind_doc_id"],
            "sample_rank": d["sample_rank"],
            "byte_length": d["byte_length"],
            "source_sha256": d["source_sha256"],
        }, ensure_ascii=False, sort_keys=True) + "\n"
        for f in conn.execute(
                "SELECT * FROM aud_fragment WHERE batch_id=? AND doc_id=?"
                " ORDER BY layer, start_byte, end_byte, label,"
                " blind_fragment_id", (batch_id, d["doc_id"])):
            yield json.dumps({
                "record_kind": "fragment",
                "blind_doc_id": f["blind_doc_id"],
                "blind_fragment_id": f["blind_fragment_id"],
                "layer": f["layer"],
                "label": f["label"],
                "start": f["start_byte"],
                "end": f["end_byte"],
                "text_sha256": f["text_sha256"],
                "text": f["seen_text"],
            }, ensure_ascii=False, sort_keys=True) + "\n"
        for r in conn.execute(
                "SELECT * FROM aud_relation WHERE batch_id=? AND doc_id=?"
                " ORDER BY rel_type, blind_relation_id",
                (batch_id, d["doc_id"])):
            yield json.dumps({
                "record_kind": "relation",
                "blind_doc_id": r["blind_doc_id"],
                "blind_relation_id": r["blind_relation_id"],
                "rel_type": r["rel_type"],
                "from": r["blind_from"],
                "to": r["blind_to"],
            }, ensure_ascii=False, sort_keys=True) + "\n"


def blind_leaks(payload: dict) -> list[str]:
    """返回盲审行中不应出现的字段（测试与 CLI 自检共用）。"""
    return sorted(set(payload) & _BLIND_FORBIDDEN_KEYS)


def resolve_blind(conn, ref: str, blind_id: str) -> dict:
    """档案查询：把一个本批盲审标识解析回正式身份（不得用于盲审导出）。"""
    batch = resolve_batch(conn, ref)
    row = conn.execute(
        "SELECT * FROM aud_blind_map WHERE batch_id=? AND blind_id=?",
        (batch["batch_id"], blind_id)).fetchone()
    if row is None:
        raise AuditError("BLIND_ID_NOT_FOUND",
                         f"blind id {blind_id!r} not in batch")
    return {
        "batch_id": batch["batch_id"],
        "kind": row["kind"],
        "blind_id": row["blind_id"],
        "real_id": row["real_id"],
        "doc_id": row["doc_id"],
    }


def blind_source(conn, ref: str, blind_doc_id: str,
                 start: Optional[int] = None, end: Optional[int] = None,
                 *, manifest_path: Optional[str] = None,
                 source_root: Optional[str] = None) -> dict:
    """按盲文档标识取原文；给出区间时只返回精确字节范围。"""
    batch = resolve_batch(conn, ref)
    batch_id = batch["batch_id"]
    d = conn.execute(
        "SELECT * FROM aud_sample_doc WHERE batch_id=? AND blind_doc_id=?",
        (batch_id, blind_doc_id)).fetchone()
    if d is None:
        raise AuditError("BLIND_DOC_NOT_FOUND",
                         f"blind document {blind_doc_id!r} not in batch")
    sources = AuditSources(conn, batch_id, manifest_path=manifest_path,
                           source_root=source_root)
    doc_id = d["doc_id"]
    if start is None and end is None:
        return {"blind_doc_id": blind_doc_id,
                "byte_length": d["byte_length"],
                "text": sources.read_text(doc_id)}
    if (not isinstance(start, int) or isinstance(start, bool)
            or not isinstance(end, int) or isinstance(end, bool)):
        raise AuditError("BAD_REQUEST", "start/end 必须是整数")
    why = sources.check_span(doc_id, start, end)
    if why is not None:
        raise AuditError("BAD_EVIDENCE_RANGE", why,
                         {"start": start, "end": end})
    raw = sources.read_span(doc_id, start, end)
    return {
        "blind_doc_id": blind_doc_id,
        "start": start,
        "end": end,
        "text_sha256": sha256_hex(raw),
        "text": raw.decode("utf-8"),
    }


def init_store(audit_dir: str) -> dict:
    """显式初始化空审计库（可选；create 也会自动初始化）。"""
    conn = open_audit(audit_dir)
    try:
        with transaction(conn):
            from .audit_common import meta_get, meta_set

            if meta_get(conn, "format_version") is None:
                meta_set(conn, "format_version", "1")
                meta_set(conn, "created_at", utc_now())
        return {"audit_dir": audit_dir, "format_version": "1"}
    finally:
        conn.close()
