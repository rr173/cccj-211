# -*- coding: utf-8 -*-
"""工作区初始化：从既有构建库复制只读底稿，固定人工裁决策略。

- 原构建库以 ``mode=ro`` 打开，只复制，不改写；
- 工作区内的 ``base.db`` 副本此后是唯一底稿，并记录原库路径/摘要；
- 人工裁决所用的层、标签与关系端点层约束在初始化时提供并固定，
  因为构建库只保存计票结果，不保存策略全文；
- 底稿的区域判定（胜出/无片段/待定）、引用归票判定（存在/不存在/待定）
  与被阻断原始关系原样导入初版（第 0 版），不重新计票、不重读标注包。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from typing import Optional

from .policy import Policy, load_policy
from .util import canonical_json, stable_id
from .ws_common import (
    BASE_COPY_NAME,
    WsError,
    base_copy_path,
    connect,
    db_path,
    meta_set,
    transaction,
    utc_now,
)

INITIAL_SEQ = 0
BATCH = 2000


def _sha256_file(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _open_base_ro(path: str) -> sqlite3.Connection:
    uri = "file:" + os.path.abspath(path) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def init_workspace(
    workspace: str,
    base_db_path: str,
    policy_path: str,
    *,
    reviewer: Optional[str] = None,
) -> dict:
    """初始化独立工作区；返回摘要信息。"""
    workspace = os.path.abspath(workspace)
    base_db_path = os.path.abspath(base_db_path)
    if not os.path.isfile(base_db_path):
        raise WsError("BASE_DB_NOT_FOUND", f"base build db not found: {base_db_path}")
    if os.path.exists(db_path(workspace)):
        raise WsError(
            "WORKSPACE_EXISTS",
            f"workspace already initialised: {workspace}",
        )
    policy = load_policy(policy_path)
    os.makedirs(workspace, exist_ok=True)

    digest, size = _sha256_file(base_db_path)
    copy_path = base_copy_path(workspace)
    _atomic_copy(base_db_path, copy_path)
    copy_digest, copy_size = _sha256_file(copy_path)
    if (copy_digest, copy_size) != (digest, size):
        os.remove(copy_path)
        raise WsError("BASE_COPY_MISMATCH", "base copy digest differs from source")

    conn = connect(workspace, create=True)
    try:
        with transaction(conn):
            _store_policy(conn, policy)
            _store_meta(conn, base_db_path, digest, size, reviewer)
            _import_base(conn, copy_path, policy)
        conn.commit()
    finally:
        conn.close()
    return {
        "workspace": workspace,
        "base_db": base_db_path,
        "base_sha256": digest,
        "base_bytes": size,
        "policy_sha256": _policy_sha(canonical_json(_policy_raw(policy))),
        "head_rev": stable_id("rev", INITIAL_SEQ, "", "initial", digest),
    }


def _atomic_copy(src: str, dst: str) -> None:
    out_dir = os.path.dirname(dst) or "."
    fd, tmp = tempfile.mkstemp(prefix=".base-copy-", suffix=".db", dir=out_dir)
    os.close(fd)
    try:
        with open(src, "rb") as s, open(tmp, "wb") as d:
            while True:
                chunk = s.read(1024 * 1024)
                if not chunk:
                    break
                d.write(chunk)
            d.flush()
            os.fsync(d.fileno())
        os.replace(tmp, dst)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    # 副本本身设为只读（工作区查询以 immutable 方式打开它）
    os.chmod(dst, 0o444)


def _policy_raw(policy: Policy) -> dict:
    return {
        "layers": {
            name: {"labels": list(lp.labels), "min_support": lp.min_support}
            for name, lp in sorted(policy.layers.items())
        },
        "relation_types": {
            name: {
                "from_layer": rp.from_layer,
                "to_layer": rp.to_layer,
                "min_support": rp.min_support,
            }
            for name, rp in sorted(policy.relation_types.items())
        },
    }


def _policy_sha(raw_json: str) -> str:
    return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()


def _store_policy(conn: sqlite3.Connection, policy: Policy) -> None:
    raw = canonical_json(_policy_raw(policy))
    conn.execute(
        "INSERT INTO ws_policy(id, raw_json, policy_sha256) VALUES (1,?,?)",
        (raw, _policy_sha(raw)),
    )
    conn.executemany(
        "INSERT INTO ws_layer(layer, labels_json) VALUES (?,?)",
        [
            (name, canonical_json(list(lp.labels)))
            for name, lp in sorted(policy.layers.items())
        ],
    )
    conn.executemany(
        "INSERT INTO ws_relation_type(rel_type, from_layer, to_layer)"
        " VALUES (?,?,?)",
        [
            (name, rp.from_layer, rp.to_layer)
            for name, rp in sorted(policy.relation_types.items())
        ],
    )


def _store_meta(conn, base_db_path, digest, size, reviewer) -> None:
    meta_set(conn, "format_version", "1")
    meta_set(conn, "created_at", utc_now())
    meta_set(conn, "base_db_path", base_db_path)
    meta_set(conn, "base_db_sha256", digest)
    meta_set(conn, "base_copy", BASE_COPY_NAME)
    if reviewer:
        meta_set(conn, "initial_reviewer", reviewer)


def _import_base(conn: sqlite3.Connection, copy_path: str, policy: Policy) -> None:
    base = _open_base_ro(copy_path)
    try:
        actual = _sha256_file(copy_path)[0]
        build_meta = {
            r["key"]: r["value"]
            for r in base.execute("SELECT key, value FROM build_meta")
        }
        _check_policy_binding(base, build_meta, policy)
        conn.execute(
            "INSERT INTO base_db(id, path, sha256, byte_length, copy_name,"
            " build_meta_json, imported_at) VALUES (1,?,?,?,?,?,?)",
            (
                conn.execute(
                    "SELECT value FROM ws_meta WHERE key='base_db_path'"
                ).fetchone()["value"],
                actual,
                os.path.getsize(copy_path),
                BASE_COPY_NAME,
                canonical_json(build_meta),
                utc_now(),
            ),
        )
        _copy_sources(conn, base)
        _copy_packages_and_proposals(conn, base)
        _copy_region_snapshot(conn, base)
        _copy_relation_snapshot(conn, base)
        _copy_blocked(conn, base)
        _seed_initial_revision(conn)
    finally:
        base.close()


def _check_policy_binding(base, build_meta: dict, policy: Policy) -> None:
    """固定策略必须覆盖构建库实际用到的层与关系类型。"""
    try:
        base_layers = set(json.loads(build_meta.get("layers", "[]")))
        base_rels = set(json.loads(build_meta.get("relation_types", "[]")))
    except (TypeError, json.JSONDecodeError):
        raise WsError("BAD_BASE_META", "base build_meta lacks layers/relation_types")
    missing_layers = base_layers - set(policy.layers)
    missing_rels = base_rels - set(policy.relation_types)
    if missing_layers or missing_rels:
        raise WsError(
            "POLICY_BINDING_MISMATCH",
            "workspace policy does not cover the base build",
            {
                "missing_layers": sorted(missing_layers),
                "missing_relation_types": sorted(missing_rels),
            },
        )
    for name, rp in policy.relation_types.items():
        if rp.from_layer not in policy.layers or rp.to_layer not in policy.layers:
            raise WsError(
                "POLICY_BINDING_MISMATCH",
                f"relation type {name!r} endpoint layers not declared",
            )


def _copy_sources(conn, base) -> None:
    rows = [
        (
            r["doc_id"], r["rel_path"], None, r["sha256"],
            r["byte_length"], None, 0,
        )
        for r in base.execute(
            "SELECT doc_id, rel_path, sha256, byte_length FROM source_doc"
            " ORDER BY doc_id"
        )
    ]
    conn.executemany(
        "INSERT INTO source_doc(doc_id, rel_path, abs_path, sha256, byte_length,"
        " verified_sha, verified) VALUES (?,?,?,?,?,?,?)",
        rows,
    )


def _copy_packages_and_proposals(conn, base) -> None:
    conn.executemany(
        "INSERT INTO base_package(package_uid, package_id, doc_id, team_id,"
        " content_hash) VALUES (?,?,?,?,?)",
        [
            (r["package_uid"], r["package_id"], r["doc_id"], r["team_id"],
             r["content_hash"])
            for r in base.execute("SELECT * FROM package ORDER BY package_uid")
        ],
    )
    rows = []
    for r in base.execute(
        "SELECT p.proposal_uid, p.package_uid, p.doc_id, p.layer, p.local_id,"
        " p.label, p.start_byte, p.end_byte, rp.region_uid"
        " FROM proposal p LEFT JOIN region_proposal rp"
        " ON rp.proposal_uid = p.proposal_uid"
        " ORDER BY p.proposal_uid"
    ):
        rows.append(tuple(r))
        if len(rows) >= BATCH:
            conn.executemany(
                "INSERT INTO base_proposal(proposal_uid, package_uid, doc_id,"
                " layer, local_id, label, start_byte, end_byte, region_uid)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                rows,
            )
            rows.clear()
    if rows:
        conn.executemany(
            "INSERT INTO base_proposal(proposal_uid, package_uid, doc_id,"
            " layer, local_id, label, start_byte, end_byte, region_uid)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            rows,
        )


def _copy_region_snapshot(conn, base) -> None:
    conn.executemany(
        "INSERT INTO base_region(region_uid, doc_id, layer, start_byte, end_byte,"
        " outcome_kind, winner_candidate, denominator, strict_majority)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (
                r["region_uid"], r["doc_id"], r["layer"], r["start_byte"],
                r["end_byte"], r["winner_kind"] or "pending",
                r["winner_candidate"], r["denominator"], r["strict_majority"],
            )
            for r in base.execute(
                "SELECT r.*, o.winner_kind, o.winner_candidate, o.denominator,"
                " o.strict_majority FROM region r LEFT JOIN region_outcome o"
                " ON o.region_uid = r.region_uid ORDER BY r.region_uid"
            )
        ],
    )
    conn.executemany(
        "INSERT INTO base_region_candidate(candidate_uid, region_uid, start_byte,"
        " end_byte, label, is_no_fragment) VALUES (?,?,?,?,?,?)",
        [
            (r["candidate_uid"], r["region_uid"], r["start_byte"],
             r["end_byte"], r["label"], r["is_no_fragment"])
            for r in base.execute("SELECT * FROM region_candidate")
        ],
    )
    conn.executemany(
        "INSERT INTO base_official_fragment(fragment_uid, doc_id, layer, label,"
        " start_byte, end_byte, region_uid, candidate_uid, text_sha256, seen_text)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (r["fragment_uid"], r["doc_id"], r["layer"], r["label"],
             r["start_byte"], r["end_byte"], r["region_uid"],
             r["candidate_uid"], r["text_sha256"], r["seen_text"])
            for r in base.execute("SELECT * FROM official_fragment")
        ],
    )


def _copy_relation_snapshot(conn, base) -> None:
    conn.executemany(
        "INSERT INTO base_relation_group(rel_group_uid, doc_id, rel_type,"
        " from_fragment, to_fragment, denominator, for_votes, against_votes,"
        " conflict_votes, strict_majority, min_support, decision)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            tuple(r)
            for r in base.execute(
                "SELECT rel_group_uid, doc_id, rel_type, from_fragment,"
                " to_fragment, denominator, for_votes, against_votes,"
                " conflict_votes, strict_majority, min_support, decision"
                " FROM relation_group ORDER BY rel_group_uid"
            )
        ],
    )
    conn.executemany(
        "INSERT INTO base_relation_group_vote(rel_group_uid, package_uid,"
        " team_id, sign, record_uids) VALUES (?,?,?,?,?)",
        [
            tuple(r)
            for r in base.execute(
                "SELECT rel_group_uid, package_uid, team_id, sign, record_uids"
                " FROM relation_group_vote ORDER BY rel_group_uid, package_uid"
            )
        ],
    )
    conn.executemany(
        "INSERT INTO base_review_item(review_uid, item_kind, doc_id, layer,"
        " rel_type, region_uid, rel_group_uid, record_uid, reason_code,"
        " detail_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            tuple(r)
            for r in base.execute(
                "SELECT review_uid, item_kind, doc_id, layer, rel_type,"
                " region_uid, rel_group_uid, record_uid, reason_code, detail_json"
                " FROM review_item ORDER BY review_uid"
            )
        ],
    )


def _copy_blocked(conn, base) -> None:
    rows = []
    cur = base.execute(
        "SELECT ri.record_uid, ri.doc_id, ri.rel_type, ri.detail_json,"
        " rr.package_uid, rr.team_id, rr.from_local, rr.to_local, rr.sign,"
        " re.from_proposal, re.to_proposal, re.blocked_reason,"
        " rec.file_name, rec.line_no, rec.record_index, rec.payload_json"
        " FROM review_item ri"
        " JOIN raw_relation rr ON rr.record_uid = ri.record_uid"
        " JOIN record rec ON rec.record_uid = ri.record_uid"
        " LEFT JOIN relation_endpoint re ON re.record_uid = ri.record_uid"
        " WHERE ri.item_kind='relation' AND ri.rel_group_uid IS NULL"
        " ORDER BY ri.record_uid"
    )
    for r in cur:
        from_region = _proposal_region(base, r["from_proposal"])
        to_region = _proposal_region(base, r["to_proposal"])
        rows.append(
            (
                r["record_uid"], r["doc_id"], r["rel_type"], r["package_uid"],
                r["team_id"], r["from_local"], r["to_local"], r["sign"],
                r["from_proposal"], r["to_proposal"], from_region, to_region,
                r["blocked_reason"], r["file_name"], r["line_no"],
                r["record_index"], r["payload_json"],
            )
        )
    conn.executemany(
        "INSERT INTO base_blocked(record_uid, doc_id, rel_type, package_uid,"
        " team_id, from_local, to_local, sign, from_proposal, to_proposal,"
        " from_region, to_region, blocked_reason, file_name, line_no,"
        " record_index, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )


def _proposal_region(base, proposal_uid) -> Optional[str]:
    if proposal_uid is None:
        return None
    row = base.execute(
        "SELECT region_uid FROM region_proposal WHERE proposal_uid=?",
        (proposal_uid,),
    ).fetchone()
    return row["region_uid"] if row else None


def _seed_initial_revision(conn) -> None:
    """第 0 版：原样承接自动计票结果。"""
    base_digest = conn.execute(
        "SELECT sha256 FROM base_db WHERE id=1"
    ).fetchone()["sha256"]
    rev_id = stable_id("rev", INITIAL_SEQ, "", "initial", base_digest)
    conn.execute(
        "INSERT INTO revision(rev_id, seq, parent_rev, base_rev, ticket_uid,"
        " created_at, reason_json) VALUES (?,?,?,?,?,?,?)",
        (
            rev_id, INITIAL_SEQ, None, None, None, utc_now(),
            canonical_json({"kind": "initial_import", "base_db_sha256":
                            base_digest}),
        ),
    )
    conn.execute(
        "INSERT INTO state_evidence(subject_kind, subject_key, seq, source_uid,"
        " ticket_uid, reviewer_id, reason, evidence_json, rev_id)"
        " VALUES ('revision', ?, ?, 'auto', NULL, NULL, ?, ?, ?)",
        (
            rev_id, INITIAL_SEQ,
            "initial import of automatic QC verdicts",
            canonical_json({"base_db_sha256": base_digest}),
            rev_id,
        ),
    )
    meta_set(conn, "head_rev", rev_id)
    meta_set(conn, "head_seq", str(INITIAL_SEQ))

    # 区域整组：胜出 -> 具体片段；无片段 -> none；待定 -> pending
    for reg in conn.execute("SELECT * FROM base_region ORDER BY region_uid"):
        kind = reg["outcome_kind"]
        fragments = []
        if kind == "candidate":
            frag = conn.execute(
                "SELECT fragment_uid FROM base_official_fragment"
                " WHERE region_uid=? AND candidate_uid=?",
                (reg["region_uid"], reg["winner_candidate"]),
            ).fetchone()
            if frag is None:
                raise WsError(
                    "BASE_INCONSISTENT",
                    f"region {reg['region_uid']} won but no official fragment",
                )
            fragments = [frag["fragment_uid"]]
        elif kind not in ("none", "no_fragment", "pending"):
            raise WsError("BASE_INCONSISTENT", f"unknown outcome kind {kind!r}")
        if kind in ("none", "no_fragment"):
            kind = "none"
        conn.execute(
            "INSERT INTO region_state(region_uid, doc_id, layer, kind,"
            " fragments_json, seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                reg["region_uid"], reg["doc_id"], reg["layer"], kind,
                canonical_json(fragments), INITIAL_SEQ, None, rev_id,
            ),
        )

    # 审定片段：自动胜出的片段原样承接
    for frag in conn.execute(
        "SELECT * FROM base_official_fragment ORDER BY fragment_uid"
    ):
        conn.execute(
            "INSERT INTO fragment_state(fragment_uid, doc_id, layer, label,"
            " start_byte, end_byte, text_sha256, seen_text, origin, region_uid,"
            " seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                frag["fragment_uid"], frag["doc_id"], frag["layer"],
                frag["label"], frag["start_byte"], frag["end_byte"],
                frag["text_sha256"], frag["seen_text"], "auto",
                frag["region_uid"], INITIAL_SEQ, None, rev_id,
            ),
        )

    # 引用判定：存在 / 不存在 / 待定
    for grp in conn.execute(
        "SELECT * FROM base_relation_group ORDER BY rel_group_uid"
    ):
        decision = grp["decision"]
        if decision == "confirmed":
            kind, origin = "present", "auto"
        elif decision == "confirmed_absent":
            kind, origin = "absent", "auto"
        else:
            kind, origin = "pending", "auto"
        conn.execute(
            "INSERT INTO relation_state(doc_id, rel_type, from_fragment,"
            " to_fragment, kind, origin, seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                grp["doc_id"], grp["rel_type"], grp["from_fragment"],
                grp["to_fragment"], kind, origin, INITIAL_SEQ, None, rev_id,
            ),
        )
        if decision == "confirmed":
            conn.execute(
                "INSERT INTO state_evidence(subject_kind, subject_key, seq,"
                " source_uid, ticket_uid, reviewer_id, reason, evidence_json,"
                " rev_id) VALUES ('relation', ?, ?, 'auto', NULL, NULL, ?, ?, ?)",
                (
                    canonical_json([
                        grp["doc_id"], grp["rel_type"], grp["from_fragment"],
                        grp["to_fragment"],
                    ]),
                    INITIAL_SEQ,
                    "confirmed by automatic QC vote tally",
                    canonical_json({
                        "rel_group_uid": grp["rel_group_uid"],
                        "for": grp["for_votes"],
                        "against": grp["against_votes"],
                        "conflict": grp["conflict_votes"],
                        "denominator": grp["denominator"],
                        "decision": decision,
                    }),
                    rev_id,
                ),
            )
