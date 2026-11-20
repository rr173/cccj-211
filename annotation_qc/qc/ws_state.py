# -*- coding: utf-8 -*-
"""审定状态的按版本读取。

状态以时间行保存：一行的生效区间为 ``[seq_added, seq_removed)``。
读取某个版本时只取在该版本生效的行；同一业务键最多一行生效。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

from .util import canonical_json
from .ws_common import WsError

REGION_FRAGMENTS = "fragments"
REGION_NONE = "none"
REGION_PENDING = "pending"

REL_PRESENT = "present"
REL_ABSENT = "absent"
REL_RETIRED = "retired"
REL_PENDING = "pending"

BLOCK_ADOPTED = "adopted"
BLOCK_REJECTED = "rejected"

_LIVE = "(seq_added <= ? AND (seq_removed IS NULL OR seq_removed > ?))"


# ---------------------------------------------------------------------------
# 片段
# ---------------------------------------------------------------------------

def fragment_row(conn: sqlite3.Connection, fragment_uid: str,
                 seq: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fragment_state WHERE fragment_uid=? AND " + _LIVE +
        " ORDER BY seq_added DESC LIMIT 1",
        (fragment_uid, seq, seq),
    ).fetchone()


def fragments_at(
    conn: sqlite3.Connection,
    seq: int,
    doc_id: Optional[str] = None,
    layer: Optional[str] = None,
) -> dict[str, sqlite3.Row]:
    sql = "SELECT * FROM fragment_state WHERE " + _LIVE
    params: list = [seq, seq]
    if doc_id is not None:
        sql += " AND doc_id=?"
        params.append(doc_id)
    if layer is not None:
        sql += " AND layer=?"
        params.append(layer)
    out: dict[str, sqlite3.Row] = {}
    for row in conn.execute(sql, params):
        # 同一 uid 若异常出现多行，取最新生效行
        prev = out.get(row["fragment_uid"])
        if prev is None or row["seq_added"] > prev["seq_added"]:
            out[row["fragment_uid"]] = row
    return out


def fragment_region_map(conn: sqlite3.Connection, seq: int,
                        doc_id: str) -> dict[str, str]:
    """由区域整组判定反查片段所属区域（不含无来源的人工片段）。"""
    out: dict[str, str] = {}
    for reg in region_states_at(conn, seq, doc_id):
        for uid in json.loads(reg["fragments_json"]):
            out.setdefault(uid, reg["region_uid"])
    return out


# ---------------------------------------------------------------------------
# 区域整组
# ---------------------------------------------------------------------------

def region_state_row(conn: sqlite3.Connection, region_uid: str,
                     seq: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM region_state WHERE region_uid=? AND " + _LIVE +
        " ORDER BY seq_added DESC LIMIT 1",
        (region_uid, seq, seq),
    ).fetchone()


def region_states_at(
    conn: sqlite3.Connection, seq: int, doc_id: Optional[str] = None
) -> dict[str, sqlite3.Row]:
    sql = "SELECT * FROM region_state WHERE " + _LIVE
    params: list = [seq, seq]
    if doc_id is not None:
        sql += " AND doc_id=?"
        params.append(doc_id)
    out: dict[str, sqlite3.Row] = {}
    for row in conn.execute(sql, params):
        prev = out.get(row["region_uid"])
        if prev is None or row["seq_added"] > prev["seq_added"]:
            out[row["region_uid"]] = row
    return out


def region_value(row: Optional[sqlite3.Row]) -> tuple:
    if row is None:
        return (None, ())
    return (row["kind"], tuple(json.loads(row["fragments_json"])))


# ---------------------------------------------------------------------------
# 引用判定
# ---------------------------------------------------------------------------

def relation_state_row(
    conn: sqlite3.Connection, doc_id: str, rel_type: str,
    from_fragment: str, to_fragment: str, seq: int,
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM relation_state WHERE doc_id=? AND rel_type=?"
        " AND from_fragment=? AND to_fragment=? AND " + _LIVE +
        " ORDER BY seq_added DESC LIMIT 1",
        (doc_id, rel_type, from_fragment, to_fragment, seq, seq),
    ).fetchone()


def relation_states_at(
    conn: sqlite3.Connection, seq: int, doc_id: Optional[str] = None
) -> dict[str, sqlite3.Row]:
    sql = "SELECT * FROM relation_state WHERE " + _LIVE
    params: list = [seq, seq]
    if doc_id is not None:
        sql += " AND doc_id=?"
        params.append(doc_id)
    out: dict[str, sqlite3.Row] = {}
    for row in conn.execute(sql, params):
        key = canonical_json(
            [row["doc_id"], row["rel_type"], row["from_fragment"],
             row["to_fragment"]]
        )
        prev = out.get(key)
        if prev is None or row["seq_added"] > prev["seq_added"]:
            out[key] = row
    return out


# ---------------------------------------------------------------------------
# 阻断办结
# ---------------------------------------------------------------------------

def blocked_state_row(conn: sqlite3.Connection, record_uid: str,
                      seq: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM blocked_state WHERE record_uid=? AND " + _LIVE +
        " ORDER BY seq_added DESC LIMIT 1",
        (record_uid, seq, seq),
    ).fetchone()


def blocked_states_at(
    conn: sqlite3.Connection, seq: int, doc_id: Optional[str] = None
) -> dict[str, sqlite3.Row]:
    sql = "SELECT * FROM blocked_state WHERE " + _LIVE
    params: list = [seq, seq]
    if doc_id is not None:
        sql += " AND record_uid IN (SELECT record_uid FROM base_blocked"
        sql += " WHERE doc_id=?)"
        params.append(doc_id)
    out: dict[str, sqlite3.Row] = {}
    for row in conn.execute(sql, params):
        prev = out.get(row["record_uid"])
        if prev is None or row["seq_added"] > prev["seq_added"]:
            out[row["record_uid"]] = row
    return out


# ---------------------------------------------------------------------------
# 版本内的有效性与待办
# ---------------------------------------------------------------------------

def present_relations_at(
    conn: sqlite3.Connection, seq: int, doc_id: str
) -> dict[str, sqlite3.Row]:
    return {
        key: row
        for key, row in relation_states_at(conn, seq, doc_id).items()
        if row["kind"] == REL_PRESENT
    }


def invalid_present_relations(
    conn: sqlite3.Connection, seq: int, doc_id: str
) -> list[dict]:
    """审定应存在的引用，若端点不是该版本的有效片段，即为无效关联。"""
    live = fragments_at(conn, seq, doc_id)
    bad = []
    for key, row in present_relations_at(conn, seq, doc_id).items():
        missing = [
            side for side, uid in (
                ("from", row["from_fragment"]), ("to", row["to_fragment"])
            )
            if uid not in live
        ]
        if missing:
            bad.append({
                "subject_kind": "relation",
                "subject_key": key,
                "doc_id": row["doc_id"],
                "rel_type": row["rel_type"],
                "from_fragment": row["from_fragment"],
                "to_fragment": row["to_fragment"],
                "missing_endpoints": missing,
            })
    return bad


def invalid_adopted_blocks(
    conn: sqlite3.Connection, seq: int, doc_id: str
) -> list[dict]:
    live = fragments_at(conn, seq, doc_id)
    bad = []
    for r in conn.execute(
        "SELECT b.*, bb.rel_type AS btype, bb.from_region AS bfrom,"
        " bb.to_region AS bto FROM blocked_state b"
        " JOIN base_blocked bb ON bb.record_uid = b.record_uid"
        " WHERE b.disposition='adopted' AND bb.doc_id=? AND " + _LIVE,
        (doc_id, seq, seq),
    ):
        missing = [
            side for side, uid in (
                ("from", r["from_fragment"]), ("to", r["to_fragment"])
            )
            if uid not in live
        ]
        if missing:
            bad.append({
                "subject_kind": "blocked",
                "subject_key": r["record_uid"],
                "doc_id": doc_id,
                "missing_endpoints": missing,
            })
    return bad


def pending_regions(
    conn: sqlite3.Connection, seq: int, doc_ids: Iterable[str]
) -> list[dict]:
    doc_ids = list(doc_ids)
    if not doc_ids:
        return []
    marks = ",".join("?" for _ in doc_ids)
    out = []
    for row in conn.execute(
        "SELECT rs.* FROM region_state rs WHERE " + _LIVE +
        " AND rs.kind='pending' AND rs.doc_id IN (" + marks + ")",
        [seq, seq] + doc_ids,
    ):
        out.append({
            "item_kind": "region",
            "subject_key": row["region_uid"],
            "doc_id": row["doc_id"],
            "layer": row["layer"],
            "reason": "region_pending",
        })
    return out


def pending_relations(
    conn: sqlite3.Connection, seq: int, doc_ids: Iterable[str]
) -> list[dict]:
    doc_ids = list(doc_ids)
    if not doc_ids:
        return []
    marks = ",".join("?" for _ in doc_ids)
    out = []
    for row in conn.execute(
        "SELECT rs.* FROM relation_state rs WHERE " + _LIVE +
        " AND rs.kind='pending' AND rs.doc_id IN (" + marks + ")",
        [seq, seq] + doc_ids,
    ):
        out.append({
            "item_kind": "relation",
            "subject_key": canonical_json([
                row["doc_id"], row["rel_type"], row["from_fragment"],
                row["to_fragment"],
            ]),
            "doc_id": row["doc_id"],
            "rel_type": row["rel_type"],
            "reason": "relation_pending",
        })
    return out


def pending_blocked(
    conn: sqlite3.Connection, seq: int, doc_ids: Iterable[str]
) -> list[dict]:
    """被阻断原始关系未办结：只算底稿里确实被阻断、且尚无办结行的记录。"""
    doc_ids = list(doc_ids)
    if not doc_ids:
        return []
    marks = ",".join("?" for _ in doc_ids)
    sql = (
        "SELECT bb.record_uid, bb.doc_id, bb.rel_type FROM base_blocked bb"
        " WHERE bb.doc_id IN (" + marks + ")"
        " AND NOT EXISTS (SELECT 1 FROM blocked_state b"
        "  WHERE b.record_uid = bb.record_uid"
        "  AND b.seq_added <= ? AND b.seq_removed IS NULL)"
    )
    params = list(doc_ids) + [seq]
    return [
        {
            "item_kind": "blocked",
            "subject_key": r["record_uid"],
            "doc_id": r["doc_id"],
            "rel_type": r["rel_type"],
            "reason": "blocked_relation_open",
        }
        for r in conn.execute(sql, params)
    ]


def publish_blockers(
    conn: sqlite3.Connection, seq: int, doc_ids: Iterable[str]
) -> list[dict]:
    """发布阻挡项：待定区域、待定引用、未办结阻断、无效关联。

    只检查本次点名的文档；其他文档的待办不阻挡本批。
    """
    doc_ids = list(doc_ids)
    out: list[dict] = []
    out.extend(pending_regions(conn, seq, doc_ids))
    out.extend(pending_relations(conn, seq, doc_ids))
    out.extend(pending_blocked(conn, seq, doc_ids))
    for doc_id in doc_ids:
        for bad in invalid_present_relations(conn, seq, doc_id):
            out.append({**bad, "reason": "invalid_relation"})
        for bad in invalid_adopted_blocks(conn, seq, doc_id):
            out.append({**bad, "reason": "invalid_adopted_block"})
        out.extend(_invalid_region_fragments(conn, seq, doc_id))
    return out


def _invalid_region_fragments(conn, seq, doc_id) -> list[dict]:
    live = fragments_at(conn, seq, doc_id)
    out = []
    for row in region_states_at(conn, seq, doc_id).values():
        if row["kind"] != REGION_FRAGMENTS:
            continue
        missing = [u for u in json.loads(row["fragments_json"]) if u not in live]
        if missing:
            out.append({
                "item_kind": "region",
                "subject_key": row["region_uid"],
                "doc_id": doc_id,
                "reason": "region_fragment_missing",
                "missing": missing,
            })
    return out


def relation_dependencies(
    conn: sqlite3.Connection, seq: int, doc_ids: Iterable[str],
    live_fragments: set,
) -> Iterator[dict]:
    """逐条产出「端点已不在合批后集合」的 present 引用（服务端游标）。"""
    doc_ids = list(doc_ids)
    if not doc_ids:
        return
    marks = ",".join("?" for _ in doc_ids)
    cur = conn.execute(
        "SELECT rs.doc_id, rs.rel_type, rs.from_fragment, rs.to_fragment"
        " FROM relation_state rs WHERE " + _LIVE +
        " AND rs.kind='present' AND rs.doc_id IN (" + marks + ")",
        [seq, seq] + doc_ids,
    )
    for row in cur:
        if row["from_fragment"] in live_fragments and row["to_fragment"] in live_fragments:
            continue
        yield {
            "doc_id": row["doc_id"],
            "rel_type": row["rel_type"],
            "from_fragment": row["from_fragment"],
            "to_fragment": row["to_fragment"],
        }


def evidence_for(
    conn: sqlite3.Connection, subject_kind: str, subject_key: str,
    seq: int,
) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM state_evidence WHERE subject_kind=? AND subject_key=?"
        " AND seq <= ? ORDER BY seq, source_uid",
        (subject_kind, subject_key, seq),
    )
    return [
        {
            "seq": r["seq"],
            "ticket_uid": r["ticket_uid"],
            "reviewer_id": r["reviewer_id"],
            "reason": r["reason"],
            "evidence": json.loads(r["evidence_json"]),
            "rev_id": r["rev_id"],
        }
        for r in rows
    ]


def require_revision(conn: sqlite3.Connection, seq: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM revision WHERE seq=?", (seq,)).fetchone()
    if row is None:
        raise WsError("REVISION_NOT_FOUND", f"no revision at seq {seq}")
    return row
