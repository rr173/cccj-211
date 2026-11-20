# -*- coding: utf-8 -*-
"""审定发布：点名审定版本与一组公文，登记后固化交付快照。

- 只要所选文档还有待定区域、待定引用、未办结的阻断事项或无效关联，
  就列出挡住发布的事项，不登记发布；其他文档的待办不阻挡本批；
- 登记成功的发布身份固定那一版与公文集合，下游可据此流式取
  审定片段与存在的有向引用；
- 每条交付记录都能回查是自动承接还是人工裁决，以及版本、工单理由与证据；
- 否定、退役和未采纳的记录留在审定历史，不进入交付表；
- 发布是只读快照：后续合批/退役/新批次不改变已登记发布的内容。
"""
from __future__ import annotations

import json
from typing import Iterator, Optional

from .util import canonical_json
from .ws_common import (
    WsError,
    exclusive,
    meta_get,
    meta_set,
    resolve_revision,
    stable_id,
    transaction,
    utc_now,
)
from .ws_state import (
    BLOCK_ADOPTED,
    REL_PRESENT,
    blocked_states_at,
    evidence_for,
    fragments_at,
    publish_blockers,
    relation_states_at,
)


def release(conn, workspace: str, *, rev: str, doc_ids: list[str]) -> dict:
    doc_ids = sorted(set(doc_ids))
    if not doc_ids:
        raise WsError("NO_DOCUMENTS", "release needs at least one document")
    with exclusive(workspace):
        with transaction(conn):
            return _release_locked(conn, rev, doc_ids)


def blockers(conn, *, rev: str = "HEAD",
             doc_ids: Optional[list[str]] = None) -> dict:
    revision = resolve_revision(conn, rev)
    if doc_ids is None:
        doc_ids = [
            r["doc_id"] for r in conn.execute(
                "SELECT doc_id FROM source_doc ORDER BY doc_id")
        ]
    doc_ids = sorted(set(doc_ids))
    _check_docs(conn, doc_ids)
    items = publish_blockers(conn, revision["seq"], doc_ids)
    return {
        "rev_id": revision["rev_id"],
        "rev_seq": revision["seq"],
        "doc_ids": doc_ids,
        "blocked": bool(items),
        "items": items,
    }


def _check_docs(conn, doc_ids) -> None:
    for doc_id in doc_ids:
        row = conn.execute(
            "SELECT 1 FROM source_doc WHERE doc_id=?", (doc_id,)
        ).fetchone()
        if row is None:
            raise WsError("UNKNOWN_DOC", f"document not in workspace: {doc_id}")


def _release_locked(conn, rev: str, doc_ids: list[str]) -> dict:
    revision = resolve_revision(conn, rev)
    _check_docs(conn, doc_ids)
    items = publish_blockers(conn, revision["seq"], doc_ids)
    if items:
        return {
            "released": False,
            "rev_id": revision["rev_id"],
            "rev_seq": revision["seq"],
            "doc_ids": doc_ids,
            "blockers": items,
        }

    n = int(meta_get(conn, "release_seq") or "0") + 1
    meta_set(conn, "release_seq", str(n))
    release_id = stable_id(
        "release", revision["rev_id"], doc_ids, n)
    seq = revision["seq"]
    ts = utc_now()
    conn.execute(
        "INSERT INTO release(release_id, rev_id, rev_seq, created_at,"
        " doc_ids_json, request_json) VALUES (?,?,?,?,?,?)",
        (release_id, revision["rev_id"], seq, ts, canonical_json(doc_ids),
         canonical_json({"rev": rev, "doc_ids": doc_ids})),
    )

    fragments = []
    relations = []
    for doc_id in doc_ids:
        fragments.extend(_fragment_rows(conn, seq, doc_id))
        relations.extend(_relation_rows(conn, seq, doc_id))
    conn.executemany(
        "INSERT INTO release_fragment(release_id, fragment_uid, doc_id, layer,"
        " label, start_byte, end_byte, text_sha256, seen_text, origin,"
        " source_kind, source_rev, ticket_uid, reviewer_id, reason,"
        " evidence_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(release_id, *row) for row in fragments],
    )
    conn.executemany(
        "INSERT INTO release_relation(release_id, doc_id, rel_type,"
        " from_fragment, to_fragment, source_kind, source_rev, ticket_uid,"
        " reviewer_id, reason, evidence_json)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [(release_id, *row) for row in relations],
    )
    conn.executemany(
        "INSERT INTO release_blocked(release_id, record_uid, disposition,"
        " rel_type, from_fragment, to_fragment, source_kind, source_rev,"
        " ticket_uid, reviewer_id, reason, evidence_json)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(release_id, *row) for row in _blocked_rows(conn, seq, doc_ids)],
    )
    return {
        "released": True,
        "release_id": release_id,
        "rev_id": revision["rev_id"],
        "rev_seq": seq,
        "doc_ids": doc_ids,
        "fragment_count": len(fragments),
        "relation_count": len(relations),
        "release_seq": n,
    }


# ---------------------------------------------------------------------------
# 交付快照（只含审定片段与存在的有向引用）
# ---------------------------------------------------------------------------

def _revision_provenance(conn, rev_id: Optional[str]) -> dict:
    if rev_id is None:
        return {"ticket_uid": None, "reviewer_id": None}
    row = conn.execute(
        "SELECT ticket_uid FROM revision WHERE rev_id=?", (rev_id,)
    ).fetchone()
    if row is None:
        return {"ticket_uid": None, "reviewer_id": None}
    ticket_uid = row["ticket_uid"]
    reviewer = None
    if ticket_uid:
        t = conn.execute(
            "SELECT reviewer_id FROM ticket WHERE ticket_uid=?", (ticket_uid,)
        ).fetchone()
        reviewer = t["reviewer_id"] if t else None
    return {"ticket_uid": ticket_uid, "reviewer_id": reviewer}


def _note_for(conn, kind: str, key: str, seq: int) -> dict:
    rows = evidence_for(conn, kind, key, seq)
    if not rows:
        return {"reason": None, "evidence_json": canonical_json([]),
                "ticket_uid": None, "reviewer_id": None,
                "level": "auto"}
    manual = [r for r in rows if r["ticket_uid"] is not None]
    chosen = (manual or rows)[-1]
    return {
        "reason": chosen["reason"],
        "evidence_json": canonical_json(chosen["evidence"]),
        "ticket_uid": chosen["ticket_uid"],
        "reviewer_id": chosen["reviewer_id"],
        "level": "manual" if chosen["ticket_uid"] else "auto",
    }


def _fragment_rows(conn, seq: int, doc_id: str) -> list:
    out = []
    for uid, row in sorted(fragments_at(conn, seq, doc_id).items()):
        prov = _revision_provenance(conn, row["related_rev"])
        note = _note_for(conn, "region", row["region_uid"], seq) \
            if row["region_uid"] else _note_for(conn, "fragment", uid, seq)
        # 出处按「令其成为审定片段的版本」判定：区域由人工裁决 -> manual，
        # 自动承接 -> auto（即使之后有人再次确认，也不改写出处）。
        if row["origin"] == "auto" and note["level"] == "auto":
            source_kind = "auto"
            ticket_uid = None
            reviewer = None
            reason = note["reason"] or "adopted by automatic QC vote tally"
            evidence = (note["evidence_json"]
                        if note["reason"] else canonical_json({
                            "region_uid": row["region_uid"],
                            "fragment_uid": uid,
                            "adopted_at": row["related_rev"],
                        }))
        else:
            source_kind = "manual"
            ticket_uid = note["ticket_uid"] or prov["ticket_uid"]
            reviewer = note["reviewer_id"] or prov["reviewer_id"]
            reason = note["reason"]
            evidence = note["evidence_json"]
        out.append((
            uid, doc_id, row["layer"], row["label"], row["start_byte"],
            row["end_byte"], row["text_sha256"], row["seen_text"],
            row["origin"], source_kind, row["related_rev"], ticket_uid,
            reviewer, reason, evidence,
        ))
    return out


def _relation_rows(conn, seq: int, doc_id: str) -> list:
    out = []
    for key, row in sorted(relation_states_at(conn, seq, doc_id).items()):
        if row["kind"] != REL_PRESENT:
            continue
        note = _note_for(conn, "relation", key, seq)
        prov = _revision_provenance(conn, row["related_rev"])
        source_kind = "auto" if row["origin"] == "auto" else "manual"
        ticket_uid = note["ticket_uid"] or prov["ticket_uid"]
        reviewer = note["reviewer_id"] or prov["reviewer_id"]
        out.append((
            doc_id, row["rel_type"], row["from_fragment"], row["to_fragment"],
            source_kind, row["related_rev"], ticket_uid, reviewer,
            note["reason"], note["evidence_json"],
        ))
    return out


def _blocked_rows(conn, seq: int, doc_ids: list) -> list:
    out = []
    for doc_id in doc_ids:
        for record_uid, row in sorted(
            blocked_states_at(conn, seq, doc_id).items()
        ):
            if row["disposition"] != BLOCK_ADOPTED:
                continue
            note = _note_for(conn, "blocked", record_uid, seq)
            prov = _revision_provenance(conn, row["related_rev"])
            out.append((
                record_uid, row["disposition"], row["rel_type"],
                row["from_fragment"], row["to_fragment"], "manual",
                row["related_rev"], note["ticket_uid"] or prov["ticket_uid"],
                note["reviewer_id"] or prov["reviewer_id"], note["reason"],
                note["evidence_json"],
            ))
    return out


# ---------------------------------------------------------------------------
# 读取发布（流式）
# ---------------------------------------------------------------------------

def get_release(conn, release_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM release WHERE release_id=?", (release_id,)
    ).fetchone()
    if row is None:
        raise WsError("RELEASE_NOT_FOUND", f"unknown release {release_id}")
    return {
        "release_id": row["release_id"],
        "rev_id": row["rev_id"],
        "rev_seq": row["rev_seq"],
        "created_at": row["created_at"],
        "doc_ids": json.loads(row["doc_ids_json"]),
        "fragment_count": conn.execute(
            "SELECT COUNT(*) c FROM release_fragment WHERE release_id=?",
            (release_id,)).fetchone()["c"],
        "relation_count": conn.execute(
            "SELECT COUNT(*) c FROM release_relation WHERE release_id=?",
            (release_id,)).fetchone()["c"],
    }


def list_releases(conn) -> list[dict]:
    return [
        {**get_release(conn, r["release_id"])}
        for r in conn.execute(
            "SELECT release_id FROM release ORDER BY rev_seq, created_at")
    ]


def stream_release(conn, release_id: str, *, what: str,
                   doc_id: Optional[str] = None) -> Iterator[str]:
    """服务端游标逐行产出审定片段 / 存在的有向引用。"""
    if what == "fragments":
        sql = (
            "SELECT fragment_uid, doc_id, layer, label, start_byte, end_byte,"
            " text_sha256, seen_text, origin, source_kind, source_rev,"
            " ticket_uid, reviewer_id FROM release_fragment"
            " WHERE release_id=?"
        )
        params = [release_id]
        if doc_id is not None:
            sql += " AND doc_id=?"
            params.append(doc_id)
        sql += " ORDER BY doc_id, layer, start_byte, end_byte"
        for r in conn.execute(sql, params):
            yield json.dumps(dict(r), ensure_ascii=False, sort_keys=True) + "\n"
        return
    if what == "relations":
        sql = (
            "SELECT doc_id, rel_type, from_fragment, to_fragment, source_kind,"
            " source_rev, ticket_uid, reviewer_id FROM release_relation"
            " WHERE release_id=?"
        )
        params = [release_id]
        if doc_id is not None:
            sql += " AND doc_id=?"
            params.append(doc_id)
        sql += " ORDER BY doc_id, rel_type, from_fragment, to_fragment"
        for r in conn.execute(sql, params):
            yield json.dumps(dict(r), ensure_ascii=False, sort_keys=True) + "\n"
        return
    if what == "provenance":
        sql = (
            "SELECT 'fragment' AS record_kind, fragment_uid AS record_uid,"
            " doc_id, layer AS detail, origin AS source_kind, source_rev,"
            " ticket_uid, reviewer_id, reason FROM release_fragment"
            " WHERE release_id=?"
            " UNION ALL"
            " SELECT 'relation', from_fragment || '->' || to_fragment, doc_id,"
            " rel_type, source_kind, source_rev, ticket_uid, reviewer_id,"
            " reason FROM release_relation WHERE release_id=?"
            " ORDER BY record_kind, doc_id"
        )
        params = [release_id, release_id]
        for r in conn.execute(sql, params):
            yield json.dumps(dict(r), ensure_ascii=False, sort_keys=True) + "\n"
        return
    raise WsError("BAD_EXPORT", f"unknown release export target: {what}")
