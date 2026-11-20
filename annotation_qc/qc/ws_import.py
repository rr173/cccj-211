# -*- coding: utf-8 -*-
"""工单导入：稳定身份、基版绑定、幂等重入与基版自洽预检。

导入只登记工单并相对其基版校验（提案先在自己的基版上成立），
不修改任何审定稿；合并在 ``ws_merge`` 中相对当前审定稿进行。
"""
from __future__ import annotations

from typing import Optional

from .util import canonical_json
from .ws_common import (
    WsError,
    resolve_revision,
    ticket_uid as make_ticket_uid,
    transaction,
    utc_now,
)
from .ws_propose import ProposalBuilder
from .ws_ticket import parse_ticket

TOPIC_REGION = "region"
TOPIC_RELATION = "relation"
TOPIC_BLOCKED = "blocked"
TOPIC_MIXED = "mixed"


def _classify(parsed) -> str:
    kinds = {
        TOPIC_REGION if parsed.regions else None,
        TOPIC_RELATION if parsed.relations else None,
        TOPIC_BLOCKED if parsed.blocked else None,
    } - {None}
    if len(kinds) == 1:
        return next(iter(kinds))
    return TOPIC_MIXED


def import_ticket(conn, sources, data: dict, *,
                  base_hint: Optional[str] = None,
                  dry_run: bool = False) -> dict:
    parsed = parse_ticket(data)
    hint = parsed.base_hint or base_hint or "HEAD"
    base_rev = resolve_revision(conn, hint)

    uid = make_ticket_uid(parsed.reviewer, base_rev["rev_id"], parsed.content)
    existing = conn.execute(
        "SELECT ticket_uid FROM ticket WHERE ticket_uid=?", (uid,)
    ).fetchone()
    if existing is not None:
        return {"ticket_uid": uid, "base_rev": base_rev["rev_id"],
                "duplicate": True, "validated": True}

    builder = ProposalBuilder(
        conn, sources, uid, parsed.reviewer, parsed.title, base_rev["seq"],
        base_rev["rev_id"], parsed)
    try:
        proposal = builder.build()
    except WsError as exc:
        raise WsError(exc.code, exc.message, {"phase": "import", **exc.detail})

    if dry_run:
        return {"ticket_uid": uid, "base_rev": base_rev["rev_id"],
                "validated": True, "proposal": proposal.to_dict()}

    with transaction(conn):
        seq = conn.execute(
            "SELECT COALESCE(MAX(import_seq), 0) + 1 FROM ticket"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO ticket(ticket_uid, reviewer_id, base_rev, topic,"
            " title, content_json, created_at, import_seq)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                uid, parsed.reviewer, base_rev["rev_id"], _classify(parsed),
                parsed.title, canonical_json(parsed.content), utc_now(), seq,
            ),
        )
        _store_fragments(conn, uid, proposal)
        conn.executemany(
            "INSERT INTO ticket_note(ticket_uid, subject_kind, subject_key,"
            " reason, evidence_json) VALUES (?,?,?,?,?)",
            [
                (uid, kind, key, note["reason"],
                 canonical_json(note["evidence"]))
                for (kind, key), note in sorted(proposal.notes.items())
            ],
        )
    return {"ticket_uid": uid, "base_rev": base_rev["rev_id"],
            "duplicate": False, "validated": True}


def _store_fragments(conn, uid: str, proposal) -> None:
    # 局部名 -> 身份；同一身份可能被多个局部名指向，全部登记以便回查
    rows = []
    for ref, fragment_uid in sorted(proposal.ref_map.items()):
        info = proposal.fragments[fragment_uid]
        rows.append((
            uid, ref, fragment_uid, info["doc_id"], info["layer"],
            info["label"], info["start"], info["end"], info["text_sha256"],
            info["seen_text"],
        ))
    conn.executemany(
        "INSERT INTO ticket_fragment(ticket_uid, local_id, fragment_uid,"
        " doc_id, layer, label, start_byte, end_byte, text_sha256, seen_text)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        rows,
    )


def ensure_ticket_imported(conn, sources, data: dict, *,
                           base_hint: Optional[str] = None) -> dict:
    return import_ticket(conn, sources, data, base_hint=base_hint)


def ticket_identity(conn, data: dict,
                    base_hint: Optional[str] = None) -> str:
    """工单稳定身份：基版在导入时固定（工单自述的 base 优先于 --base）。"""
    parsed = parse_ticket(data)
    hint = parsed.base_hint or base_hint or "HEAD"
    base_rev = resolve_revision(conn, hint)
    return make_ticket_uid(parsed.reviewer, base_rev["rev_id"], parsed.content)


def resolve_ticket_or_data(conn, sources, *, ticket_uid: Optional[str] = None,
                           data: Optional[dict] = None,
                           base_hint: Optional[str] = None):
    """合批入口：已知工单身份则直接读取；否则导入/复用后返回行。"""
    if ticket_uid:
        row = conn.execute(
            "SELECT * FROM ticket WHERE ticket_uid=?", (ticket_uid,)
        ).fetchone()
        if row is None:
            raise WsError("TICKET_NOT_FOUND", f"unknown ticket {ticket_uid}")
        return row
    if data is None:
        raise WsError("TICKET_REQUIRED", "provide a ticket file or ticket uid")
    # 已存在同一工单（同一复核员、同一基版、同一内容）时直接复用，
    # 不再因当前 HEAD 已前移而重新导入。
    uid = ticket_identity(conn, data, base_hint)
    row = conn.execute(
        "SELECT * FROM ticket WHERE ticket_uid=?", (uid,)
    ).fetchone()
    if row is not None:
        return row
    result = import_ticket(conn, sources, data, base_hint=base_hint)
    row = conn.execute(
        "SELECT * FROM ticket WHERE ticket_uid=?", (result["ticket_uid"],)
    ).fetchone()
    return row
