# -*- coding: utf-8 -*-
"""按版本查看与证据展开。"""
from __future__ import annotations

import json
from typing import Optional

from .ws_common import WsError, relation_key, resolve_revision
from .ws_state import (
    blocked_state_row,
    blocked_states_at,
    evidence_for,
    fragments_at,
    invalid_adopted_blocks,
    invalid_present_relations,
    pending_blocked,
    pending_regions,
    pending_relations,
    region_state_row,
    region_states_at,
    relation_state_row,
    relation_states_at,
)


def _region_desc(conn, row) -> dict:
    return {
        "region_uid": row["region_uid"],
        "doc_id": row["doc_id"],
        "layer": row["layer"],
        "kind": row["kind"],
        "fragments": json.loads(row["fragments_json"]),
        "seq_added": row["seq_added"],
        "related_rev": row["related_rev"],
    }


def _fragment_desc(row) -> dict:
    return {
        "fragment_uid": row["fragment_uid"],
        "doc_id": row["doc_id"],
        "layer": row["layer"],
        "label": row["label"],
        "start": row["start_byte"],
        "end": row["end_byte"],
        "text_sha256": row["text_sha256"],
        "seen_text": row["seen_text"],
        "origin": row["origin"],
        "region_uid": row["region_uid"],
        "related_rev": row["related_rev"],
    }


def _relation_desc(row) -> dict:
    return {
        "doc_id": row["doc_id"],
        "rel_type": row["rel_type"],
        "from_fragment": row["from_fragment"],
        "to_fragment": row["to_fragment"],
        "kind": row["kind"],
        "origin": row["origin"],
        "related_rev": row["related_rev"],
    }


def view_revision(conn, rev: str, *, doc_id: Optional[str] = None,
                  layer: Optional[str] = None) -> dict:
    revision = resolve_revision(conn, rev)
    seq = revision["seq"]
    doc_ids = [doc_id] if doc_id else [
        r["doc_id"] for r in conn.execute(
            "SELECT doc_id FROM source_doc ORDER BY doc_id")]

    out = {
        "rev_id": revision["rev_id"],
        "rev_seq": seq,
        "parent_rev": revision["parent_rev"],
        "base_rev": revision["base_rev"],
        "ticket_uid": revision["ticket_uid"],
        "documents": {},
    }
    all_pdr = []
    all_pr = []
    all_pb = []
    for d in doc_ids:
        regions = [
            _region_desc(conn, row)
            for row in region_states_at(conn, seq, d).values()
            if layer is None or row["layer"] == layer
        ]
        regions.sort(key=lambda x: (x["layer"], x["region_uid"]))
        frag_rows = fragments_at(conn, seq, d, layer)
        fragments = sorted(
            (_fragment_desc(r) for r in frag_rows.values()),
            key=lambda x: (x["layer"], x["start"], x["end"], x["label"]))
        relation_rows = relation_states_at(conn, seq, d)
        relations = sorted(
            (_relation_desc(r) for r in relation_rows.values()),
            key=lambda x: (x["rel_type"], x["from_fragment"], x["to_fragment"]))
        blocked = []
        for record_uid, row in sorted(blocked_states_at(conn, seq, d).items()):
            blocked.append({
                "record_uid": record_uid,
                "disposition": row["disposition"],
                "rel_type": row["rel_type"],
                "from_fragment": row["from_fragment"],
                "to_fragment": row["to_fragment"],
                "related_rev": row["related_rev"],
            })
        out["documents"][d] = {
            "regions": regions,
            "fragments": fragments,
            "relations": relations,
            "blocked_closed": blocked,
            "invalid_relations": invalid_present_relations(conn, seq, d),
            "invalid_adopted_blocks": invalid_adopted_blocks(conn, seq, d),
        }
        all_pdr.extend(pending_regions(conn, seq, [d]))
        all_pr.extend(pending_relations(conn, seq, [d]))
        all_pb.extend(pending_blocked(conn, seq, [d]))
    out["pending"] = {
        "regions": all_pdr,
        "relations": all_pr,
        "blocked": all_pb,
    }
    return out


def show_region(conn, rev: str, region_uid: str) -> dict:
    revision = resolve_revision(conn, rev)
    seq = revision["seq"]
    row = region_state_row(conn, region_uid, seq)
    if row is None:
        raise WsError("REGION_NOT_FOUND", f"region {region_uid} not at {rev}")
    base = conn.execute(
        "SELECT * FROM base_region WHERE region_uid=?", (region_uid,)
    ).fetchone()
    base_candidates = [
        dict(r) for r in conn.execute(
            "SELECT * FROM base_region_candidate WHERE region_uid=?"
            " ORDER BY is_no_fragment, start_byte, end_byte, label",
            (region_uid,))
    ]
    members = [
        _fragment_desc(conn.execute(
            "SELECT * FROM fragment_state WHERE fragment_uid=?"
            " AND seq_added<=? AND (seq_removed IS NULL OR seq_removed>?)"
            " ORDER BY seq_added DESC LIMIT 1", (uid, seq, seq)).fetchone())
        for uid in json.loads(row["fragments_json"])
    ]
    base_reviews = [
        dict(r) for r in conn.execute(
            "SELECT * FROM base_review_item WHERE region_uid=? ORDER BY review_uid",
            (region_uid,))
    ]
    return {
        "rev_id": revision["rev_id"],
        "rev_seq": seq,
        "region": _region_desc(conn, row),
        "envelope": [base["start_byte"], base["end_byte"]] if base else None,
        "base_outcome": {
            "kind": base["outcome_kind"],
            "winner_candidate": base["winner_candidate"],
            "denominator": base["denominator"],
            "strict_majority": base["strict_majority"],
        } if base else None,
        "base_candidates": base_candidates,
        "current_fragments": members,
        "evidence": evidence_for(conn, "region", region_uid, seq),
        "base_review_items": base_reviews,
    }


def show_relation(conn, rev: str, group_or_key: str) -> dict:
    revision = resolve_revision(conn, rev)
    seq = revision["seq"]
    # 兼容关系组 UID：先用底稿组表定位有序端点
    group = conn.execute(
        "SELECT * FROM base_relation_group WHERE rel_group_uid=?",
        (group_or_key,)).fetchone()
    if group is not None:
        key = relation_key(group["doc_id"], group["rel_type"],
                           group["from_fragment"], group["to_fragment"])
    else:
        key = group_or_key
    doc_id, rel_type, frm, to = json.loads(key)
    row = relation_state_row(conn, doc_id, rel_type, frm, to, seq)
    votes = []
    if group is not None:
        votes = [dict(r) for r in conn.execute(
            "SELECT * FROM base_relation_group_vote WHERE rel_group_uid=?"
            " ORDER BY package_uid", (group["rel_group_uid"],))]
    return {
        "rev_id": revision["rev_id"],
        "rev_seq": seq,
        "relation_key": key,
        "current": _relation_desc(row) if row else None,
        "base_group": dict(group) if group else None,
        "base_votes": votes,
        "evidence": evidence_for(conn, "relation", key, seq),
    }


def show_record(conn, rev: str, record_uid: str) -> dict:
    revision = resolve_revision(conn, rev)
    seq = revision["seq"]
    base = conn.execute(
        "SELECT * FROM base_blocked WHERE record_uid=?", (record_uid,)
    ).fetchone()
    if base is None:
        raise WsError("BLOCKED_NOT_FOUND", f"blocked record {record_uid}")
    state = blocked_state_row(conn, record_uid, seq)
    return {
        "rev_id": revision["rev_id"],
        "rev_seq": seq,
        "base_blocked": dict(base),
        "payload": json.loads(base["payload_json"]) if base["payload_json"] else None,
        "current_disposition": dict(state) if state else None,
        "evidence": evidence_for(conn, "blocked", record_uid, seq),
    }


def show_release(conn, release_id: str) -> dict:
    from .ws_release import get_release

    info = get_release(conn, release_id)
    fragments = [dict(r) for r in conn.execute(
        "SELECT * FROM release_fragment WHERE release_id=?"
        " ORDER BY doc_id, layer, start_byte", (release_id,))]
    relations = [dict(r) for r in conn.execute(
        "SELECT * FROM release_relation WHERE release_id=?"
        " ORDER BY doc_id, rel_type, from_fragment", (release_id,))]
    blocked = [dict(r) for r in conn.execute(
        "SELECT * FROM release_blocked WHERE release_id=?"
        " ORDER BY record_uid", (release_id,))]
    return {**info, "fragments": fragments, "relations": relations,
            "blocked": blocked}
