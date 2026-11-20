# -*- coding: utf-8 -*-
"""合批：把一份已导入工单相对当前审定稿三方合并，生成新版本。

三方比较：工单基版（base）、当前审定稿（current）、工单提案（proposal）。

- 只一边相对基版变更 → 取该变更；
- 两边给出相同业务值 → 合并，双方理由与证据随版本留存；
- 两边给出不同业务值 → 冲突，整份工单不生效；
- 未改过的事项不是删除。

对象键不重叠时也要做依赖检查：合批后存在的引用、已办结事项的端点必须
落在有效片段上；不能借同区另一片段顶替端点。

所有工作（含幂等回执登记）在一个 ``BEGIN IMMEDIATE`` 事务内完成，
并持有工作区文件锁；失败/中断不留下可见半版。成功后同一工单重试
返回同一版本（幂等回执）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

from .util import canonical_json, stable_id
from .ws_common import (
    exclusive,
    relation_key,
    resolve_revision,
    revision_id,
    transaction,
    utc_now,
)
from .ws_propose import ProposalBuilder, base_region_values
from .ws_state import (
    BLOCK_ADOPTED,
    BLOCK_REJECTED,
    REGION_FRAGMENTS,
    REL_ABSENT,
    REL_PENDING,
    REL_PRESENT,
    blocked_state_row,
    blocked_states_at,
    fragment_row,
    fragments_at,
    region_state_row,
    relation_state_row,
    relation_states_at,
)
from .ws_ticket import parse_ticket, ticket_content_sha

MERGED = "merged"
CONFLICT = "conflict"
REOPEN = "reopen"
RESTORE_BASE = "restore_base"
# 「退回待办」是一个显式的业务值，必须区别于「基版没有该事项」（None），
# 否则三方比较会把 reopen 误判为未改动。
_REOPENED = {"disposition": "reopened", "from_fragment": None,
             "to_fragment": None}


@dataclass
class Conflict:
    kind: str                   # region | relation | blocked | endpoint | dependency
    subject_key: str
    doc_id: str
    reason: str
    base: object = None
    current: object = None
    proposal: object = None
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "subject_key": self.subject_key,
            "doc_id": self.doc_id,
            "reason": self.reason,
            "base": _jsonable(self.base),
            "current": _jsonable(self.current),
            "proposal": _jsonable(self.proposal),
            **self.extra,
        }


def _jsonable(value):
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def merge(conn, sources, workspace: str, *, ticket_uid: Optional[str] = None,
          data: Optional[dict] = None, base_hint: Optional[str] = None) -> dict:
    """合批入口：``conn`` 为工作区库连接，``sources`` 为原文存取器。"""
    from .ws_import import resolve_ticket_or_data

    with exclusive(workspace):
        ticket_row = resolve_ticket_or_data(
            conn, sources, ticket_uid=ticket_uid, data=data,
            base_hint=base_hint)
        return _merge_locked(conn, sources, ticket_row)


def preview(conn, sources, *, ticket_uid: Optional[str] = None,
            data: Optional[dict] = None, base_hint: Optional[str] = None) -> dict:
    """按工单基版预览整批提案（不改当前审定稿、不登记批次）。"""
    from .ws_import import resolve_ticket_or_data

    ticket_row = resolve_ticket_or_data(
        conn, sources, ticket_uid=ticket_uid, data=data, base_hint=base_hint)
    parsed = parse_ticket(json.loads(ticket_row["content_json"]))
    base_rev = resolve_revision(conn, ticket_row["base_rev"])
    proposal = ProposalBuilder(
        conn, sources, ticket_row["ticket_uid"], ticket_row["reviewer_id"],
        ticket_row["title"], base_rev["seq"], base_rev["rev_id"],
        parsed).build()
    return {
        "ticket_uid": ticket_row["ticket_uid"],
        "base_rev": base_rev["rev_id"],
        "base_seq": base_rev["seq"],
        "proposal": proposal.describe(conn),
    }


def _merge_locked(conn, sources, ticket_row) -> dict:
    uid = ticket_row["ticket_uid"]
    content_sha = ticket_content_sha(json.loads(ticket_row["content_json"]))

    prior = conn.execute(
        "SELECT * FROM merge_batch WHERE ticket_uid=? AND ticket_sha256=?"
        " ORDER BY created_at DESC LIMIT 1",
        (uid, content_sha),
    ).fetchone()
    if prior is not None:
        receipt = json.loads(prior["receipt_json"])
        if prior["status"] == MERGED:
            head = resolve_revision(conn, "HEAD")
            return {**receipt, "replayed": True, "head_rev": head["rev_id"],
                    "head_seq": head["seq"]}
        return {**receipt, "replayed": True}

    parsed = parse_ticket(json.loads(ticket_row["content_json"]))
    base_rev = resolve_revision(conn, ticket_row["base_rev"])
    current_rev = resolve_revision(conn, "HEAD")
    batch_id = stable_id(
        "batch", uid, base_rev["rev_id"], current_rev["rev_id"], content_sha)

    racing = conn.execute(
        "SELECT * FROM merge_batch WHERE batch_id=?", (batch_id,)
    ).fetchone()
    if racing is not None:
        return {**json.loads(racing["receipt_json"]), "replayed": True}

    proposal = ProposalBuilder(
        conn, sources, uid, ticket_row["reviewer_id"], ticket_row["title"],
        base_rev["seq"], base_rev["rev_id"], parsed).build()

    with transaction(conn):
        # 事务内再次确认没有并发者先落结果
        again = conn.execute(
            "SELECT * FROM merge_batch WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if again is not None:
            return {**json.loads(again["receipt_json"]), "replayed": True}

        conflicts: list[Conflict] = []
        plan = _plan_merge(conn, proposal, base_rev["seq"], current_rev["seq"],
                           conflicts)
        if conflicts:
            receipt = _conflict_receipt(uid, batch_id, base_rev, current_rev,
                                        conflicts)
            _record_batch(conn, batch_id, uid, base_rev, current_rev,
                          content_sha, None, CONFLICT, receipt)
            return {**receipt, "replayed": False}

        empty = not (plan.regions or plan.relations or plan.blocked
                     or plan.new_fragments)
        if empty:
            # 两版业务值一致、提案也未改动：不生成空版本，只登记回执
            receipt = {
                "status": MERGED,
                "ticket_uid": uid,
                "batch_id": batch_id,
                "base_rev": base_rev["rev_id"],
                "current_rev": current_rev["rev_id"],
                "result_rev": current_rev["rev_id"],
                "result_seq": current_rev["seq"],
                "changes": {},
                "no_changes": True,
            }
            _record_batch(conn, batch_id, uid, base_rev, current_rev,
                          content_sha, current_rev["rev_id"], MERGED, receipt)
            return {**receipt, "replayed": False,
                    "head_rev": current_rev["rev_id"],
                    "head_seq": current_rev["seq"]}

        new_rev = _apply_plan(conn, proposal, plan, base_rev, current_rev,
                              batch_id)
        receipt = {
            "status": MERGED,
            "ticket_uid": uid,
            "batch_id": batch_id,
            "base_rev": base_rev["rev_id"],
            "current_rev": current_rev["rev_id"],
            "result_rev": new_rev["rev_id"],
            "result_seq": new_rev["seq"],
            "changes": plan.summary,
        }
        _record_batch(conn, batch_id, uid, base_rev, current_rev, content_sha,
                      new_rev["rev_id"], MERGED, receipt)
    return {**receipt, "replayed": False, "head_rev": new_rev["rev_id"],
            "head_seq": new_rev["seq"]}


def _conflict_receipt(uid, batch_id, base_rev, current_rev, conflicts) -> dict:
    return {
        "status": CONFLICT,
        "ticket_uid": uid,
        "batch_id": batch_id,
        "base_rev": base_rev["rev_id"],
        "current_rev": current_rev["rev_id"],
        "result_rev": None,
        "conflicts": [c.to_dict() for c in conflicts],
    }


def _record_batch(conn, batch_id, uid, base_rev, current_rev, content_sha,
                  result_rev, status, receipt) -> None:
    conn.execute(
        "INSERT INTO merge_batch(batch_id, ticket_uid, base_rev, current_rev,"
        " ticket_sha256, result_rev, status, conflict_json, receipt_json,"
        " created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            batch_id, uid, base_rev["rev_id"], current_rev["rev_id"],
            content_sha, result_rev, status,
            None if status == MERGED else canonical_json(receipt["conflicts"]),
            canonical_json(receipt), utc_now(),
        ),
    )


# ---------------------------------------------------------------------------
# 三方计划
# ---------------------------------------------------------------------------

@dataclass
class MergePlan:
    regions: dict = field(default_factory=dict)     # uid -> (value, side)
    relations: dict = field(default_factory=dict)   # key -> (kind, side)
    blocked: dict = field(default_factory=dict)     # record -> item+side
    new_fragments: dict = field(default_factory=dict)
    summary: dict = field(default_factory=dict)

    def bump(self, name: str) -> None:
        self.summary[name] = self.summary.get(name, 0) + 1


def _plan_merge(conn, proposal, base_seq, current_seq, conflicts) -> MergePlan:
    plan = MergePlan()
    _plan_regions(conn, proposal, base_seq, current_seq, plan, conflicts)
    _plan_relations(conn, proposal, base_seq, current_seq, plan, conflicts)
    _plan_blocked(conn, proposal, base_seq, current_seq, plan, conflicts)
    if conflicts:
        return plan
    _check_endpoint_dependencies(conn, proposal, base_seq, current_seq, plan,
                                 conflicts)
    if conflicts:
        return plan
    _check_dependency_retirement(conn, proposal, base_seq, current_seq, plan,
                                 conflicts)
    return plan


def _plan_regions(conn, proposal, base_seq, current_seq, plan, conflicts):
    docs = set()
    for region_uid in proposal.regions:
        row = conn.execute(
            "SELECT doc_id FROM base_region WHERE region_uid=?", (region_uid,)
        ).fetchone()
        docs.add(row["doc_id"])
    for doc_id in sorted(docs):
        base_vals = base_region_values(conn, base_seq, doc_id)
        cur_vals = base_region_values(conn, current_seq, doc_id)
        for region_uid in sorted(proposal.regions):
            if conn.execute(
                "SELECT 1 FROM base_region WHERE region_uid=? AND doc_id=?",
                (region_uid, doc_id),
            ).fetchone() is None:
                continue
            prop_val = proposal.regions[region_uid]
            chosen, side = _three_way(
                base_vals.get(region_uid), cur_vals.get(region_uid), prop_val,
                conflicts, "region", region_uid, doc_id)
            if chosen is None:
                continue
            plan.regions[region_uid] = (chosen, side)
            plan.bump("regions")
    for uid, info in sorted(proposal.fragments.items()):
        if fragment_row(conn, uid, current_seq) is None:
            plan.new_fragments[uid] = info


def _plan_relations(conn, proposal, base_seq, current_seq, plan, conflicts):
    for key in sorted(proposal.relations):
        doc_id, rel_type, frm, to = _key_parts(key)
        base_val = _relation_value(conn, base_seq, key)
        cur_val = _relation_value(conn, current_seq, key)
        chosen, side = _three_way(
            base_val, cur_val, proposal.relations[key], conflicts, "relation",
            key, doc_id)
        if chosen is None:
            continue
        plan.relations[key] = (chosen, side)
        plan.bump("relations")


def _plan_blocked(conn, proposal, base_seq, current_seq, plan, conflicts):
    for record_uid in sorted(proposal.blocked):
        item = proposal.blocked[record_uid]
        doc_id = item["doc_id"]
        base_val = _blocked_value(conn, base_seq, record_uid)
        cur_val = _blocked_value(conn, current_seq, record_uid)
        prop_val = _blocked_value_from_proposal(item)
        chosen, side = _three_way(
            base_val, cur_val, prop_val, conflicts, "blocked", record_uid,
            doc_id)
        if chosen is None:
            continue
        plan.blocked[record_uid] = {**item, "value": chosen, "source": side}
        plan.bump("blocked")


def _three_way(base, current, proposal, conflicts, kind, key, doc_id):
    if proposal == base:
        return None, None
    if current == base:
        return proposal, "proposal"
    if current == proposal:
        return current, "both"
    conflicts.append(Conflict(
        kind=kind, subject_key=key, doc_id=doc_id,
        reason=f"{kind} changed differently on both sides since ticket base",
        base=base, current=current, proposal=proposal,
    ))
    return None, None


def _relation_value(conn, seq, key):
    doc_id, rel_type, frm, to = _key_parts(key)
    row = relation_state_row(conn, doc_id, rel_type, frm, to, seq)
    return row["kind"] if row else None


def _blocked_value(conn, seq, record_uid):
    row = blocked_state_row(conn, seq, record_uid)
    if row is None:
        return None
    return {
        "disposition": row["disposition"],
        "from_fragment": row["from_fragment"],
        "to_fragment": row["to_fragment"],
    }


def _blocked_value_from_proposal(item) -> Optional[dict]:
    if item["disposition"] == REOPEN:
        return _REOPENED
    if item["disposition"] == BLOCK_REJECTED:
        return {"disposition": BLOCK_REJECTED,
                "from_fragment": None, "to_fragment": None}
    return {
        "disposition": BLOCK_ADOPTED,
        "from_fragment": item["from_fragment"],
        "to_fragment": item["to_fragment"],
    }


def _key_parts(key: str):
    return tuple(json.loads(key))


# ---------------------------------------------------------------------------
# 依赖检查
# ---------------------------------------------------------------------------

def _check_endpoint_dependencies(conn, proposal, base_seq, current_seq, plan,
                                 conflicts):
    """合批后存在的引用、已办结阻断的端点必须落在有效片段上。

    逐条流式核对当前版本的关系判定（服务端游标，不整张关系网驻留内存），
    只沿本批触及的文档与索引走；对象键完全不重叠也照查。
    """
    from .ws_state import relation_dependencies

    docs = proposal.touched_docs
    live = _live_after(conn, current_seq, plan, docs)
    seen = set()
    # 只看自己这一版已经生效、且提案没有另行裁决的引用
    overridden = {key for key, (kind, _s) in plan.relations.items()}
    for dep in relation_dependencies(conn, current_seq, docs, live):
        key = canonical_json([dep["doc_id"], dep["rel_type"],
                              dep["from_fragment"], dep["to_fragment"]])
        if key in overridden:
            continue
        if key in seen:
            continue
        seen.add(key)
        missing_side = ("from" if dep["from_fragment"] not in live else "to")
        missing_uid = (dep["from_fragment"] if missing_side == "from"
                       else dep["to_fragment"])
        conflicts.append(Conflict(
            kind="endpoint", subject_key=key, doc_id=dep["doc_id"],
            reason="relation endpoint would not be a live fragment after"
            " merge; a sibling fragment from the same region is not a"
            " substitute",
            current={"side": missing_side, "fragment": missing_uid},
            extra={"side": missing_side, "missing_fragment": missing_uid},
        ))
    # 本提案新增/确认的引用：端点同样必须落在合批后的有效片段上
    for key, (kind, _side) in sorted(plan.relations.items()):
        if kind != REL_PRESENT:
            continue
        doc_id, rel_type, frm, to = _key_parts(key)
        for side, uid in (("from", frm), ("to", to)):
            if uid in live:
                continue
            conflicts.append(Conflict(
                kind="endpoint", subject_key=key, doc_id=doc_id,
                reason="proposed relation endpoint would not be a live"
                " fragment after merge; a sibling fragment from the same"
                " region is not a substitute",
                proposal={"side": side, "fragment": uid},
                extra={"side": side, "missing_fragment": uid},
            ))
    blocks = _effective_blocks(conn, current_seq, plan, docs)
    reopened = {uid for uid, item in plan.blocked.items()
                if item["disposition"] == REOPEN}
    for record_uid, value in sorted(blocks.items()):
        if value["disposition"] != BLOCK_ADOPTED or record_uid in reopened:
            continue
        for side, uid in (("from", value["from_fragment"]),
                          ("to", value["to_fragment"])):
            if uid in live:
                continue
            conflicts.append(Conflict(
                kind="endpoint", subject_key=record_uid,
                doc_id=_blocked_doc(conn, record_uid),
                reason="adopted blocked record endpoint would not be live"
                " after merge",
                current={"side": side, "fragment": uid},
                extra={"record_uid": record_uid, "side": side,
                       "missing_fragment": uid},
            ))


def _check_dependency_retirement(conn, proposal, base_seq, current_seq, plan,
                                 conflicts):
    """当次裁决替换片段时，既有判定不得被静默悬空。"""
    if not plan.regions:
        return
    docs = proposal.touched_docs
    live = _live_after(conn, current_seq, plan, docs)
    rels = _effective_relations(conn, current_seq, plan, docs)
    for key, kind in sorted(rels.items()):
        if kind not in (REL_PRESENT, REL_ABSENT):
            continue
        doc_id, rel_type, frm, to = _key_parts(key)
        if frm in live and to in live:
            continue
        # 同一工单已就这条判定作出明确裁决（退役、否定或重述）即视为已处理；
        # 「没有」是对现存两个端点的判断，替掉端点时应同时给出去向。
        if key in proposal.relations:
            continue
        conflicts.append(Conflict(
            kind="dependency", subject_key=key, doc_id=doc_id,
            reason="replacing fragments would drop an existing adjudication"
            " without an explicit retirement in this same ticket",
            base={"kind": kind}, current={"kind": kind}, proposal=None,
            extra={"relation_key": key,
                   "missing_fragments": [u for u in (frm, to) if u not in live]},
        ))
    blocks = _effective_blocks(conn, current_seq, plan, docs)
    for record_uid, value in sorted(blocks.items()):
        if value["disposition"] != BLOCK_ADOPTED:
            continue
        if value["from_fragment"] in live and value["to_fragment"] in live:
            continue
        if record_uid in plan.blocked:
            continue
        conflicts.append(Conflict(
            kind="dependency", subject_key=record_uid,
            doc_id=_blocked_doc(conn, record_uid),
            reason="replacing fragments would leave an adopted blocked record"
            " dangling; reopen or re-adjudicate it in this ticket",
            base={"disposition": BLOCK_ADOPTED},
            current={"disposition": BLOCK_ADOPTED}, proposal=None,
            extra={"record_uid": record_uid,
                   "missing_fragments": [
                       u for u in (value["from_fragment"],
                                   value["to_fragment"]) if u not in live]},
        ))


def _live_after(conn, seq, plan, docs) -> set:
    """合批后的有效片段集合（仅限本批触及的文档）。

    只有受裁决区域直接替换的片段会失效；其他区域（含被拆分子集所属的
    父区域）的成员保持有效，除非其身份与新集合冲突。
    """
    live: set = set()
    replaced = set(plan.regions)
    for doc_id in docs:
        for uid, row in fragments_at(conn, seq, doc_id).items():
            region = row["region_uid"]
            if region is not None and region in replaced:
                # 该区域被整组裁决：旧成员不自动保留，由新值决定
                continue
            live.add(uid)
        for region_uid, (value, _side) in plan.regions.items():
            row = conn.execute(
                "SELECT doc_id FROM base_region WHERE region_uid=?",
                (region_uid,),
            ).fetchone()
            if row is None or row["doc_id"] != doc_id:
                continue
            if value[0] == REGION_FRAGMENTS:
                live.update(value[1])
    return live


def _effective_relations(conn, seq, plan, docs) -> dict:
    out = {}
    for doc_id in docs:
        out.update({k: r["kind"]
                    for k, r in relation_states_at(conn, seq, doc_id).items()})
    for key, (kind, _side) in plan.relations.items():
        if kind in (REL_PENDING, RESTORE_BASE):
            out.pop(key, None)
        else:
            out[key] = kind
    return out


def _effective_blocks(conn, seq, plan, docs) -> dict:
    out = {}
    for doc_id in docs:
        for record_uid, row in blocked_states_at(conn, seq, doc_id).items():
            out[record_uid] = {
                "disposition": row["disposition"],
                "from_fragment": row["from_fragment"],
                "to_fragment": row["to_fragment"],
            }
    for record_uid, item in plan.blocked.items():
        value = item["value"]
        if value is None or value is _REOPENED or item["disposition"] == REOPEN:
            out.pop(record_uid, None)
        else:
            out[record_uid] = value
    return out


def _blocked_doc(conn, record_uid) -> str:
    row = conn.execute(
        "SELECT doc_id FROM base_blocked WHERE record_uid=?", (record_uid,)
    ).fetchone()
    return row["doc_id"] if row else ""


# ---------------------------------------------------------------------------
# 落地新版本（只写变化的事项）
# ---------------------------------------------------------------------------

def _apply_plan(conn, proposal, plan, base_rev, current_rev, batch_id):
    seq = current_rev["seq"] + 1
    rev_id = revision_id(seq, current_rev["rev_id"], proposal.ticket_uid,
                         batch_id)
    conn.execute(
        "INSERT INTO revision(rev_id, seq, parent_rev, base_rev, ticket_uid,"
        " created_at, reason_json) VALUES (?,?,?,?,?,?,?)",
        (
            rev_id, seq, current_rev["rev_id"], base_rev["rev_id"],
            proposal.ticket_uid, utc_now(),
            canonical_json({
                "kind": "merge",
                "ticket_uid": proposal.ticket_uid,
                "reviewer": proposal.reviewer,
                "title": proposal.title,
                "batch_id": batch_id,
                "summary": plan.summary,
            }),
        ),
    )
    _write_fragments(conn, proposal, plan, seq, rev_id)
    _write_regions(conn, proposal, plan, seq, rev_id)
    _write_relations(conn, proposal, plan, seq, rev_id)
    _write_blocked(conn, proposal, plan, seq, rev_id)
    _write_evidence(conn, proposal, plan, seq, rev_id)
    conn.execute("UPDATE ws_meta SET value=? WHERE key='head_rev'", (rev_id,))
    conn.execute("UPDATE ws_meta SET value=? WHERE key='head_seq'", (str(seq),))
    return conn.execute(
        "SELECT * FROM revision WHERE rev_id=?", (rev_id,)).fetchone()


def _close(conn, table, where, params, seq) -> None:
    conn.execute(
        f"UPDATE {table} SET seq_removed=? WHERE {where} AND seq_removed IS NULL",
        (seq, *params),
    )


def _region_members(conn, seq, region_uid) -> list:
    row = region_state_row(conn, region_uid, seq)
    if row is None:
        return []
    return json.loads(row["fragments_json"])


def _write_fragments(conn, proposal, plan, seq, rev_id) -> None:
    prev = seq - 1
    for region_uid, (value, _side) in sorted(plan.regions.items()):
        keep = set(value[1]) if value[0] == REGION_FRAGMENTS else set()
        for uid in _region_members(conn, prev, region_uid):
            if uid not in keep:
                _close(conn, "fragment_state", "fragment_uid=?", (uid,), seq)
    for uid, info in sorted(plan.new_fragments.items()):
        if fragment_row(conn, uid, prev) is not None:
            continue
        conn.execute(
            "INSERT INTO fragment_state(fragment_uid, doc_id, layer, label,"
            " start_byte, end_byte, text_sha256, seen_text, origin,"
            " region_uid, seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                uid, info["doc_id"], info["layer"], info["label"],
                info["start"], info["end"], info["text_sha256"],
                info["seen_text"], info["origin"],
                proposal.fragment_regions.get(uid), seq, None, rev_id,
            ),
        )
        plan.bump("fragments_added")


def _write_regions(conn, proposal, plan, seq, rev_id) -> None:
    for region_uid, (value, side) in sorted(plan.regions.items()):
        _close(conn, "region_state", "region_uid=?", (region_uid,), seq)
        row = conn.execute(
            "SELECT doc_id, layer FROM base_region WHERE region_uid=?",
            (region_uid,),
        ).fetchone()
        conn.execute(
            "INSERT INTO region_state(region_uid, doc_id, layer, kind,"
            " fragments_json, seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (region_uid, row["doc_id"], row["layer"], value[0],
             canonical_json(sorted(value[1])), seq, None, rev_id),
        )
        if side == "both":
            plan.bump("regions_agreed")


def _write_relations(conn, proposal, plan, seq, rev_id) -> None:
    for key, (kind, side) in sorted(plan.relations.items()):
        doc_id, rel_type, frm, to = _key_parts(key)
        where = ("doc_id=? AND rel_type=? AND from_fragment=?"
                 " AND to_fragment=?")
        params = (doc_id, rel_type, frm, to)
        _close(conn, "relation_state", where, params, seq)
        if kind == REL_PENDING:
            continue
        if kind == RESTORE_BASE:
            base_row = relation_state_row(conn, doc_id, rel_type, frm, to,
                                          proposal.base_seq)
            if base_row is not None:
                _insert_relation(conn, doc_id, rel_type, frm, to,
                                 base_row["kind"], base_row["origin"], seq,
                                 rev_id)
            continue
        _insert_relation(conn, doc_id, rel_type, frm, to, kind,
                         "manual" if side in ("proposal", "both") else "auto",
                         seq, rev_id)
        if side == "both":
            plan.bump("relations_agreed")


def _insert_relation(conn, doc_id, rel_type, frm, to, kind, origin, seq,
                     rev_id):
    conn.execute(
        "INSERT INTO relation_state(doc_id, rel_type, from_fragment,"
        " to_fragment, kind, origin, seq_added, seq_removed, related_rev)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (doc_id, rel_type, frm, to, kind, origin, seq, None, rev_id),
    )


def _write_blocked(conn, proposal, plan, seq, rev_id) -> None:
    for record_uid, item in sorted(plan.blocked.items()):
        _close(conn, "blocked_state", "record_uid=?", (record_uid,), seq)
        if item["disposition"] == REOPEN:
            plan.bump("blocked_reopened")
            continue
        conn.execute(
            "INSERT INTO blocked_state(record_uid, disposition, rel_type,"
            " from_fragment, to_fragment, from_region_uid, to_region_uid,"
            " seq_added, seq_removed, related_rev)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                record_uid, item["disposition"], item["rel_type"],
                item.get("from_fragment"), item.get("to_fragment"),
                item.get("from_region"), item.get("to_region"), seq, None,
                rev_id,
            ),
        )
        plan.bump("blocked_closed")
        # 关联一条已人工确认存在的审定引用：不新增团队票，只登记关联。
        # 目标引用必须在本版已生效（present），否则视为无效关联，发布前会被拦下。


def _write_evidence(conn, proposal, plan, seq, rev_id) -> None:
    for (kind, key), note in sorted(proposal.notes.items()):
        conn.execute(
            "INSERT OR IGNORE INTO state_evidence(subject_kind, subject_key,"
            " seq, source_uid, ticket_uid, reviewer_id, reason,"
            " evidence_json, rev_id) VALUES (?,?,?,?,?,?,?,?,?)",
            (kind, key, seq, "ticket", proposal.ticket_uid,
             proposal.reviewer, note["reason"], canonical_json(note["evidence"]),
             rev_id),
        )
    # 双方给出相同业务值：保留双方理由与证据（重复重试不追加）
    for kind, key in sorted(_agreed_targets(plan)):
        prior = conn.execute(
            "SELECT source_uid, ticket_uid, reviewer_id, reason,"
            " evidence_json FROM state_evidence WHERE subject_kind=?"
            " AND subject_key=? AND seq < ? ORDER BY seq DESC LIMIT 1",
            (kind, key, seq),
        ).fetchone()
        if prior is None:
            continue
        if (prior["ticket_uid"] == proposal.ticket_uid):
            continue
        conn.execute(
            "INSERT OR IGNORE INTO state_evidence(subject_kind, subject_key,"
            " seq, source_uid, ticket_uid, reviewer_id, reason,"
            " evidence_json, rev_id) VALUES (?,?,?,?,?,?,?,?,?)",
            (kind, key, seq, prior["source_uid"], prior["ticket_uid"],
             prior["reviewer_id"], prior["reason"], prior["evidence_json"],
             rev_id),
        )


def _agreed_targets(plan) -> set:
    out = set()
    for region_uid, (_value, side) in plan.regions.items():
        if side == "both":
            out.add(("region", region_uid))
    for key, (_kind, side) in plan.relations.items():
        if side == "both":
            out.add(("relation", key))
    for record_uid, item in plan.blocked.items():
        if item.get("source") == "both":
            out.add(("blocked", record_uid))
    return out
