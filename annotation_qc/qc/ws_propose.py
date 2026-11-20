# -*- coding: utf-8 -*-
"""工单提案：相对工单基版把裁决解析成可合并的变更集，并先在自己的基版上成立。

提案的语义（均相对基版）::

    regions   : 每个被裁决区域给出完整审定片段集合（fragments / none）
    relations : 每个被裁决引用给出 present / absent / retired
    blocked   : 每个被裁决阻断记录给出 adopted / rejected / reopen

「present」确认存在，「absent」确认不存在（只对现存两个端点），
「retired」只针对基版已有的引用判定且不是否定票。
片段身份、区域集合、引用键都用内容派生或有序端点定键，与工单局部名、
提交顺序无关。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

from .ws_common import WsError, relation_key
from .ws_state import (
    BLOCK_ADOPTED,
    BLOCK_REJECTED,
    REGION_FRAGMENTS,
    REGION_NONE,
    REL_ABSENT,
    REL_PENDING,
    REL_PRESENT,
    REL_RETIRED,
    blocked_state_row,
    blocked_states_at,
    fragments_at,
    region_states_at,
    relation_state_row,
    relation_states_at,
)
from .ws_ticket import DocUniverse, ParsedTicket

BLOCK_REOPEN = "reopen"


@dataclass
class Proposal:
    ticket_uid: str
    reviewer: str
    title: Optional[str]
    base_seq: int
    base_rev: str
    doc_ids: list
    # 变更集中显式出现的事项（值均为「本提案希望的结果」）
    regions: dict = field(default_factory=dict)      # uid -> (kind, (uid,...))
    relations: dict = field(default_factory=dict)    # key -> kind
    blocked: dict = field(default_factory=dict)      # record -> disposition dict
    fragments: dict = field(default_factory=dict)    # uid -> 片段描述（新增/引用）
    notes: dict = field(default_factory=dict)        # (kind,key) -> {reason,...}
    fragment_regions: dict = field(default_factory=dict)  # uid -> region_uid
    ref_map: dict = field(default_factory=dict)      # 工单 ref -> 片段 uid
    touched_regions: set = field(default_factory=set)
    touched_docs: set = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "ticket_uid": self.ticket_uid,
            "reviewer": self.reviewer,
            "title": self.title,
            "base_rev": self.base_rev,
            "base_seq": self.base_seq,
            "doc_ids": self.doc_ids,
            "regions": {k: {"kind": v[0], "fragments": list(v[1])}
                        for k, v in sorted(self.regions.items())},
            "relations": {k: v for k, v in sorted(self.relations.items())},
            "blocked": {k: dict(v) for k, v in sorted(self.blocked.items())},
            "new_fragments": {
                uid: {k: info[k] for k in
                      ("doc_id", "layer", "label", "start", "end")}
                for uid, info in sorted(self.fragments.items())
            },
            "notes": [
                {"subject_kind": k[0], "subject_key": k[1], "reason": v["reason"],
                 "evidence": v["evidence"]}
                for k, v in sorted(self.notes.items())
            ],
        }

    def describe(self, conn) -> dict:
        """预览用：在本工单基版上拟议的整批提案与关联后果。"""
        from .ws_state import (
            blocked_state_row, fragments_at, relation_state_row)

        out = self.to_dict()
        fragments = {}
        for uid, info in sorted(self.fragments.items()):
            fragments[uid] = {
                "doc_id": info["doc_id"], "layer": info["layer"],
                "label": info["label"], "start": info["start"],
                "end": info["end"], "origin": info["origin"],
            }
        out["fragments"] = fragments
        out["relation_consequences"] = []
        for key in sorted(self.relations):
            doc_id, rel_type, frm, to = json.loads(key)
            base_row = relation_state_row(
                conn, doc_id, rel_type, frm, to, self.base_seq)
            base_kind = base_row["kind"] if base_row else None
            out["relation_consequences"].append({
                "relation_key": key,
                "from": frm, "to": to, "type": rel_type,
                "base_kind": base_kind,
                "proposed_kind": self.relations[key],
                "from_live_at_base": frm in fragments_at(
                    conn, self.base_seq, doc_id),
                "to_live_at_base": to in fragments_at(
                    conn, self.base_seq, doc_id),
            })
        out["blocked_consequences"] = []
        for record_uid, item in sorted(self.blocked.items()):
            out["blocked_consequences"].append({
                "record_uid": record_uid,
                "disposition": item["disposition"],
                "relation_key": item.get("relation_key"),
                "from_fragment": item.get("from_fragment"),
                "to_fragment": item.get("to_fragment"),
            })
        return out


class ProposalBuilder:
    def __init__(self, conn, sources, ticket_uid: str, reviewer: str,
                 title: Optional[str], base_seq: int, base_rev: str,
                 parsed: ParsedTicket):
        self.conn = conn
        self.sources = sources
        self.parsed = parsed
        self.proposal = Proposal(
            ticket_uid=ticket_uid, reviewer=reviewer, title=title,
            base_seq=base_seq, base_rev=base_rev, doc_ids=[],
        )
        self._universes: dict[str, DocUniverse] = {}

    # ------------------------------------------------------------------

    def build(self) -> Proposal:
        self._collect_docs()
        self._build_regions()
        self._build_relations()
        self._build_blocked()
        self._check_fragment_dependencies()
        self._check_blocked_dependencies()
        self.proposal.doc_ids = sorted(self.proposal.touched_docs)
        return self.proposal

    # ------------------------------------------------------------------
    # 文档与区域
    # ------------------------------------------------------------------

    def _collect_docs(self) -> None:
        for reg in self.parsed.regions:
            row = self.conn.execute(
                "SELECT * FROM base_region WHERE region_uid=?", (reg.region_uid,)
            ).fetchone()
            if row is None:
                raise WsError(
                    "TICKET_UNKNOWN_REGION",
                    f"region {reg.region_uid!r} not in workspace base")
            self.proposal.touched_docs.add(row["doc_id"])
        for rel in self.parsed.relations:
            self.proposal.touched_docs.add(rel.doc_id)
        for blk in self.parsed.blocked:
            row = self.conn.execute(
                "SELECT doc_id FROM base_blocked WHERE record_uid=?",
                (blk.record_uid,),
            ).fetchone()
            if row is None:
                raise WsError(
                    "TICKET_UNKNOWN_BLOCKED",
                    f"blocked record {blk.record_uid!r} not in workspace base")
            if blk.disposition == "adopted" and blk.doc_id != row["doc_id"]:
                raise WsError(
                    "TICKET_BLOCKED_DOC_MISMATCH",
                    f"blocked {blk.record_uid!r}: adopted relation doc"
                    f" {blk.doc_id!r} != original doc {row['doc_id']!r}")
            self.proposal.touched_docs.add(row["doc_id"])

    def _universe(self, doc_id: str) -> DocUniverse:
        if doc_id not in self._universes:
            self._universes[doc_id] = DocUniverse(
                self.conn, self.proposal.base_seq, self.sources, doc_id)
        return self._universes[doc_id]

    def _build_regions(self) -> None:
        seen = set()
        for parsed in self.parsed.regions:
            if parsed.region_uid in seen:
                raise WsError(
                    "TICKET_FORMAT",
                    f"region {parsed.region_uid!r} adjudicated twice")
            seen.add(parsed.region_uid)
            row = self.conn.execute(
                "SELECT * FROM base_region WHERE region_uid=?",
                (parsed.region_uid,),
            ).fetchone()
            uni = self._universe(row["doc_id"])
            uni.plan_region(parsed, row)
            uids = []
            for frag in parsed.fragments:
                self.proposal.fragment_regions[frag.local_uid] = row["region_uid"]
                self.proposal.ref_map[frag.ref] = frag.local_uid
                self.proposal.fragments.setdefault(frag.local_uid, {
                    "uid": frag.local_uid, "doc_id": frag.doc_id,
                    "layer": frag.layer, "label": frag.label,
                    "start": frag.start, "end": frag.end,
                    "seen_text": frag.seen_text,
                    "text_sha256": frag.text_sha256, "origin": frag.kind,
                })
                uids.append(frag.local_uid)
            if parsed.decision == "none":
                value = (REGION_NONE, ())
            else:
                value = (REGION_FRAGMENTS, tuple(sorted(uids)))
            self.proposal.regions[parsed.region_uid] = value
            self.proposal.touched_regions.add(parsed.region_uid)
            self.proposal.notes[("region", parsed.region_uid)] = {
                "reason": parsed.reason, "evidence": parsed.evidence,
            }
            self._validate_evidence(parsed.evidence, "region",
                                    parsed.region_uid)

    # ------------------------------------------------------------------
    # 引用
    # ------------------------------------------------------------------

    def _build_relations(self) -> None:
        seen = set()
        for parsed in self.parsed.relations:
            key = relation_key(parsed.doc_id, parsed.rel_type,
                               parsed.from_fragment, parsed.to_fragment)
            if key in seen:
                raise WsError(
                    "TICKET_FORMAT", f"relation {parsed.from_fragment!r} ->"
                    f" {parsed.to_fragment!r} adjudicated twice")
            seen.add(key)
            rel_pol = self.conn.execute(
                "SELECT * FROM ws_relation_type WHERE rel_type=?",
                (parsed.rel_type,),
            ).fetchone()
            if rel_pol is None:
                raise WsError(
                    "UNKNOWN_RELATION_TYPE",
                    f"relation type {parsed.rel_type!r} not declared")
            uni = self._universe(parsed.doc_id)
            from_info = self._endpoint(uni, parsed, parsed.from_fragment, "from")
            to_info = self._endpoint(uni, parsed, parsed.to_fragment, "to")
            frm, to = from_info["uid"], to_info["uid"]
            key = relation_key(parsed.doc_id, parsed.rel_type, frm, to)

            base_row = relation_state_row(
                self.conn, parsed.doc_id, parsed.rel_type, frm, to,
                self.proposal.base_seq)
            base_kind = base_row["kind"] if base_row else None

            if parsed.decision == REL_RETIRED:
                if base_kind not in (REL_PRESENT, REL_ABSENT):
                    raise WsError(
                        "TICKET_RETIRE_UNKNOWN",
                        f"cannot retire {parsed.rel_type} {frm} -> {to}: no"
                        " existing present/absent adjudication at base",
                        {"relation_key": key})
            else:
                self._check_layer_constraint(
                    parsed.rel_type, rel_pol, from_info, to_info)
                if parsed.decision == REL_ABSENT:
                    # 「没有」只对现存两个端点作出判断
                    if from_info["live_base"] is False or to_info["live_base"] is False:
                        raise WsError(
                            "TICKET_ABSENT_ENDPOINT_NOT_LIVE",
                            f"cannot confirm absence of {parsed.rel_type}"
                            f" {frm} -> {to}: endpoints are not both live at"
                            " the ticket base",
                            {"relation_key": key})
                if parsed.decision == REL_PRESENT:
                    if base_kind == REL_PRESENT:
                        pass  # 确认既有存在：等同保持
            self.proposal.relations[key] = parsed.decision if parsed.decision != REL_PENDING else base_kind
            self.proposal.notes[("relation", key)] = {
                "reason": parsed.reason, "evidence": parsed.evidence,
            }
            self._validate_evidence(parsed.evidence, "relation", key)

    def _endpoint(self, uni: DocUniverse, parsed, ref: str, side: str) -> dict:
        try:
            info = uni.resolve_endpoint(ref)
        except WsError as exc:
            if exc.code == "TICKET_UNKNOWN_ENDPOINT":
                exc.detail.setdefault("relation", {
                    "doc": parsed.doc_id, "type": parsed.rel_type,
                    "from": parsed.from_fragment, "to": parsed.to_fragment,
                    "side": side,
                })
            raise
        info = dict(info)
        live_base = info["uid"] in fragments_at(
            self.conn, self.proposal.base_seq, info["doc_id"], info["layer"])
        info["live_base"] = live_base
        return info

    def _check_layer_constraint(self, rel_type, rel_pol, from_info, to_info):
        if from_info["layer"] != rel_pol["from_layer"]:
            raise WsError(
                "RELATION_LAYER_MISMATCH",
                f"relation {rel_type!r} from endpoint is on layer"
                f" {from_info['layer']!r}, policy requires"
                f" {rel_pol['from_layer']!r}")
        if to_info["layer"] != rel_pol["to_layer"]:
            raise WsError(
                "RELATION_LAYER_MISMATCH",
                f"relation {rel_type!r} to endpoint is on layer"
                f" {to_info['layer']!r}, policy requires"
                f" {rel_pol['to_layer']!r}")

    # ------------------------------------------------------------------
    # 阻断记录
    # ------------------------------------------------------------------

    def _build_blocked(self) -> None:
        seen = set()
        for parsed in self.parsed.blocked:
            if parsed.record_uid in seen:
                raise WsError(
                    "TICKET_FORMAT",
                    f"blocked record {parsed.record_uid!r} adjudicated twice")
            seen.add(parsed.record_uid)
            row = self.conn.execute(
                "SELECT * FROM base_blocked WHERE record_uid=?",
                (parsed.record_uid,),
            ).fetchone()
            if parsed.disposition == BLOCK_REOPEN:
                self.proposal.blocked[parsed.record_uid] = {
                    "disposition": BLOCK_REOPEN,
                    "rel_type": row["rel_type"], "doc_id": row["doc_id"],
                }
            elif parsed.disposition == BLOCK_REJECTED:
                self.proposal.blocked[parsed.record_uid] = {
                    "disposition": BLOCK_REJECTED,
                    "rel_type": row["rel_type"], "doc_id": row["doc_id"],
                }
            else:
                if parsed.rel_type != row["rel_type"]:
                    raise WsError(
                        "TICKET_BLOCKED_TYPE_MISMATCH",
                        f"blocked {parsed.record_uid!r}: adopted relation type"
                        f" {parsed.rel_type!r} != original {row['rel_type']!r}")
                uni = self._universe(row["doc_id"])
                from_info = uni.resolve_endpoint(parsed.from_fragment)
                to_info = uni.resolve_endpoint(parsed.to_fragment)
                rel_pol = self.conn.execute(
                    "SELECT * FROM ws_relation_type WHERE rel_type=?",
                    (row["rel_type"],),
                ).fetchone()
                if rel_pol is None:
                    raise WsError(
                        "UNKNOWN_RELATION_TYPE",
                        f"relation type {row['rel_type']!r} not declared")
                self._check_layer_constraint(row["rel_type"], rel_pol,
                                             from_info, to_info)
                from_region = self._region_of(from_info["uid"])
                to_region = self._region_of(to_info["uid"])
                if from_region != row["from_region"] or to_region != row["to_region"]:
                    raise WsError(
                        "TICKET_BLOCKED_REGION_MISMATCH",
                        f"blocked {parsed.record_uid!r}: adopted endpoints must"
                        " stay in the original endpoint regions",
                        {
                            "expected": [row["from_region"], row["to_region"]],
                            "actual": [from_region, to_region],
                        })
                target_key = relation_key(
                    row["doc_id"], row["rel_type"], from_info["uid"],
                    to_info["uid"])
                if not self._relation_present_after(target_key):
                    raise WsError(
                        "TICKET_BLOCKED_NOT_CONFIRMED",
                        f"blocked {parsed.record_uid!r}: adopted target must be"
                        " an adjudicated reference already confirmed present"
                        " (decide it present in this ticket or rely on the"
                        " base verdict)",
                        {"relation_key": target_key})
                self.proposal.blocked[parsed.record_uid] = {
                    "disposition": BLOCK_ADOPTED,
                    "rel_type": row["rel_type"], "doc_id": row["doc_id"],
                    "from_fragment": from_info["uid"],
                    "to_fragment": to_info["uid"],
                    "from_region": from_region, "to_region": to_region,
                    "relation_key": target_key,
                }
            self.proposal.notes[("blocked", parsed.record_uid)] = {
                "reason": parsed.reason, "evidence": parsed.evidence,
            }
            self._validate_evidence(parsed.evidence, "blocked", parsed.record_uid)

    def _relation_present_after(self, key: str) -> bool:
        """该引用在本提案拟合后是否已确认存在（含基版既有判定）。"""
        if key in self.proposal.relations:
            return self.proposal.relations[key] == REL_PRESENT
        doc_id, rel_type, frm, to = json.loads(key)
        row = relation_state_row(
            self.conn, doc_id, rel_type, frm, to, self.proposal.base_seq)
        return row is not None and row["kind"] == REL_PRESENT

    def _region_of(self, fragment_uid: str) -> Optional[str]:
        if fragment_uid in self.proposal.fragment_regions:
            return self.proposal.fragment_regions[fragment_uid]
        for reg_uid, row in region_states_at(
            self.conn, self.proposal.base_seq
        ).items():
            if fragment_uid in json.loads(row["fragments_json"]):
                return reg_uid
        return None

    # ------------------------------------------------------------------
    # 基版上的自洽性：不得静默丢弃既有判定
    # ------------------------------------------------------------------

    def _live_after(self, doc_id: str) -> set:
        """本提案在基版上拟合后的该文档片段集合。"""
        live = set()
        for reg_uid, row in region_states_at(
            self.conn, self.proposal.base_seq, doc_id
        ).items():
            if reg_uid in self.proposal.regions:
                kind, uids = self.proposal.regions[reg_uid]
            else:
                kind, uids = row["kind"], json.loads(row["fragments_json"])
            if kind == REGION_FRAGMENTS:
                live.update(uids)
        return live

    def _check_fragment_dependencies(self) -> None:
        for doc_id in self.proposal.touched_docs:
            if not self.proposal.touched_regions:
                continue
            live = self._live_after(doc_id)
            for key, row in relation_states_at(
                self.conn, self.proposal.base_seq, doc_id
            ).items():
                if row["kind"] not in (REL_PRESENT, REL_ABSENT):
                    continue
                ends_missing = (
                    row["from_fragment"] not in live
                    or row["to_fragment"] not in live
                )
                if not ends_missing:
                    continue
                if self.proposal.relations.get(key) == REL_RETIRED:
                    continue
                raise WsError(
                    "TICKET_RELATION_MUST_RETIRE",
                    "replacing fragments invalidates an existing adjudication"
                    f" ({row['kind']}) {row['from_fragment']} ->"
                    f" {row['to_fragment']}; retire it explicitly in the same"
                    " ticket",
                    {"relation_key": key,
                     "missing_from": row["from_fragment"] not in live,
                     "missing_to": row["to_fragment"] not in live})

    def _check_blocked_dependencies(self) -> None:
        for doc_id in self.proposal.touched_docs:
            if not self.proposal.touched_regions:
                continue
            live = self._live_after(doc_id)
            for record_uid, row in blocked_states_at(
                self.conn, self.proposal.base_seq, doc_id
            ).items():
                if row["disposition"] != BLOCK_ADOPTED:
                    continue
                if (row["from_fragment"] in live
                        and row["to_fragment"] in live):
                    continue
                if record_uid in self.proposal.blocked:
                    continue
                raise WsError(
                    "TICKET_BLOCKED_MUST_REOPEN",
                    "replacing fragments invalidates an adopted blocked record"
                    f" {record_uid}; reopen it or re-adjudicate it in the same"
                    " ticket",
                    {"record_uid": record_uid})

    # ------------------------------------------------------------------

    def _validate_evidence(self, evidence: list, kind: str, key: str) -> None:
        """证据只作指针校验；引用不改变其自动判定。

        指针必须落在工作区底稿快照中：候选、原始记录或复核项。
        """
        for item in evidence:
            pointer = item["pointer"]
            target = item["kind"]
            if target == "candidate":
                uid = pointer.get("candidate") or pointer.get("uid")
                row = self._snapshot_lookup(
                    "base_region_candidate", "candidate_uid", uid)
            elif target == "record":
                uid = pointer.get("record") or pointer.get("uid")
                row = self._snapshot_lookup("base_blocked", "record_uid", uid)
            else:
                uid = pointer.get("review") or pointer.get("uid")
                row = self._snapshot_lookup(
                    "base_review_item", "review_uid", uid)
            if row is None:
                raise WsError(
                    "TICKET_EVIDENCE_UNKNOWN",
                    f"{kind} {key}: evidence pointer {uid!r} not found in"
                    " workspace base",
                    {"evidence_kind": target, "pointer": uid})

    def _snapshot_lookup(self, table: str, column: str, uid):
        if not uid:
            return None
        return self.conn.execute(
            f"SELECT 1 FROM {table} WHERE {column}=?", (uid,)
        ).fetchone()


def build_proposal(conn, sources, ticket_row) -> Proposal:
    from .ws_ticket import parse_ticket

    data = json.loads(ticket_row["content_json"])
    parsed = parse_ticket(data)
    builder = ProposalBuilder(
        conn, sources, ticket_row["ticket_uid"], ticket_row["reviewer_id"],
        ticket_row["title"], ticket_row["base_rev"],
        _seq_of(conn, ticket_row["base_rev"]), parsed,
    )
    return builder.build()


def _seq_of(conn, rev_id: str) -> int:
    row = conn.execute("SELECT seq FROM revision WHERE rev_id=?", (rev_id,)).fetchone()
    if row is None:
        raise WsError("REVISION_NOT_FOUND", f"unknown base revision {rev_id!r}")
    return row["seq"]


def normalize_region_value(kind: str, fragments_json: str) -> tuple:
    if kind == REGION_FRAGMENTS:
        return (REGION_FRAGMENTS, tuple(sorted(json.loads(fragments_json))))
    return (kind, ())


def base_region_values(conn, seq: int, doc_id: str) -> dict:
    out = {}
    for uid, row in region_states_at(conn, seq, doc_id).items():
        out[uid] = normalize_region_value(row["kind"], row["fragments_json"])
    return out


def base_relation_values(conn, seq: int, doc_id: str) -> dict:
    return {k: r["kind"] for k, r in
            relation_states_at(conn, seq, doc_id).items()}


def base_blocked_values(conn, seq: int, doc_id: str) -> dict:
    out = {}
    for record_uid, row in blocked_states_at(conn, seq, doc_id).items():
        out[record_uid] = {
            "disposition": row["disposition"],
            "from_fragment": row["from_fragment"],
            "to_fragment": row["to_fragment"],
            "rel_type": row["rel_type"],
            "doc_id": doc_id,
        }
    return out
