# -*- coding: utf-8 -*-
"""工单：解析、结构校验与语义校验（均相对工单基版）。

工单是复核员提交的一份 JSON 文件::

    {
      "reviewer": "alice",
      "title": "拆分 X 并退役旧引用",
      "regions": [
        {"region": "<region_uid>", "decision": "fragments",
         "fragments": [
            {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
            {"ref": "X2", "fragment": "<既有审定片段 uid>"}
         ]}
      ],
      "relations": [
        {"doc": "D1", "type": "ref", "from": "X1", "to": "Y",
         "decision": "present", "reason": "..."},
        {"doc": "D1", "type": "ref", "from": "X", "to": "Y",
         "decision": "retired", "reason": "X 已拆分为 X1/X2"}
      ],
      "blocked": [
        {"record": "<record_uid>", "disposition": "adopted",
         "relation": {"doc": "D1", "type": "ref", "from": "X2", "to": "Y"},
         "reason": "..."}
      ]
    }

- 片段引用可指向：工单本地的 ``ref``、同一基版上的既有审定片段 ``fragment``、
  或底稿证据 ``proposal``（必须恰好是基版审定的那个片段）；
- 关系端点同理；引用判定用有序端点 + 类型定键；
- 每项裁决必须给出非空 ``reason``；
- ``evidence`` 可选，列证据指针（候选 / 原始记录 / 复核项），
  引用证据不改变其自动判定。

解析器不读数据库；语义校验需要基版与原文时由调用方传入。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

from .util import canonical_json
from .ws_common import WsError

ALLOWED_TOP = {"reviewer", "title", "base", "regions", "relations", "blocked",
               "notes"}
ALLOWED_REGION = {"region", "decision", "fragments", "reason", "evidence"}
ALLOWED_FRAGMENT = {"ref", "fragment", "proposal", "start", "end", "label"}
ALLOWED_RELATION = {"doc", "type", "from", "to", "decision", "reason",
                    "evidence"}
ALLOWED_BLOCKED = {"record", "disposition", "relation", "reason", "evidence"}
ALLOWED_RETIRE = {"doc", "type", "from", "to", "reason", "evidence"}

REGION_DECISIONS = ("fragments", "none")
RELATION_DECISIONS = ("present", "absent", "retired")
BLOCKED_DECISIONS = ("adopted", "rejected", "reopen")


@dataclass
class ParsedFragment:
    ref: str
    kind: str                    # manual | existing | proposal
    local_uid: str               # 规划出的审定片段身份
    doc_id: str
    layer: str
    label: str
    start: int
    end: int
    seen_text: str               # 由原文取出（manual/proposal）或基版快照
    text_sha256: str
    evidence: dict = field(default_factory=dict)


@dataclass
class ParsedRegion:
    region_uid: str
    decision: str
    fragments: list[ParsedFragment]
    reason: str
    evidence: list


@dataclass
class ParsedRelation:
    doc_id: str
    rel_type: str
    from_fragment: str
    to_fragment: str
    decision: str
    reason: str
    evidence: list


@dataclass
class ParsedBlocked:
    record_uid: str
    disposition: str
    doc_id: str
    rel_type: str
    from_fragment: Optional[str]
    to_fragment: Optional[str]
    reason: str
    evidence: list


@dataclass
class ParsedTicket:
    reviewer: str
    content: dict
    title: Optional[str]
    base_hint: Optional[str]
    regions: list[ParsedRegion]
    relations: list[ParsedRelation]
    blocked: list[ParsedBlocked]
    notes: list


def load_ticket(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        raise WsError("TICKET_NOT_FOUND", f"ticket not found: {path}")
    except json.JSONDecodeError as exc:
        raise WsError("TICKET_FORMAT", f"invalid JSON: {exc.msg}",
                      {"line": exc.lineno, "column": exc.colno})
    if not isinstance(data, dict):
        raise WsError("TICKET_FORMAT", "ticket must be a JSON object")
    return data


def parse_ticket(data: dict) -> ParsedTicket:
    """只做结构层解析；不访问数据库。"""
    _reject_unknown(data, ALLOWED_TOP, "ticket")
    reviewer = data.get("reviewer")
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise WsError("TICKET_FORMAT", "ticket.reviewer must be a non-empty string")
    title = data.get("title")
    if title is not None and not isinstance(title, str):
        raise WsError("TICKET_FORMAT", "ticket.title must be a string")
    base_hint = data.get("base")
    if base_hint is not None and not isinstance(base_hint, (str, int)):
        raise WsError("TICKET_FORMAT", "ticket.base must be a revision id or seq")

    raw_regions = _list_field(data, "regions")
    raw_relations = _list_field(data, "relations")
    raw_blocked = _list_field(data, "blocked")
    notes = _list_field(data, "notes")

    regions = [_parse_region_shape(r, i) for i, r in enumerate(raw_regions)]
    relations = [_parse_relation_shape(r, i) for i, r in enumerate(raw_relations)]
    blocked = [_parse_blocked_shape(r, i) for i, r in enumerate(raw_blocked)]

    if not (regions or relations or blocked):
        raise WsError("TICKET_EMPTY", "ticket contains no decisions")
    return ParsedTicket(
        reviewer=reviewer.strip(),
        content=data,
        title=title,
        base_hint=str(base_hint) if base_hint is not None else None,
        regions=regions,
        relations=relations,
        blocked=blocked,
        notes=notes,
    )


def _parse_region_shape(raw: dict, idx: int) -> ParsedRegion:
    loc = {"region_index": idx}
    if not isinstance(raw, dict):
        raise WsError("TICKET_FORMAT", "region entry must be an object", loc)
    _reject_unknown(raw, ALLOWED_REGION, f"regions[{idx}]")
    region_uid = _nonempty_str(raw.get("region"), "region", loc)
    decision = raw.get("decision")
    if decision not in REGION_DECISIONS:
        raise WsError(
            "TICKET_FORMAT",
            f"regions[{idx}].decision must be one of {REGION_DECISIONS}", loc)
    reason = _nonempty_str(raw.get("reason"), "reason", loc)
    evidence = _evidence(raw.get("evidence"), loc)
    frags_raw = raw.get("fragments", [])
    if not isinstance(frags_raw, list):
        raise WsError("TICKET_FORMAT", "regions[].fragments must be a list", loc)
    if decision == "none" and frags_raw:
        raise WsError(
            "TICKET_FORMAT",
            f"regions[{idx}]: decision 'none' must not list fragments", loc)
    if decision == "fragments" and not frags_raw:
        raise WsError(
            "TICKET_FORMAT",
            f"regions[{idx}]: decision 'fragments' needs at least one fragment",
            loc)
    frags = [_parse_fragment_shape(f, idx, j) for j, f in enumerate(frags_raw)]
    refs = [f.ref for f in frags]
    if len(set(refs)) != len(refs):
        raise WsError(
            "TICKET_FORMAT", f"regions[{idx}]: duplicate fragment ref", loc)
    return ParsedRegion(region_uid, decision, frags, reason, evidence)


def _parse_fragment_shape(raw: dict, region_idx: int, idx: int) -> ParsedFragment:
    loc = {"region_index": region_idx, "fragment_index": idx}
    if not isinstance(raw, dict):
        raise WsError("TICKET_FORMAT", "fragment entry must be an object", loc)
    _reject_unknown(raw, ALLOWED_FRAGMENT, f"regions[].fragments[{idx}]")
    ref = _nonempty_str(raw.get("ref"), "ref", loc)
    keys = [k for k in ("fragment", "proposal", "start") if k in raw]
    if len(keys) != 1:
        raise WsError(
            "TICKET_FORMAT",
            f"fragment {ref!r} must specify exactly one of: fragment uid,"
            " proposal uid, or manual start/end", loc)
    if keys[0] in ("fragment", "proposal"):
        uid = _nonempty_str(raw[keys[0]], keys[0], loc)
        if "end" in raw or "label" in raw:
            raise WsError(
                "TICKET_FORMAT",
                f"fragment {ref!r}: existing/proposal fragments must not"
                " override end/label", loc)
        if not uid:
            raise WsError("TICKET_FORMAT", "empty fragment reference", loc)
        return ParsedFragment(
            ref=ref, kind=keys[0], local_uid="", doc_id="", layer="",
            label="", start=-1, end=-1, seen_text="", text_sha256="",
            evidence={keys[0]: uid},
        )
    start = _int(raw.get("start"), "start", loc)
    end = _int(raw.get("end"), "end", loc)
    label = _nonempty_str(raw.get("label"), "label", loc)
    return ParsedFragment(
        ref=ref, kind="manual", local_uid="", doc_id="", layer="", label=label,
        start=start, end=end, seen_text="", text_sha256="",
        evidence={"manual": [start, end, label]},
    )


def _parse_relation_shape(raw: dict, idx: int) -> ParsedRelation:
    loc = {"relation_index": idx}
    if not isinstance(raw, dict):
        raise WsError("TICKET_FORMAT", "relation entry must be an object", loc)
    _reject_unknown(raw, ALLOWED_RELATION, f"relations[{idx}]")
    decision = raw.get("decision")
    if decision not in RELATION_DECISIONS:
        raise WsError(
            "TICKET_FORMAT",
            f"relations[{idx}].decision must be one of {RELATION_DECISIONS}",
            loc)
    return ParsedRelation(
        doc_id=_nonempty_str(raw.get("doc"), "doc", loc),
        rel_type=_nonempty_str(raw.get("type"), "type", loc),
        from_fragment=_nonempty_str(raw.get("from"), "from", loc),
        to_fragment=_nonempty_str(raw.get("to"), "to", loc),
        decision=decision,
        reason=_nonempty_str(raw.get("reason"), "reason", loc),
        evidence=_evidence(raw.get("evidence"), loc),
    )


def _parse_blocked_shape(raw: dict, idx: int) -> ParsedBlocked:
    loc = {"blocked_index": idx}
    if not isinstance(raw, dict):
        raise WsError("TICKET_FORMAT", "blocked entry must be an object", loc)
    _reject_unknown(raw, ALLOWED_BLOCKED, f"blocked[{idx}]")
    disposition = raw.get("disposition")
    if disposition not in BLOCKED_DECISIONS:
        raise WsError(
            "TICKET_FORMAT",
            f"blocked[{idx}].disposition must be one of {BLOCKED_DECISIONS}",
            loc)
    rel = raw.get("relation")
    if disposition in ("rejected", "reopen") and rel is not None:
        raise WsError(
            "TICKET_FORMAT",
            f"blocked[{idx}]: {disposition} must not carry a relation", loc)
    if disposition == "adopted":
        if not isinstance(rel, dict):
            raise WsError(
                "TICKET_FORMAT",
                f"blocked[{idx}]: adopted requires a relation target", loc)
        _reject_unknown(rel, {"doc", "type", "from", "to"},
                        f"blocked[{idx}].relation")
        doc_id = _nonempty_str(rel.get("doc"), "doc", loc)
        rel_type = _nonempty_str(rel.get("type"), "type", loc)
        frm = _nonempty_str(rel.get("from"), "from", loc)
        to = _nonempty_str(rel.get("to"), "to", loc)
    else:
        doc_id = rel_type = frm = to = None
    return ParsedBlocked(
        record_uid=_nonempty_str(raw.get("record"), "record", loc),
        disposition=disposition,
        doc_id=doc_id or "",
        rel_type=rel_type or "",
        from_fragment=frm,
        to_fragment=to,
        reason=_nonempty_str(raw.get("reason"), "reason", loc),
        evidence=_evidence(raw.get("evidence"), loc),
    )


# ---------------------------------------------------------------------------
# 端点到审定片段的解析（需要基版与原文）
# ---------------------------------------------------------------------------

class DocUniverse:
    """单篇公文在工单基版上可见的片段，以及本工单新划/新选的片段。

    相同 (doc, layer, start, end, label) 一律映射到同一个内容派生身份，
    与工单局部名、提交顺序无关。端点解析只在本文档内进行。
    """

    def __init__(self, conn, seq: int, sources, doc_id: str):
        from .ws_state import fragments_at

        self.conn = conn
        self.doc_id = doc_id
        self.base_seq = seq
        self.sources = sources
        self.by_uid: dict[str, dict] = {}
        self.by_ref: dict[str, str] = {}
        for uid, row in fragments_at(conn, seq, doc_id).items():
            self.by_uid[uid] = {
                "uid": uid, "doc_id": doc_id, "layer": row["layer"],
                "label": row["label"], "start": row["start_byte"],
                "end": row["end_byte"], "seen_text": row["seen_text"],
                "text_sha256": row["text_sha256"], "origin": row["origin"],
            }

    def plan_region(self, parsed, region_row) -> None:
        """规划该区域的完整片段集合（先全部解析，再登记引用）。"""
        layer = region_row["layer"]
        envelope = (region_row["start_byte"], region_row["end_byte"])
        planned = [self._resolve_one(frag, layer) for frag in parsed.fragments]
        for frag in planned:
            if not (envelope[0] <= frag.start < frag.end <= envelope[1]):
                raise WsError(
                    "TICKET_FRAGMENT_OUTSIDE_REGION",
                    f"fragment {frag.ref!r}[{frag.start},{frag.end}) is outside"
                    f" region envelope [{envelope[0]},{envelope[1]})",
                    {"fragment_uid": frag.local_uid})
        _check_overlaps(planned)
        for frag in planned:
            if frag.ref in self.by_ref:
                raise WsError(
                    "TICKET_FORMAT", f"duplicate fragment ref {frag.ref!r}")
            self.by_ref[frag.ref] = frag.local_uid
            self.by_uid.setdefault(frag.local_uid, {
                "uid": frag.local_uid, "doc_id": frag.doc_id,
                "layer": frag.layer, "label": frag.label, "start": frag.start,
                "end": frag.end, "seen_text": frag.seen_text,
                "text_sha256": frag.text_sha256, "origin": frag.kind,
            })
        parsed.fragments = planned

    def _resolve_one(self, frag, layer: str):
        from .ws_common import fragment_uid

        if frag.kind == "manual":
            self._check_label(layer, frag.label)
            if not (0 <= frag.start < frag.end):
                raise WsError(
                    "TICKET_BAD_RANGE",
                    f"fragment {frag.ref!r} has empty/negative range")
            ok, why = self.sources.check_span(self.doc_id, frag.start, frag.end)
            if not ok:
                raise WsError(
                    "TICKET_BAD_RANGE",
                    f"fragment {frag.ref!r}[{frag.start},{frag.end}): {why}")
            text = self.sources.text_for(self.doc_id, frag.start, frag.end)
            uid = fragment_uid(self.doc_id, layer, frag.start, frag.end,
                               frag.label)
            return _with(frag, uid=uid, doc_id=self.doc_id, layer=layer,
                         label=frag.label, start=frag.start, end=frag.end,
                         seen=text)
        uid = frag.evidence.get(frag.kind)
        if frag.kind == "fragment":
            info = self.by_uid.get(uid)
            if info is None:
                raise WsError(
                    "TICKET_UNKNOWN_FRAGMENT",
                    f"fragment {frag.ref!r}: {uid!r} is not an adjudicated"
                    f" fragment of doc {self.doc_id} at the ticket base")
            if info["layer"] != layer:
                raise WsError(
                    "TICKET_FRAGMENT_LAYER_MISMATCH",
                    f"fragment {frag.ref!r}: {uid!r} is on layer"
                    f" {info['layer']!r}, region is on {layer!r}")
            self._check_label(layer, info["label"])
            return _with(frag, uid=uid, doc_id=self.doc_id, layer=layer,
                         label=info["label"], start=info["start"],
                         end=info["end"], seen=info["seen_text"])
        row = self.conn.execute(
            "SELECT * FROM base_proposal WHERE proposal_uid=?", (uid,)
        ).fetchone()
        if row is None:
            raise WsError(
                "TICKET_UNKNOWN_FRAGMENT",
                f"fragment {frag.ref!r}: unknown proposal {uid!r}")
        if row["doc_id"] != self.doc_id or row["layer"] != layer:
            raise WsError(
                "TICKET_FRAGMENT_LAYER_MISMATCH",
                f"fragment {frag.ref!r}: proposal is on doc {row['doc_id']}"
                f" layer {row['layer']}, region is {self.doc_id}/{layer}")
        target = fragment_uid(self.doc_id, layer, row["start_byte"],
                              row["end_byte"], row["label"])
        live = self.by_uid.get(target)
        if live is None:
            raise WsError(
                "TICKET_PROPOSAL_NOT_ADOPTED",
                f"fragment {frag.ref!r}: proposal {uid!r} is not the adopted"
                " verdict at this revision; choose a candidate or draw a"
                " manual span")
        self._check_label(layer, live["label"])
        return _with(frag, uid=target, doc_id=self.doc_id, layer=layer,
                     label=live["label"], start=live["start"],
                     end=live["end"], seen=live["seen_text"])

    def _check_label(self, layer: str, label: str) -> None:
        row = self.conn.execute(
            "SELECT labels_json FROM ws_layer WHERE layer=?", (layer,)
        ).fetchone()
        if row is None:
            raise WsError("UNKNOWN_LAYER", f"layer {layer!r} not declared")
        if label not in json.loads(row["labels_json"]):
            raise WsError(
                "UNKNOWN_LABEL",
                f"label {label!r} not allowed on layer {layer!r}")

    def resolve_endpoint(self, ref: str) -> dict:
        uid = self.by_ref.get(ref) or (ref if ref in self.by_uid else None)
        if uid is None:
            raise WsError(
                "TICKET_UNKNOWN_ENDPOINT",
                f"endpoint {ref!r} is neither a fragment ref in this ticket"
                " nor an adjudicated fragment at the ticket's base revision")
        return self.by_uid[uid]


def _check_overlaps(fragments) -> None:
    """同区域片段不得正长度重叠；相邻（首尾相碰）可并存。"""
    ordered = sorted(fragments, key=lambda f: (f.start, f.end, f.label))
    for prev, cur in zip(ordered, ordered[1:]):
        if cur.start < prev.end:
            raise WsError(
                "TICKET_FRAGMENT_OVERLAP",
                f"fragments {prev.ref!r}[{prev.start},{prev.end}) and"
                f" {cur.ref!r}[{cur.start},{cur.end}) overlap",
                {"region_fragments": [prev.ref, cur.ref]})


def _with(frag: ParsedFragment, **kw) -> ParsedFragment:
    data = {
        "ref": frag.ref, "kind": frag.kind, "local_uid": kw.get("uid", ""),
        "doc_id": kw.get("doc_id", ""), "layer": kw.get("layer", ""),
        "label": kw.get("label", frag.label), "start": kw.get("start", -1),
        "end": kw.get("end", -1), "seen_text": kw.get("seen", ""),
        "text_sha256": "", "evidence": frag.evidence,
    }
    import hashlib

    data["text_sha256"] = hashlib.sha256(
        data["seen_text"].encode("utf-8")).hexdigest()
    merged = dict(frag.evidence)
    merged["resolved_uid"] = data["local_uid"]
    data["evidence"] = merged
    return ParsedFragment(**data)


# ---------------------------------------------------------------------------
# 结构辅助
# ---------------------------------------------------------------------------

def _evidence(raw, loc) -> list:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise WsError("TICKET_FORMAT", "evidence must be a list", loc)
    out = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict) or "kind" not in item:
            raise WsError(
                "TICKET_FORMAT",
                "evidence item must be an object with 'kind'", loc)
        kind = item["kind"]
        if kind not in ("candidate", "record", "review"):
            raise WsError(
                "TICKET_FORMAT",
                f"evidence kind must be candidate/record/review, got {kind!r}",
                loc)
        pointer = {k: v for k, v in item.items() if k != "kind"}
        if not any(k in pointer for k in
                   ("candidate", "region", "record", "review", "uid")):
            raise WsError(
                "TICKET_FORMAT", "evidence item needs a pointer field", loc)
        out.append({"kind": kind, "pointer": pointer})
    return out


def _reject_unknown(obj: dict, allowed: set, where: str) -> None:
    extra = set(obj) - allowed
    if extra:
        raise WsError(
            "TICKET_FORMAT",
            f"{where} has unknown field(s): {sorted(extra)}",
            {"unknown": sorted(extra)})


def _list_field(data: dict, name: str) -> list:
    value = data.get(name, [])
    if value is None:
        return []
    if not isinstance(value, list):
        raise WsError("TICKET_FORMAT", f"ticket.{name} must be a list")
    if any(not isinstance(x, dict) for x in value):
        raise WsError("TICKET_FORMAT", f"ticket.{name} entries must be objects")
    return value


def _nonempty_str(value, name: str, loc: dict) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WsError("TICKET_FORMAT", f"{name} must be a non-empty string", loc)
    return value.strip()


def _int(value, name: str, loc: dict) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise WsError("TICKET_FORMAT", f"{name} must be an integer", loc)
    return value


def canonical_ticket_content(content: dict) -> str:
    return canonical_json(content)


def ticket_content_sha(content: dict) -> str:
    import hashlib

    return hashlib.sha256(canonical_ticket_content(content).encode("utf-8")).hexdigest()
