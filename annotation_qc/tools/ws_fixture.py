# -*- coding: utf-8 -*-
"""可重建的小样本：裁决工作区的端到端场景。

生成一个构建库（旧片段 X、另一层 Y、已确认的 X→Y、一条被阻断的原始关系），
并写出甲/乙两份工单。仓库只保留这个生成方式与说明，不保留产物。

用法::

    python -I tools/ws_fixture.py --root /tmp/wsample
    python -I tools/ws_fixture.py --root /tmp/wsample --build
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.build import build  # noqa: E402
from qc.policy import parse_policy  # noqa: E402

TEXT = (
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "!@#$%^&*()-_=+[]{};:'\",.<>/?\\|`~"
)
POLICY = {
    "layers": {
        "clause": {"labels": ["L1", "L2"], "min_support": 2},
        "annex": {"labels": ["A1"], "min_support": 2},
    },
    "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2},
        "reverse_ref": {"from_layer": "annex", "to_layer": "clause",
                        "min_support": 2},
    },
}
DOCS = {
    "D1": {"path": "sources/d1.txt", "text": TEXT.encode("utf-8")},
    "D2": {"path": "sources/d2.txt", "text": b"HELLOWORLD" * 5},
}


def _write_jsonl(path, header, records):
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(header, ensure_ascii=False, sort_keys=True) + "\n")
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")


def generate(root: str) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    os.makedirs(os.path.join(root, "packages"), exist_ok=True)
    documents = []
    for doc_id, spec in DOCS.items():
        abs_path = os.path.join(root, spec["path"])
        with open(abs_path, "wb") as fh:
            fh.write(spec["text"])
        documents.append({
            "doc_id": doc_id, "path": spec["path"],
            "sha256": hashlib.sha256(spec["text"]).hexdigest(),
        })
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"documents": documents}, fh, ensure_ascii=False, indent=2,
                  sort_keys=True)
    with open(os.path.join(root, "policy.json"), "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh, ensure_ascii=False, indent=2, sort_keys=True)
    digests = {d["doc_id"]: d["sha256"] for d in documents}
    text1 = DOCS["D1"]["text"]

    def hdr(team, doc="D1"):
        return {"kind": "package", "package_id": f"PKG-{doc}-{team}",
                "doc_id": doc, "team_id": team,
                "source_digest": digests[doc]}

    def frag(lid, layer, label, s, e, doc="D1"):
        text = DOCS[doc]["text"]
        return {"kind": "fragment", "id": lid, "layer": layer, "label": label,
                "start": s, "end": e, "seen_text": text[s:e].decode()}

    def review(layer, ranges):
        return {"kind": "review", "layer": layer, "ranges": ranges}

    def relation(t, f, to, sign="for"):
        return {"kind": "relation", "type": t, "from_id": f, "to_id": to,
                "sign": sign}

    pkg = os.path.join(root, "packages")
    # X=[0,10) L1 与 Y=[20,26) A1 各两票胜出；X->Y 两票确认
    _write_jsonl(os.path.join(pkg, "d1-a.jsonl"), hdr("tA"), [
        review("clause", [[0, 10]]), review("annex", [[20, 26]]),
        relation("ref", "x", "y"),
        frag("x", "clause", "L1", 0, 10),
        frag("y", "annex", "A1", 20, 26),
    ])
    _write_jsonl(os.path.join(pkg, "d1-b.jsonl"), hdr("tB"), [
        review("clause", [[0, 10]]), review("annex", [[20, 26]]),
        frag("x", "clause", "L1", 0, 10),
        frag("y", "annex", "A1", 20, 26),
        relation("ref", "x", "y"),
    ])
    # 第三队提出同区另一标签 -> 落败；其 X'->Y 关系被阻断
    _write_jsonl(os.path.join(pkg, "d1-c.jsonl"), hdr("tC"), [
        review("clause", [[0, 10]]), review("annex", [[20, 26]]),
        frag("c1", "clause", "L2", 0, 10),
        frag("cy", "annex", "A1", 20, 26),
        relation("ref", "c1", "cy"),
    ])
    # D2 独立
    for team in ("uA", "uB"):
        _write_jsonl(os.path.join(pkg, f"d2-{team}.jsonl"), hdr(team, "D2"), [
            review("clause", [[0, 10]]),
            frag("p", "clause", "L1", 0, 10, doc="D2"),
        ])

    db = os.path.join(root, "warehouse.db")
    result = build(
        manifest_path=os.path.join(root, "manifest.json"),
        packages_root=pkg,
        output_db=db,
        policy=parse_policy(POLICY),
    )
    if result.errors:
        raise SystemExit([e.to_dict() for e in result.errors[:5]])

    # 工单模板：真实 uid 在初始化工作区后由 --write-tickets 填入
    info = {
        "root": root,
        "warehouse": db,
        "manifest": os.path.join(root, "manifest.json"),
        "policy": os.path.join(root, "policy.json"),
        "documents": documents,
    }
    return info


def write_tickets(workspace: str, out_dir: str) -> dict:
    """按工作区当前 HEAD 写出甲（拆分+退役）与乙（新增反向引用）工单。"""
    from qc.ws_common import connect, resolve_revision
    from qc.ws_view import view_revision

    conn = connect(workspace)
    try:
        base = resolve_revision(conn, "HEAD")["rev_id"]
        view = view_revision(conn, "HEAD", doc_id="D1")
        regions = {r["layer"]: r["region_uid"]
                   for r in view["documents"]["D1"]["regions"]}
        frags = {(f["layer"], f["start"]): f["fragment_uid"]
                 for f in view["documents"]["D1"]["fragments"]}
        blocked = [p["subject_key"] for p in view["pending"]["blocked"]]
    finally:
        conn.close()
    x, y = frags[("clause", 0)], frags[("annex", 20)]
    os.makedirs(out_dir, exist_ok=True)
    alice = {
        "reviewer": "alice", "title": "拆分 X 并退役 X->Y", "base": base,
        "regions": [{
            "region": regions["clause"], "decision": "fragments",
            "reason": "X 拆为 X1/X2",
            "fragments": [
                {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
                {"ref": "X2", "start": 4, "end": 10, "label": "L1"},
            ],
        }],
        "relations": [{
            "doc": "D1", "type": "ref", "from": x, "to": y,
            "decision": "retired", "reason": "X 拆分后旧边失效",
        }],
    }
    bob = {
        "reviewer": "bob", "title": "新增 Y->X", "base": base,
        "relations": [{
            "doc": "D1", "type": "reverse_ref", "from": y, "to": x,
            "decision": "present", "reason": "新增反向引用",
        }],
    }
    for name, data in (("alice.json", alice), ("bob.json", bob)):
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=True)
    return {"base_rev": base, "x": x, "y": y, "blocked_record": blocked[0],
            "tickets": [os.path.join(out_dir, n) for n in
                        ("alice.json", "bob.json")]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--workspace", default=None,
                    help="初始化后写工单的工作区（可选）")
    ap.add_argument("--tickets-dir", default=None)
    args = ap.parse_args()
    info = generate(args.root)
    if args.workspace:
        from qc.ws_init import init_workspace

        ws = args.workspace
        shutil.rmtree(ws, ignore_errors=True)
        init_workspace(ws, info["warehouse"], info["policy"])
        info["tickets"] = write_tickets(
            ws, args.tickets_dir or os.path.join(args.root, "tickets"))
    print(json.dumps(info, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
