# -*- coding: utf-8 -*-
"""盲审可复跑小样本：用真实管道构建含多篇公文、正式片段与有向引用的发布。

生成内容（仓库只保留生成方式，不提交产物）：

- 六篇公文 D1…D6（互不相同的 UTF-8 原文，含多字节字符）；
- D1 有三条正式片段（两个 clause、一个 annex）与两条有向引用；
- D2/D4/D5 各有一条正式片段；D3 没有任何团队提名（不在任何发布中）；
- D6 只有一段未达 min_support 的孤立提议 → 构建为待定，不进入发布；
  发布 R1 点名 D1/D2/D4/D5/D6，D6 因待办被阻挡（演示发布边界）；
- 另发 R2 只点名 D1/D2/D4/D5，成功登记。

随后可用 ``qc-audit`` 在该发布上创建盲审批次。可用 ``--reverse`` 以相反的
文件/记录顺序重建同一构建库，用于核对样本不随输入排列漂移。

用法::

    python -I tools/audit_fixture.py --root /tmp/audit-demo
    python -I tools/audit_fixture.py --root /tmp/audit-demo --reverse
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
from qc.ws_common import connect  # noqa: E402
from qc.ws_init import init_workspace  # noqa: E402
from qc.ws_release import release  # noqa: E402

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

# 每篇公文 = ASCII 前缀（片段与审核区间都落在这里，兼容既有构建管道对
# 字符边界的判定）+ 中文尾部（盲审证据坐标演示多字节边界）。所有片段都在
# ASCII 区，因此构建阶段的坐标天然合法。
TEXT = {
    "D1": "AAAA0000 BBBB1111 CCCC2222 DDDD3333 EEEE4444 FFFF5555 "
          "甲一乙二丙三丁四戊五己六庚七辛八壬九癸十子丑寅卯辰巳午未",
    "D2": "DOC2-0000 DOC2-1111 DOC2-2222 DOC2-3333 "
          "档案数字化复制件与原件具有同等效力，任何单位不得擅自篡改。",
    "D3": "DOC3-0000 DOC3-1111 丁三公文仅一队提名，未达最少支持团队数。",
    "D4": "DOC4-0000 DOC4-1111 DOC4-2222 "
          "戊四公文的两家团队一致确认了附件条款，并同意引用附件甲。",
    "D5": "DOC5-0000 DOC5-1111 DOC5-2222 DOC5-3333 DOC5-4444 "
          "己五公文经复核确认存在正式片段，并有一条指向附件的有向引用。",
    "D6": "DOC6-0000 DOC6-1111 庚六公文只有一段孤立的提议，没有第二个团队附和。",
}


# 片段定义（字节区间，全部落在 ASCII 前缀内）：(local_id, layer, label, start, end)
FRAGMENTS = {
    "D1": [("x1", "clause", "L1", 0, 8),
           ("x2", "clause", "L1", 10, 18),
           ("y", "annex", "A1", 20, 28)],
    "D2": [("p", "clause", "L1", 0, 9)],
    "D4": [("q", "clause", "L2", 0, 9)],
    "D5": [("r", "clause", "L1", 0, 9),
           ("s", "annex", "A1", 20, 28)],
}
# 有向引用：(doc, type, from_local, to_local)
RELATIONS = {
    "D1": [("ref", "x1", "y"), ("ref", "x2", "y")],
    "D5": [("ref", "r", "s")],
}


def _text_bytes(doc_id: str) -> bytes:
    return TEXT[doc_id].encode("utf-8")


def _review_ranges(doc_id: str) -> list:
    """覆盖每个片段所在层的完整审核区间。"""
    out = {}
    for _lid, layer, _label, start, end in FRAGMENTS.get(doc_id, []):
        out.setdefault(layer, [])
        out[layer].append([start, end])
    return [[layer, out[layer]] for layer in sorted(out)]


def _write_jsonl(path, header, records) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(header, ensure_ascii=False, sort_keys=True) + "\n")
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")


def generate(root: str, *, reverse: bool = False) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    pkg_root = os.path.join(root, "packages")
    os.makedirs(pkg_root, exist_ok=True)
    documents = []
    for doc_id in sorted(TEXT):
        data = _text_bytes(doc_id)
        rel_path = f"sources/{doc_id.lower()}.txt"
        with open(os.path.join(root, rel_path), "wb") as fh:
            fh.write(data)
        documents.append({"doc_id": doc_id, "path": rel_path,
                          "sha256": hashlib.sha256(data).hexdigest()})
    with open(os.path.join(root, "manifest.json"), "w",
              encoding="utf-8") as fh:
        json.dump({"documents": documents}, fh, ensure_ascii=False, indent=2,
                  sort_keys=True)
    with open(os.path.join(root, "policy.json"), "w",
              encoding="utf-8") as fh:
        json.dump(POLICY, fh, ensure_ascii=False, indent=2, sort_keys=True)
    digests = {d["doc_id"]: d["sha256"] for d in documents}

    def header(team, doc_id):
        return {"kind": "package", "package_id": f"PKG-{doc_id}-{team}",
                "doc_id": doc_id, "team_id": team,
                "source_digest": digests[doc_id]}

    def fragment_record(doc_id, local_id, layer, label, start, end):
        raw = _text_bytes(doc_id)
        if not (0 <= start < end <= len(raw)):
            raise ValueError(f"bad span {doc_id}[{start},{end})")
        text = raw[start:end].decode("utf-8")
        return {"kind": "fragment", "id": local_id, "layer": layer,
                "label": label, "start": start, "end": end,
                "seen_text": text}

    def package_records(doc_id, team):
        recs = []
        reviews = _review_ranges(doc_id)
        for layer, ranges in reviews:
            recs.append({"kind": "review", "layer": layer, "ranges": ranges})
        for (_t, frm, to) in RELATIONS.get(doc_id, []):
            recs.append({"kind": "relation", "type": "ref", "from_id": frm,
                         "to_id": to, "sign": "for"})
        for frag in FRAGMENTS.get(doc_id, []):
            recs.append(fragment_record(doc_id, *frag))
        return recs

    # 每篇正式片段/引用都由两个团队一致提出；D6 只有一个团队提议 -> 待定
    plan = []
    for doc_id in sorted(FRAGMENTS):
        plan.append((doc_id, "tA"))
        plan.append((doc_id, "tB"))
    if reverse:
        plan.reverse()
        # 每包内部记录也逆序，验证记录物理顺序不影响结果
    for index, (doc_id, team) in enumerate(plan):
        records = package_records(doc_id, team)
        if reverse:
            records.reverse()
        _write_jsonl(os.path.join(pkg_root, f"{index:03d}-{doc_id}-{team}.jsonl"),
                     header(team, doc_id), records)
    # D3 与 D6 的孤立提议（各只有一个团队）
    _write_jsonl(os.path.join(pkg_root, "900-D3-solo.jsonl"),
                 header("tSolo", "D3"), [
                     {"kind": "review", "layer": "clause",
                      "ranges": [[0, 9]]},
                     fragment_record("D3", "s", "clause", "L1", 0, 9),
                 ])
    _write_jsonl(os.path.join(pkg_root, "901-D6-solo.jsonl"),
                 header("tSolo", "D6"), [
                     {"kind": "review", "layer": "clause",
                      "ranges": [[0, 9]]},
                     fragment_record("D6", "s", "clause", "L1", 0, 9),
                 ])

    db = os.path.join(root, "warehouse.db")
    result = build(
        manifest_path=os.path.join(root, "manifest.json"),
        packages_root=pkg_root,
        output_db=db,
        policy=parse_policy(POLICY),
    )
    if result.errors:
        raise SystemExit([e.to_dict() for e in result.errors[:5]])

    ws = os.path.join(root, "ws")
    init_workspace(ws, db, os.path.join(root, "policy.json"))
    conn = connect(ws)
    try:
        # R-blocked 点名 D3/D6：D3 没有待办也不在发布快照中，D6 待定 -> 阻挡
        blocked = release(conn, ws, rev="HEAD",
                          doc_ids=["D1", "D3", "D6"])
        r1 = release(conn, ws, rev="HEAD",
                     doc_ids=["D1", "D2", "D4", "D5"])
    finally:
        conn.close()
    return {
        "root": root,
        "workspace": ws,
        "warehouse": db,
        "manifest": os.path.join(root, "manifest.json"),
        "policy": os.path.join(root, "policy.json"),
        "documents": documents,
        "release": r1,
        "blocked_release": blocked,
        "reverse": reverse,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--reverse", action="store_true",
                    help="以相反的文件/记录顺序重建，用于样本稳定性核对")
    args = ap.parse_args()
    info = generate(args.root, reverse=args.reverse)
    print(json.dumps(info, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
