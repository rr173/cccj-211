# -*- coding: utf-8 -*-
"""分仓台账演示夹具：用真实管道构建裁决工作区小样本。

仓库只保留本生成方法，不保留产物。生成内容：

- 九篇公文（档-甲 … 档-壬），覆盖：共享非豁免条款、只共享豁免落款、
  只共享不足最短字数的片段、底稿相同但无片段、改选后不含旧条款等情形；
- 发布 R1（rev0，全部九篇）与发布 R2（rev1，档-甲/档-丙，旧条款已废止），
  其中 R2 通过真实工单合批产生，当前审定稿（HEAD）与 R1 发布内容不同，
  可检验「不拿当前审定稿代替发布快照」。

用法::

    python -I tools/ledger_fixture.py --root /tmp/ldemo
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.build import build  # noqa: E402
from qc.policy import parse_policy  # noqa: E402

POLICY = {
    "layers": {
        "clause": {"labels": ["L1", "L2"], "min_support": 2},
        "annex": {"labels": ["A1"], "min_support": 2},
    },
    "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2},
    },
}

# 共享/专用文本部件 -----------------------------------------------------------
CLAUSE_OLD = "第十七条 馆藏档案数字化复制件与原件具有同等效力，任何单位和个人不得擅自篡改、隐匿、损毁。"
SIGNOFF = "此致敬礼，某某市档案局办公室。"          # 固定落款：建账时列入豁免表
SHORT = "特此通知。"                               # 15 字节 < 约定最短 16 字节
U_JIA = "甲单位成立于一九五三年，承担市级综合档案管理职责。"
U_YI1 = "乙单位即日起开放馆藏民国档案一批，供社会公众查阅利用。"
U_YI2 = "乙单位查阅大厅每周一至周五对外开放，节假日除外。"
U_BING = "丙单位拟于明年启动馆藏档案数字化项目，预算另行报批。"
U_WU = "戊单位本年度共接收各门类档案一千余卷，已全部登记入库。"
U_JI = "己单位本年度向市档案馆移交档案八百余卷，交接手续齐备。"
U_REN = "壬单位档案库房改扩建工程已于上月通过竣工验收。"
T_LIANHE = "庚辛两单位关于开展联合执法档案整理工作的通知底稿。"


def bin_of(doc_id: str) -> int:
    return hashlib.sha256(doc_id.encode("utf-8")).digest()[0] & 1


def pick_id(base: str, want: int) -> str:
    """确定性选择满足分仓奇偶要求的档号（无历史仓位组的哈希分仓）。"""
    i = 0
    while True:
        cand = base if i == 0 else f"{base}-{i}"
        if bin_of(cand) == want:
            return cand
        i += 1


def _write_jsonl(path, header, records):
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(header, ensure_ascii=False, sort_keys=True) + "\n")
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")


def generate(root: str) -> dict:
    """生成上游小样本（构建库 + 裁决工作区 + R1/R2 两次发布）。"""
    from qc.ws_common import connect, resolve_revision
    from qc.ws_init import init_workspace
    from qc.ws_merge import merge
    from qc.ws_release import release
    from qc.ws_source import WorkspaceSources

    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    os.makedirs(os.path.join(root, "packages"), exist_ok=True)

    docs = {
        "甲": pick_id("档-甲", 0),   # 无历史仓位时进训练仓（首字节偶）
        "乙": pick_id("档-乙", 1),   # 无历史仓位时进评测仓（首字节奇）
        "丙": "档-丙", "丁": "档-丁", "戊": "档-戊",
        "己": "档-己", "庚": "档-庚", "辛": "档-辛", "壬": "档-壬",
    }
    # 每篇公文的片段序列（层, 文本）；庚/辛无片段但底稿相同
    parts = {
        docs["甲"]: [("clause", U_JIA), ("clause", CLAUSE_OLD),
                     ("clause", SIGNOFF), ("annex", U_YI1)],
        docs["乙"]: [("clause", U_YI1), ("clause", U_YI2),
                     ("clause", SIGNOFF)],
        docs["丙"]: [("clause", U_BING), ("clause", CLAUSE_OLD),
                     ("clause", SIGNOFF)],
        docs["丁"]: [("clause", CLAUSE_OLD), ("clause", SIGNOFF)],
        docs["戊"]: [("clause", U_WU), ("clause", SIGNOFF),
                     ("clause", SHORT)],
        docs["己"]: [("clause", U_JI), ("clause", SIGNOFF),
                     ("clause", SHORT)],
        docs["庚"]: [],
        docs["辛"]: [],
        docs["壬"]: [("clause", U_REN), ("clause", SIGNOFF)],
    }
    texts = {
        docs["甲"]: "甲单位关于档案数字化工作的请示（第一稿）\n",
        docs["乙"]: "乙单位关于开放馆藏档案的公告\n",
        docs["丙"]: "丙单位关于档案数字化项目立项的函\n",
        docs["丁"]: "丁单位档案数字化协作协议\n",
        docs["戊"]: "戊单位年度档案工作报告\n",
        docs["己"]: "己单位年度档案移交报告\n",
        docs["庚"]: T_LIANHE,
        docs["辛"]: T_LIANHE,
        docs["壬"]: "壬单位库房改扩建工程通报\n",
    }
    # 片段文本拼进原文（正文 + 部件，部件间以换行分隔），记录字节坐标。
    # 注：换行分隔保证每个片段的起止都落在 ASCII 字节旁，即上游
    # check_span 认可的 UTF-8 字符边界（多字节字符紧邻的坐标会被其拒绝）。
    documents = []
    frag_specs = {}
    for doc_id, base_text in texts.items():
        body = base_text
        frags = []
        for layer, text in parts[doc_id]:
            start = len(body.encode("utf-8"))
            body += text
            frags.append({"layer": layer, "text": text,
                          "start": start,
                          "end": len(body.encode("utf-8"))})
            body += "\n"
        rel = f"sources/{doc_id}.txt"
        with open(os.path.join(root, rel), "w", encoding="utf-8",
                  newline="\n") as fh:
            fh.write(body)
        documents.append({
            "doc_id": doc_id, "path": rel,
            "sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        })
        frag_specs[doc_id] = frags
    manifest_path = os.path.join(root, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump({"documents": documents}, fh, ensure_ascii=False, indent=2,
                  sort_keys=True)
    policy_path = os.path.join(root, "policy.json")
    with open(policy_path, "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh, ensure_ascii=False, indent=2, sort_keys=True)

    # 标注包：两篇团队重复提交相同片段（min_support=2 胜出）
    digests = {d["doc_id"]: d["sha256"] for d in documents}
    pkg_dir = os.path.join(root, "packages")
    for doc_id, frags in frag_specs.items():
        if not frags:
            continue  # 庚/辛：无片段，只有底稿
        layers = sorted({f["layer"] for f in frags})
        for team in ("tA", "tB"):
            header = {"kind": "package", "package_id": f"PKG-{doc_id}-{team}",
                      "doc_id": doc_id, "team_id": team,
                      "source_digest": digests[doc_id]}
            records = []
            for layer in layers:
                spans = [[f["start"], f["end"]] for f in frags
                         if f["layer"] == layer]
                records.append({"kind": "review", "layer": layer,
                                "ranges": spans})
            for i, f in enumerate(frags):
                records.append({
                    "kind": "fragment", "id": f"f{i}", "layer": f["layer"],
                    "label": "L1" if f["layer"] == "clause" else "A1",
                    "start": f["start"], "end": f["end"],
                    "seen_text": f["text"]})
            _write_jsonl(os.path.join(pkg_dir, f"{doc_id}-{team}.jsonl"),
                         header, records)

    warehouse = os.path.join(root, "warehouse.db")
    result = build(manifest_path=manifest_path, packages_root=pkg_dir,
                   output_db=warehouse, policy=parse_policy(POLICY))
    if result.errors:
        raise SystemExit([e.to_dict() for e in result.errors[:5]])

    workspace = os.path.join(root, "ws")
    shutil.rmtree(workspace, ignore_errors=True)
    init_workspace(workspace, warehouse, policy_path)

    conn = connect(workspace)
    try:
        r1 = release(conn, workspace, rev="HEAD", doc_ids=list(texts))
        assert r1["released"], r1

        # 工单：废止 甲/丙 的旧条款区域（产生 rev1，使 HEAD 与 R1 不同）
        def clause_region(doc_id, text):
            start = next(f["start"] for f in frag_specs[doc_id]
                         if f["layer"] == "clause" and f["text"] == text)
            row = conn.execute(
                "SELECT region_uid FROM base_region"
                " WHERE doc_id=? AND layer='clause' AND start_byte=?",
                (doc_id, start)).fetchone()
            assert row is not None, (doc_id, start)
            return row["region_uid"]

        head = resolve_revision(conn, "HEAD")["rev_id"]
        ticket = {
            "reviewer": "carol", "title": "废止旧条款", "base": head,
            "regions": [
                {"region": clause_region(docs["甲"], CLAUSE_OLD),
                 "decision": "none", "reason": "旧条款已废止"},
                {"region": clause_region(docs["丙"], CLAUSE_OLD),
                 "decision": "none", "reason": "旧条款已废止"},
            ],
        }
        sources = WorkspaceSources(conn, manifest_path=manifest_path)
        merged = merge(conn, sources, workspace, data=ticket)
        assert merged["status"] == "merged", merged
        r2 = release(conn, workspace, rev="HEAD",
                     doc_ids=[docs["甲"], docs["丙"]])
        assert r2["released"], r2
    finally:
        conn.close()

    info = {
        "root": root,
        "workspace": workspace,
        "warehouse": warehouse,
        "manifest": manifest_path,
        "releases": {"R1": r1["release_id"], "R2": r2["release_id"]},
        "docs": docs,
        "texts": {
            "CLAUSE_OLD": CLAUSE_OLD, "SIGNOFF": SIGNOFF, "SHORT": SHORT,
        },
        "digests": {
            "clause_old": hashlib.sha256(CLAUSE_OLD.encode("utf-8")).hexdigest(),
            "signoff": hashlib.sha256(SIGNOFF.encode("utf-8")).hexdigest(),
        },
        "exempt": [hashlib.sha256(SIGNOFF.encode("utf-8")).hexdigest()],
        "whole_digests": {d["doc_id"]: d["sha256"] for d in documents},
    }
    with open(os.path.join(root, "info.json"), "w", encoding="utf-8") as fh:
        json.dump(info, fh, ensure_ascii=False, indent=2, sort_keys=True)
    return info


def write_synthetic_workspace(workspace: str, docs: dict,
                              releases: list) -> None:
    """直接写最小上游库（仅台账读取的三张表），供单元测试/规模生成使用。

    docs: ``{doc_id: 底稿文本}``
    releases: ``[{"release_id", "rev_id", "rev_seq",
                  "docs": {doc_id: [{"uid", "layer", "text"}]}}]``
    """
    os.makedirs(os.path.abspath(workspace), exist_ok=True)
    path = os.path.join(os.path.abspath(workspace), "workspace.db")
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE source_doc (
                doc_id TEXT PRIMARY KEY, rel_path TEXT, abs_path TEXT,
                sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL,
                verified_sha TEXT, verified INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE release (
                release_id TEXT PRIMARY KEY, rev_id TEXT NOT NULL,
                rev_seq INTEGER NOT NULL, created_at TEXT NOT NULL,
                doc_ids_json TEXT NOT NULL, request_json TEXT NOT NULL);
            CREATE TABLE release_fragment (
                release_id TEXT NOT NULL, fragment_uid TEXT NOT NULL,
                doc_id TEXT NOT NULL, layer TEXT NOT NULL,
                label TEXT NOT NULL, start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL, text_sha256 TEXT NOT NULL,
                seen_text TEXT NOT NULL, origin TEXT NOT NULL,
                source_kind TEXT NOT NULL, source_rev TEXT NOT NULL,
                ticket_uid TEXT, reviewer_id TEXT, reason TEXT,
                evidence_json TEXT NOT NULL,
                PRIMARY KEY (release_id, fragment_uid));
            """)
        for doc_id, text in docs.items():
            data = text.encode("utf-8")
            conn.execute(
                "INSERT INTO source_doc(doc_id, sha256, byte_length)"
                " VALUES (?,?,?)",
                (doc_id, hashlib.sha256(data).hexdigest(), len(data)))
        for rel in releases:
            doc_ids = sorted(rel["docs"])
            conn.execute(
                "INSERT INTO release(release_id, rev_id, rev_seq, created_at,"
                " doc_ids_json, request_json) VALUES (?,?,?,?,?,?)",
                (rel["release_id"], rel["rev_id"], rel["rev_seq"],
                 "2026-01-01T00:00:00Z",
                 json.dumps(doc_ids, ensure_ascii=False), "{}"))
            for doc_id, frags in rel["docs"].items():
                for f in frags:
                    conn.execute(
                        "INSERT INTO release_fragment(release_id, fragment_uid,"
                        " doc_id, layer, label, start_byte, end_byte,"
                        " text_sha256, seen_text, origin, source_kind,"
                        " source_rev, evidence_json)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (rel["release_id"], f["uid"], doc_id, f["layer"],
                         "L1", 0, len(f["text"].encode("utf-8")),
                         hashlib.sha256(f["text"].encode("utf-8")).hexdigest(),
                         f["text"], "auto", "auto", rel["rev_id"], "[]"))
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    args = ap.parse_args()
    info = generate(args.root)
    print(json.dumps(info, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
