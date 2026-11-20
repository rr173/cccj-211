# -*- coding: utf-8 -*-
"""运行时高扇出引用生成器（产物不提交）。

构造一个工作区：单篇大公文上有大量已确认引用指向同一批片段；
随后提交一份撤除某个被大量引用的端点的工单。用于核对：

- 端点撤除时依赖检查不漏报（所有引用都命中）；
- 依赖检查只沿索引走当前文档，不扫描其他公文、不把整张关系网载入内存。

用法::

    python -I tools/fanoutgen.py --root /tmp/fanout --fanout 20000
"""
from __future__ import annotations

import argparse
import hashlib
import resource
import time
import json
import os
import shutil
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.build import build  # noqa: E402
from qc.policy import parse_policy  # noqa: E402
from qc.util import canonical_json, stable_id  # noqa: E402
from qc.ws_common import connect, resolve_revision, utc_now  # noqa: E402
from qc.ws_init import init_workspace  # noqa: E402
from qc.ws_merge import merge  # noqa: E402
from qc.ws_source import WorkspaceSources  # noqa: E402

POLICY = {
    "layers": {
        "clause": {"labels": ["L1"], "min_support": 2},
        "annex": {"labels": ["A1"], "min_support": 2},
    },
    "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2},
    },
}


def generate(root: str, *, fanout: int, doc_bytes: int = 400_000) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    os.makedirs(os.path.join(root, "packages"), exist_ok=True)
    alphabet = b"0123456789abcdefghijklmnopqrstuvwxyz"
    text = bytes(alphabet[i % len(alphabet)] for i in range(doc_bytes))
    with open(os.path.join(root, "sources", "big.txt"), "wb") as fh:
        fh.write(text)
    digest = hashlib.sha256(text).hexdigest()
    # 第二篇 unrelated 公文（用于确认依赖检查不跨文档扫描）
    other = b"unrelated-document-" * 10
    with open(os.path.join(root, "sources", "other.txt"), "wb") as fh:
        fh.write(other)
    other_digest = hashlib.sha256(other).hexdigest()
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"documents": [
            {"doc_id": "BIG", "path": "sources/big.txt", "sha256": digest},
            {"doc_id": "OTHER", "path": "sources/other.txt",
             "sha256": other_digest},
        ]}, fh)
    with open(os.path.join(root, "policy.json"), "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh)

    # 所有 annex 片段落在文档后半段，clause 片段在 [0,100)
    mid = doc_bytes // 2
    n_annex = max(2, min(50, fanout // 50))
    annex_spans = [
        (mid + i * 4, mid + i * 4 + 3) for i in range(n_annex)
    ]
    anchor = (0, 10)

    def write_pkg(team: str, records: list) -> None:
        path = os.path.join(root, "packages", f"{team}.jsonl")
        header = {
            "kind": "package", "package_id": f"PKG-{team}", "doc_id": "BIG",
            "team_id": team, "source_digest": digest,
        }
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(header, sort_keys=True) + "\n")
            for rec in records:
                fh.write(json.dumps(rec, sort_keys=True) + "\n")

    def frag(lid, layer, label, s, e):
        return {"kind": "fragment", "id": lid, "layer": layer, "label": label,
                "start": s, "end": e, "seen_text": text[s:e].decode()}

    # 两个团队共同提出 anchor 与全部 annex 片段 -> 自动胜出
    for team in ("v1", "v2"):
        records = [
            {"kind": "review", "layer": "clause", "ranges": [[0, 100]]},
            {"kind": "review", "layer": "annex",
             "ranges": [[sp[0], sp[1]] for sp in annex_spans]},
            frag("anchor", "clause", "L1", *anchor),
        ]
        for i, sp in enumerate(annex_spans):
            records.append(frag(f"a{i}", "annex", "A1", *sp))
        write_pkg(team, records)
    # 一个 unrelated 公文的小包（独立文件，各有一个包头）
    for team in ("o1", "o2"):
        with open(os.path.join(root, "packages", f"other-{team}.jsonl"), "w",
                  encoding="utf-8") as fh:
            fh.write(json.dumps({
                "kind": "package", "package_id": f"PKG-OTHER-{team}",
                "doc_id": "OTHER", "team_id": team,
                "source_digest": other_digest,
            }, sort_keys=True) + "\n")
            fh.write(json.dumps({"kind": "review", "layer": "clause",
                                 "ranges": [[0, 10]]}, sort_keys=True) + "\n")
            fh.write(json.dumps({
                "kind": "fragment", "id": "o", "layer": "clause",
                "label": "L1", "start": 0, "end": 10,
                "seen_text": other[0:10].decode(),
            }, sort_keys=True) + "\n")

    build(
        manifest_path=os.path.join(root, "manifest.json"),
        packages_root=os.path.join(root, "packages"),
        output_db=os.path.join(root, "warehouse.db"),
        policy=parse_policy(POLICY),
    )
    return {"annex_shape": {"spans": annex_spans, "anchor": anchor},
            "fanout": fanout, "doc_bytes": doc_bytes}


def _import_split_ticket(conn, ws, sources, anchor, clause_region):
    """以 anchor 基版导入拆分工单（此时它自洽，不涉及任何引用）。"""
    from qc.ws_import import import_ticket

    base = resolve_revision(conn, "HEAD")
    ticket = {
        "reviewer": "stress", "base": base["rev_id"],
        "regions": [{
            "region": clause_region, "decision": "fragments",
            "reason": "撤除被大量引用的端点",
            "fragments": [{"ref": "A", "start": 0, "end": 5, "label": "L1"},
                          {"ref": "B", "start": 5, "end": 10, "label": "L1"}],
        }],
    }
    result = import_ticket(conn, sources, ticket)
    return result["ticket_uid"], base["rev_id"]


def _inject_fanout(conn, ws, fanout, anchor, annex, base_rev):
    """在工单基版之后写入大量人工确认引用，全部以将被撤除的 anchor 为起点。

    每条引用的终点各不相同 -> N 个互不相同的业务键；对象键不在待合批
    工单里，只有合批时的端点依赖检查能发现它们会因 anchor 撤除而悬空。
    """
    from qc.util import canonical_json, stable_id
    from qc.ws_common import utc_now

    head = resolve_revision(conn, "HEAD")
    seq = head["seq"] + 1
    rev_id = stable_id("rev", seq, head["rev_id"], "fanout-injection", "x")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO revision(rev_id, seq, parent_rev, base_rev, ticket_uid,"
        " created_at, reason_json) VALUES (?,?,?,?,?,?,?)",
        (rev_id, seq, head["rev_id"], head["rev_id"], None, utc_now(),
         canonical_json({"kind": "fanout_injection"})))
    conn.execute("UPDATE ws_meta SET value=? WHERE key='head_rev'", (rev_id,))
    conn.execute("UPDATE ws_meta SET value=? WHERE key='head_seq'", (str(seq),))
    conn.executemany(
        "INSERT INTO fragment_state(fragment_uid, doc_id, layer, label,"
        " start_byte, end_byte, text_sha256, seen_text, origin, region_uid,"
        " seq_added, seq_removed, related_rev)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [("fan-dst-%06d" % i, "BIG", "annex", "A1", annex[0][0],
          annex[0][1], "0" * 64, "x", "manual", None, seq, None, rev_id)
         for i in range(fanout)])
    conn.executemany(
        "INSERT INTO relation_state(doc_id, rel_type, from_fragment,"
        " to_fragment, kind, origin, seq_added, seq_removed, related_rev)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        [("BIG", "ref", anchor, "fan-dst-%06d" % i, "present", "manual",
          seq, None, rev_id) for i in range(fanout)])
    # 只留一条 unrelated 公文的引用，用来确认依赖检查不跨文档扫描
    conn.execute(
        "INSERT INTO relation_state(doc_id, rel_type, from_fragment,"
        " to_fragment, kind, origin, seq_added, seq_removed, related_rev)"
        " VALUES ('OTHER', 'ref', 'frag-other-a', 'frag-other-b', 'present',"
        " 'manual', ?, NULL, ?)", (seq, rev_id))
    conn.execute("COMMIT")
    return seq


def run_fanout(root: str, fanout: int) -> dict:
    info = generate(root, fanout=fanout)
    ws = os.path.join(root, "ws")
    manifest = os.path.join(root, "manifest.json")
    init_workspace(ws, os.path.join(root, "warehouse.db"),
                   os.path.join(root, "policy.json"))
    conn = connect(ws)
    sources = WorkspaceSources(conn, manifest_path=manifest)
    anchor = conn.execute(
        "SELECT fragment_uid FROM fragment_state WHERE doc_id='BIG'"
        " AND layer='clause' LIMIT 1").fetchone()["fragment_uid"]
    annex = [tuple(r) for r in conn.execute(
        "SELECT start_byte, end_byte FROM fragment_state WHERE doc_id='BIG'"
        " AND layer='annex' ORDER BY start_byte LIMIT 1")]
    clause_region = conn.execute(
        "SELECT region_uid FROM base_region WHERE doc_id='BIG'"
        " AND layer='clause' AND start_byte=0 AND end_byte=10"
    ).fetchone()["region_uid"]
    ticket_uid, base_rev = _import_split_ticket(
        conn, ws, sources, anchor, clause_region)
    inject_seq = _inject_fanout(conn, ws, fanout, anchor, annex, base_rev)
    conn.close()

    conn = connect(ws)
    sources = WorkspaceSources(conn, manifest_path=manifest)
    t0 = time.perf_counter()
    result = merge(conn, sources, ws, ticket_uid=ticket_uid)
    elapsed = time.perf_counter() - t0
    conn.close()
    conflicts = result.get("conflicts", [])
    # 端点撤除引起的依赖以 endpoint 冲突呈现；dependency 冲突用于
    # 「已在工单里裁决但未退役」的情形。两类都算受影响的引用。
    affected = [c for c in conflicts
                if c.get("kind") in ("endpoint", "dependency")
                and c.get("subject_key")]
    cross_doc = [c for c in affected if '"OTHER"' in c["subject_key"]]
    return {
        "fanout": fanout,
        "status": result["status"],
        "conflicts_reported": len(conflicts),
        "affected_relations_reported": len(affected),
        "expected_relations": fanout,
        "missing_relations": fanout - len(affected),
        "cross_document_scanned": bool(cross_doc),
        "head_unchanged": result.get("result_rev") is None,
        "injected_at_seq": inject_seq,
        "merge_seconds": round(elapsed, 3),
        "peak_rss_kb": resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--fanout", type=int, default=20000)
    ap.add_argument("--doc-bytes", type=int, default=400_000)
    args = ap.parse_args()
    out = run_fanout(args.root, args.fanout)
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
