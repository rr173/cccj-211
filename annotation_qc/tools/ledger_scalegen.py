# -*- coding: utf-8 -*-
"""规模生成与更新工作量实测（产物写到临时目录，不提交）。

生成一个合成上游：大量互不相干的小组（两篇共享一条组内条款）+
一个共享同一条款的大组，然后：

1. 建账并分批选入全部公文；
2. 一次只影响一个小组的变更（登记一条组内人工证据）；
3. 一次连入大组的变更（新选一篇复用大组条款的公文）；
4. 全库重建对照。

每步打印诊断（读取的发布/公文、访问的摘要/证据行、重算分量与成员数、
耗时），用于区分局部计算与全库重建。

用法::

    python -I tools/ledger_scalegen.py --root /tmp/lscale \
        --groups 1500 --large-group 2000
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.ledger import Ledger, init_ledger  # noqa: E402
from tools.ledger_fixture import write_synthetic_workspace  # noqa: E402

BIG_CLAUSE = "big-shared-clause-" + "x" * 64


def generate(root: str, groups: int, large_group: int) -> dict:
    """生成合成上游：groups 个两人小组 + 一个 large_group 篇的大组。"""
    ws = os.path.join(root, "ws")
    docs = {}
    release_docs = {}

    def add(doc_id, fragments):
        docs[doc_id] = f"draft-{doc_id}"
        release_docs[doc_id] = [
            {"uid": f"{doc_id}-u", "layer": "clause",
             "text": f"unique-fragment-{doc_id}-" + "y" * 32},
            *fragments,
        ]

    for i in range(groups):
        clause = f"small-group-clause-{i}-" + "z" * 32
        add(f"g{i}-a", [{"uid": f"g{i}-a-c", "layer": "clause", "text": clause}])
        add(f"g{i}-b", [{"uid": f"g{i}-b-c", "layer": "clause", "text": clause}])
    for j in range(large_group):
        add(f"L{j}", [{"uid": f"L{j}-c", "layer": "clause", "text": BIG_CLAUSE}])
    add("L-new", [{"uid": "L-new-c", "layer": "clause", "text": BIG_CLAUSE}])

    write_synthetic_workspace(ws, docs, [
        {"release_id": "RS", "rev_id": "rev-scale", "rev_seq": 1,
         "docs": release_docs},
    ])
    return {"workspace": ws, "groups": groups, "large_group": large_group,
            "doc_count": len(docs)}


def _show(title, diag):
    print(f"\n[{title}] mode={diag['mode']}"
          f" elapsed_ms={diag['elapsed_ms']}")
    print(f"  读取发布: {diag['upstream_releases_read']}"
          f"  读取公文数: {len(diag['upstream_docs_read'])}")
    print(f"  访问摘要行: {diag['ledger_digest_rows_scanned']}"
          f"  访问证据行: {diag['ledger_evidence_rows_scanned']}")
    print(f"  受影响旧分量: {len(diag['components_touched'])}"
          f"  重算分量: {len(diag['components_recomputed'])}"
          f"  重算成员: {diag['members_recomputed']}")


def measure(root: str, groups: int, large_group: int) -> None:
    info = generate(root, groups, large_group)
    ledger_dir = os.path.join(root, "ledger")
    init_ledger(ledger_dir, info["workspace"], layer="clause",
                min_segment_bytes=8, exempt_digests=[])
    print(f"上游: {info['doc_count']} 篇公文 = {groups} 个两人小组"
          f" + 1 个 {large_group} 篇大组（共享同一条款）")
    lg = Ledger(ledger_dir)
    try:
        # 分批选入（每批 1000 篇）
        pending = [f"g{i}-a" for i in range(groups)] + \
                  [f"g{i}-b" for i in range(groups)] + \
                  [f"L{j}" for j in range(large_group)]
        batch = 1000
        t_ms = 0
        for k in range(0, len(pending), batch):
            part = pending[k:k + batch]
            rec = lg.apply_change({
                "request_id": f"load-{k}", "base_version": lg.version,
                "select": [{"doc_id": d, "release_id": "RS"} for d in part]})
            t_ms += rec["diagnostics"]["elapsed_ms"]
        st = lg.status()
        print(f"选入完成: {st['docs_selected']} 篇, {st['components']} 个分量,"
              f" 累计 {t_ms:.0f} ms")

        rec = lg.apply_change({
            "request_id": "op-small", "base_version": lg.version,
            "evidence_register": [{
                "evidence_id": "E-small", "doc_a": "g0-a", "doc_b": "g0-b",
                "provenance": "组内人工核实"}]})
        _show("局部变更：一个小组内登记人工证据", rec["diagnostics"])

        rec = lg.apply_change({
            "request_id": "op-large", "base_version": lg.version,
            "select": [{"doc_id": "L-new", "release_id": "RS"}]})
        _show("局部变更：新公文连入大组", rec["diagnostics"])

        rec = lg.apply_change({
            "request_id": "op-small-2", "base_version": lg.version,
            "evidence_revoke": ["E-small"]})
        _show("局部变更：撤销小组证据（分量分裂）", rec["diagnostics"])

        result = lg.rebuild_all()
        _show("全库重建（对照）", result["diagnostics"])

        verify = lg.verify()
        print(f"\n增量维护与全量重算一致: {verify['consistent']}")
    finally:
        lg.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--groups", type=int, default=1500)
    ap.add_argument("--large-group", type=int, default=2000)
    args = ap.parse_args()
    if os.path.isdir(args.root):
        shutil.rmtree(args.root)
    os.makedirs(args.root, exist_ok=True)
    measure(args.root, args.groups, args.large_group)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
