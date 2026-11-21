# -*- coding: utf-8 -*-
"""不依赖产品实现的参考抽样：独立复算选中公文集合。

只用标准库、按需求文字重写一遍规则：

1. 抽样总体 = 某发布中包含至少一条正式片段或正式引用的不同公文；
   直接读上游 SQLite 的 ``release_fragment`` / ``release_relation``（不调用
   产品的抽样/批次代码）；
2. 每篇公文分值 = ``sha256(DOMAIN + b"\\0" + str(seed) + b"\\0" + 档号)``；
3. 按 ``(分值, 档号 UTF-8 字节)`` 升序取前 k 篇，不放回；
4. 名单按档号字节序输出，便于与产品批次对照。

用法::

    python -I tools/audit_reference.py --workspace WS --release REL \
        --sample-size 3 --seed 20261121
    python -I tools/audit_reference.py --workspace WS --release REL \
        --batch B --sample-size 3 --seed 20261121   # 与批次对照
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3

DOMAIN = b"annotation_qc.publish_audit.sample.v1"


def score(seed: int, doc_id: str) -> bytes:
    h = hashlib.sha256()
    h.update(DOMAIN)
    h.update(b"\x00")
    h.update(str(seed).encode("ascii"))
    h.update(b"\x00")
    h.update(doc_id.encode("utf-8"))
    return h.digest()


def population(workspace: str, release_id: str) -> list[str]:
    path = os.path.join(os.path.abspath(workspace), "workspace.db")
    conn = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    try:
        docs = {
            r[0] for r in conn.execute(
                "SELECT doc_id FROM release_fragment WHERE release_id=?"
                " UNION SELECT doc_id FROM release_relation"
                " WHERE release_id=?", (release_id, release_id))
        }
    finally:
        conn.close()
    return sorted(docs, key=lambda d: d.encode("utf-8"))


def sample(workspace: str, release_id: str, sample_size: int,
           seed: int) -> dict:
    pop = population(workspace, release_id)
    if sample_size > len(pop):
        raise SystemExit(json.dumps({
            "ok": False, "error": "SAMPLE_TOO_LARGE",
            "population_size": len(pop), "sample_size": sample_size}))
    ranked = sorted(pop, key=lambda d: (score(seed, d), d.encode("utf-8")))
    chosen = sorted(ranked[:sample_size], key=lambda d: d.encode("utf-8"))
    return {
        "release_id": release_id,
        "sample_size": sample_size,
        "seed": seed,
        "population_size": len(pop),
        "population": pop,
        "sample": chosen,
        "ranked": ranked,
    }


def batch_sample(audit_dir: str, batch_id: str) -> list[str]:
    path = os.path.join(os.path.abspath(audit_dir), "audit.db")
    conn = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    try:
        return [
            r[0] for r in conn.execute(
                "SELECT doc_id FROM aud_sample_doc WHERE batch_id=?"
                " ORDER BY sample_rank", (batch_id,))
        ]
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--release", required=True)
    ap.add_argument("--sample-size", type=int, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--batch", default=None, help="可选：审计批次 ID，用于对照")
    ap.add_argument("--audit", default=None, help="审计目录（配合 --batch）")
    ap.add_argument("--query", type=int, default=None,
                    help="查某篇公文在 (seed) 下的排名，1 起")
    args = ap.parse_args()
    result = sample(args.workspace, args.release, args.sample_size, args.seed)
    if args.query is not None:
        idx = args.query - 1
        if 0 <= idx < len(result["ranked"]):
            result["ranked_at"] = {"rank": args.query,
                                   "doc_id": result["ranked"][idx]}
    if args.batch:
        if not args.audit:
            raise SystemExit("--batch 需要同时给出 --audit")
        batch_docs = sorted(batch_sample(args.audit, args.batch),
                            key=lambda d: d.encode("utf-8"))
        result["batch_sample"] = batch_docs
        result["matches_batch"] = batch_docs == result["sample"]
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("matches_batch", True) else 4


if __name__ == "__main__":
    raise SystemExit(main())
