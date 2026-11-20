# -*- coding: utf-8 -*-
"""运行时规模生成器（不提交其产物）。

生成可控规模的合成原文与标注包，用于验证：
- 同一层海量提议全部相互交叠时，归组仍是排序 + 单遍扫描线（O(n log n)），
  不会枚举两两交叠、不会生成平方级交叠边；
- 少数文档上的稠密已审核区间，覆盖资格按包合并后双指针推进；
- 同一引用的票分散在许多包时，归票为哈希分组的线性过程。

所有坐标落在 ASCII 字节上（天然 UTF-8 边界），内容逐字节可核对。

用法::

    python tools/scalegen.py --root scale_tmp --teams 12 --proposals 4000
    python tools/scalegen.py --root scale_tmp --teams 12 --proposals 4000 --build
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random

POLICY = {
    "layers": {
        "clause": {"labels": ["L1", "L2", "L3"], "min_support": 2},
        "annex": {"labels": ["A1"], "min_support": 2},
    },
    "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2},
    },
}


def generate(root: str, n_teams: int = 10, n_proposals: int = 2000,
             doc_bytes: int = 100_000, seed: int = 1) -> dict:
    rng = random.Random(seed)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    os.makedirs(os.path.join(root, "packages"), exist_ok=True)

    # 可重复的 ASCII 原文（每字节一个字符，边界处处合法）
    alphabet = b"0123456789abcdefghijklmnopqrstuvwxyz"
    text = bytes(alphabet[i % len(alphabet)] for i in range(doc_bytes))
    with open(os.path.join(root, "sources", "big.txt"), "wb") as fh:
        fh.write(text)
    digest = hashlib.sha256(text).hexdigest()
    manifest = {"documents": [
        {"doc_id": "BIG", "path": "sources/big.txt", "sha256": digest}
    ]}
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, sort_keys=True)
    with open(os.path.join(root, "policy.json"), "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh, sort_keys=True)

    # 争议热区固定在文档前部，所有团队的 clause 提议都落在其中并两两交叠：
    # 起点取 [0, zone//2)，终点取 (zone//2, zone]，于是任意两区间都共享
    # 中部公共段，保证全部正长度交叠（同包内坐标去重）。
    zone = min(1000, doc_bytes)
    mid = zone // 2
    pkg_dir = os.path.join(root, "packages")
    relation_votes = 0
    total_proposals = 0

    # annex 固定候选，供分散在各包的关系票使用
    annex_span = (zone + 10, zone + 30)

    for t in range(n_teams):
        team = f"scaleTeam{t:03d}"
        path = os.path.join(pkg_dir, f"{team}.jsonl")
        header = {
            "kind": "package", "package_id": f"PKG-{team}",
            "doc_id": "BIG", "team_id": team, "source_digest": digest,
        }
        seen: set[tuple] = set()
        records = [{
            "kind": "review", "layer": "clause",
            # 稠密审核：整段热区完整覆盖（紧邻分片也接上）
            "ranges": [[0, zone]],
        }, {
            "kind": "review", "layer": "annex",
            "ranges": [list(annex_span)],
        }]
        # 每包一个 annex 提议（同坐标同标签 -> 同一候选）
        records.append({
            "kind": "fragment", "id": "anc", "layer": "annex",
            "label": "A1", "start": annex_span[0], "end": annex_span[1],
            "seen_text": text[annex_span[0]:annex_span[1]].decode(),
        })
        # 每包一个固定坐标的 clause 锚点提议（同一候选），作为分散关系票端点。
        # 锚点候选由所有投票团队共同提出，保证它成为大区域的胜出候选。
        anchor = (mid - 1, mid + 1)
        records.append({
            "kind": "fragment", "id": "anchor", "layer": "clause",
            "label": "L1", "start": anchor[0], "end": anchor[1],
            "seen_text": text[anchor[0]:anchor[1]].decode(),
        })
        seen.add((anchor[0], anchor[1], "L1"))
        # 再提一个同坐标不同局部 ID 的副本，验证同候选多局部 ID 只计一票
        records.append({
            "kind": "fragment", "id": "anchor_copy", "layer": "clause",
            "label": "L1", "start": anchor[0], "end": anchor[1],
            "seen_text": text[anchor[0]:anchor[1]].decode(),
        })
        # 海量交叠提议集中在少数「重包」团队（前 heavy_teams 个）。
        # 这些团队同时提出很多不同候选 -> 在大区域里自相矛盾、不给任何候选
        # 加票，但仍在分母；投票团队只提锚点，保持单候选有效票。
        heavy = t < max(2, n_teams // 4)
        if heavy:
            for i in range(n_proposals):
                start = rng.randrange(0, mid)
                end = rng.randrange(mid + 1, zone + 1)
                label = f"L{rng.randrange(1, 4)}"
                key = (start, end, label)
                if key in seen:
                    continue
                seen.add(key)
                records.append({
                    "kind": "fragment", "id": f"c{i:05d}", "layer": "clause",
                    "label": label, "start": start, "end": end,
                    "seen_text": text[start:end].decode(),
                })
                total_proposals += 1
        # 分散在各投票包的关系票（重包团队不提关系，避免噪声）
        if not heavy:
            records.append({
                "kind": "relation", "type": "ref",
                "from_id": "anchor", "to_id": "anc",
                "sign": "for" if t % 2 == 0 else "against",
            })
            relation_votes += 1
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(header, sort_keys=True) + "\n")
            for rec in records:
                fh.write(json.dumps(rec, sort_keys=True, ensure_ascii=False) + "\n")

    summary = {
        "teams": n_teams,
        "clause_proposals": total_proposals,
        "relation_votes": relation_votes,
        "doc_bytes": doc_bytes,
        "hot_zone": [0, zone],
        "expected_regions": 2,
    }
    with open(os.path.join(root, "scale_summary.json"), "w",
              encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--teams", type=int, default=10)
    ap.add_argument("--proposals", type=int, default=2000)
    ap.add_argument("--doc-bytes", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--build", action="store_true",
                    help="生成后立即运行构建并打印规模/计时")
    args = ap.parse_args()
    summary = generate(args.root, args.teams, args.proposals,
                       args.doc_bytes, args.seed)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.build:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import time
        from qc.build import build
        from qc.policy import parse_policy
        from qc.query import QueryDB

        t0 = time.perf_counter()
        out_db = os.path.join(args.root, "warehouse.db")
        result = build(
            manifest_path=os.path.join(args.root, "manifest.json"),
            packages_root=os.path.join(args.root, "packages"),
            output_db=out_db,
            policy=parse_policy(
                json.load(open(os.path.join(args.root, "policy.json")))),
        )
        elapsed = time.perf_counter() - t0
        if result.errors:
            print("BUILD REJECTED", [e.to_dict() for e in result.errors[:5]])
            return 2
        with QueryDB(out_db) as q:
            counters = q.meta()["run_counters"]
        print(json.dumps({
            "elapsed_seconds": round(elapsed, 3),
            "counters": counters,
        }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
