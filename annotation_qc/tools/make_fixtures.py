# -*- coding: utf-8 -*-
"""可重建的小夹具生成器。

生成一份严格按字节坐标构造的原文、原文清单、策略声明，以及覆盖以下
对照场景的标注包（每个团队一个 JSONL 包）：

R1  同层 A[0,4) B[3,7) C[6,9) 传递归并为单组 [0,9)；
    团队 D 以 [0,5)+[5,9) 审完且未提议 -> 给「无片段」投票；
    团队 E 以 [0,5)+[6,9) 只审一部分 -> 不进分母、其提议被排除；
    最终同候选 L2 得到 3 票（分母 4，严格多数 3，min_support 2）-> 正式片段。
R2  四个团队审完：两队投同候选、一队自相矛盾、一队无提议 -> 待核。
R3  三个团队审完且均无提议 -> 「无片段」胜出，确认不入仓。
R4  紧邻（首尾相碰）的两个提议不合组。
关系：两队赞成、第三队无票 -> 确认一条正向引用；第三队改投同区另一候选的
    关系 -> 端点被另一候选胜出阻断，可查原因；反向仅一票 -> 待核；
    同团队同关系赞成+反对 -> 冲突票不计任一方但入分母；
    跨包同名局部 ID（多个包都用 "f1"）必须各自独立解析。

用法::

    python tools/make_fixtures.py --root fixtures
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os

# 40 个 ASCII 字符（40 字节，全部单字节，天然是 UTF-8 边界）
DOC_TEXT = "0123456789abcdefghijklmnopqrstuvwxyz!@#$"
assert len(DOC_TEXT) == 40

POLICY = {
    "layers": {
        "clause": {"labels": ["L1", "L2", "L3", "L4"], "min_support": 2},
        "annex": {"labels": ["A1"], "min_support": 2},
    },
    "relation_types": {
        "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2},
        # 反向引用是独立关系类型：方向固定为 annex -> clause，单独核算
        "reverse_ref": {"from_layer": "annex", "to_layer": "clause",
                        "min_support": 2},
    },
}


def write_jsonl(path: str, header: dict, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(header, ensure_ascii=False, sort_keys=True) + "\n")
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")


def frag(local_id: str, layer: str, label: str, start: int, end: int) -> dict:
    return {
        "kind": "fragment",
        "id": local_id,
        "layer": layer,
        "label": label,
        "start": start,
        "end": end,
        "seen_text": DOC_TEXT[start:end],
    }


def review(layer: str, ranges: list[list[int]]) -> dict:
    return {"kind": "review", "layer": layer, "ranges": ranges}


def relation(rel_type: str, from_id: str, to_id: str, sign: str) -> dict:
    return {
        "kind": "relation",
        "type": rel_type,
        "from_id": from_id,
        "to_id": to_id,
        "sign": sign,
    }


def build_all(root: str) -> None:
    src_dir = os.path.join(root, "sources")
    pkg_dir = os.path.join(root, "packages")
    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(pkg_dir, exist_ok=True)

    doc_path = os.path.join(src_dir, "doc1.txt")
    with open(doc_path, "wb") as fh:
        fh.write(DOC_TEXT.encode("utf-8"))
    digest = hashlib.sha256(DOC_TEXT.encode("utf-8")).hexdigest()
    manifest = {
        "documents": [
            {"doc_id": "D1", "path": "sources/doc1.txt", "sha256": digest}
        ]
    }
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
    with open(os.path.join(root, "policy.json"), "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh, ensure_ascii=False, indent=2, sort_keys=True)

    def header(team: str) -> dict:
        return {
            "kind": "package",
            "package_id": f"PKG-{team}",
            "doc_id": "D1",
            "team_id": team,
            "source_digest": digest,
        }

    def pkg(team: str, records: list[dict], sub: str | None = None) -> None:
        rel = f"{team}.jsonl" if sub is None else os.path.join(sub, f"{team}.jsonl")
        write_jsonl(os.path.join(pkg_dir, rel), header(team), records)

    # ---- R1 [0,9)：A/B/C 传递交叠；D 完整覆盖无提议；E 部分覆盖 ----------
    pkg("teamA", [
        review("clause", [[0, 9]]),
        frag("f1", "clause", "L2", 0, 4),
    ])
    pkg("teamB", [
        review("clause", [[0, 9]]),
        # 跨包同名局部 ID：与 teamA 的 f1 同 ID 但相互独立
        frag("f1", "clause", "L2", 3, 7),
    ])
    pkg("teamC", [
        review("clause", [[0, 9]]),
        frag("f1", "clause", "L2", 6, 9),
    ])
    pkg("teamD", [
        # [0,5)+[5,9) 紧邻可接上，完整覆盖；无提议 -> 无片段票
        review("clause", [[0, 5], [5, 9]]),
    ])
    pkg("teamE", [
        # [0,5)+[6,9) 中间缺字节 5；部分覆盖，提议不投票
        review("clause", [[0, 5], [6, 9]]),
        frag("f9", "clause", "L2", 6, 9),
    ])

    # ---- R2 [10,18)：4 队审完，2 同候选 / 1 自相矛盾 / 1 无提议 -> 待核 --
    pkg("teamF", [
        review("clause", [[10, 18]]),
        frag("g1", "clause", "L3", 10, 18),
    ])
    pkg("teamG", [
        review("clause", [[10, 18]]),
        frag("g1", "clause", "L3", 10, 18),
    ])
    pkg("teamH", [
        review("clause", [[10, 18]]),
        # 同一团队提出两个不同候选 -> 自相矛盾，不加票但在分母
        frag("h1", "clause", "L3", 10, 18),
        frag("h2", "clause", "L4", 11, 17),
    ])
    pkg("teamI", [
        review("clause", [[10, 18]]),  # 完整覆盖无提议 -> 无片段票
    ])

    # ---- R3 [19,26)：同候选 2 票 vs 无片段 3 票（分母 5）-> 无片段胜出 ---
    # 区域必须由提议产生：J、K 提同一候选 L3[19,26)；L、V、W 审完无提议
    # 用分包 bundle 形态覆盖多文件读取：包头仅在第一个文件首行
    bundle = os.path.join(pkg_dir, "teamJ-bundle")
    write_jsonl(os.path.join(bundle, "01-head.jsonl"), header("teamJ"), [
        review("clause", [[19, 26]]),
        frag("j1", "clause", "L3", 19, 26),
    ])
    with open(os.path.join(bundle, "02-more.jsonl"), "w",
              encoding="utf-8", newline="\n") as fh:
        fh.write(
            json.dumps(review("annex", [[28, 34]]),
                       ensure_ascii=False, sort_keys=True) + "\n"
        )
    pkg("teamK", [
        review("clause", [[19, 26]]),
        frag("k1", "clause", "L3", 19, 26),
    ])
    pkg("teamL", [review("clause", [[19, 26]])])
    pkg("teamV", [review("clause", [[19, 26]])])
    pkg("teamW", [review("clause", [[19, 26]])])

    # ---- R4 紧邻不合组：[28,31) 与 [31,34) 首尾相碰 -> 两个区域 ----------
    # 为让两者都能胜出，各给两个团队的同候选票
    pkg("teamM", [
        review("clause", [[28, 34]]),
        frag("m1", "clause", "L1", 28, 31),
        frag("m2", "clause", "L1", 31, 34),
    ])
    pkg("teamN", [
        review("clause", [[28, 34]]),
        frag("n1", "clause", "L1", 28, 31),
        frag("n2", "clause", "L1", 31, 34),
    ])
    # annex 层正式片段：[28,34)，供关系端点使用
    pkg("teamO", [
        review("annex", [[28, 34]]),
        frag("a1", "annex", "A1", 28, 34),
    ])
    pkg("teamP", [
        review("annex", [[28, 34]]),
        frag("a1", "annex", "A1", 28, 34),
    ])

    # ---- 关系 -----------------------------------------------------------
    # 正向 ref：clause[28,31) L1 -> annex[28,34) A1
    # teamM、teamN 各投赞成（2 票，分母 2，min_support 2）-> 确认引用
    # 先在 teamM 写关系再写片段，同时覆盖「引用可先于片段出现」
    pkg("teamM", [
        review("clause", [[28, 34]]),
        review("annex", [[28, 34]]),
        relation("ref", "m1", "a1", "for"),
        frag("m1", "clause", "L1", 28, 31),
        frag("m2", "clause", "L1", 31, 34),
        frag("a1", "annex", "A1", 28, 34),
    ])
    pkg("teamN", [
        review("clause", [[28, 34]]),
        review("annex", [[28, 34]]),
        frag("n1", "clause", "L1", 28, 31),
        frag("n2", "clause", "L1", 31, 34),
        frag("a1", "annex", "A1", 28, 34),
        relation("ref", "n1", "a1", "for"),
    ])
    # teamQ：看过两个片段但没有关系记录 -> 不补反对票，不进分母
    pkg("teamQ", [
        review("clause", [[28, 31]]),
        review("annex", [[28, 34]]),
        frag("q1", "clause", "L1", 28, 31),
        frag("qa", "annex", "A1", 28, 34),
    ])
    # teamR：端点落在同区另一候选上（clause 区域候选 L1[28,31) 胜出；
    # 该队投给不同标签 L2 的同坐标提议）-> 关系被阻断，可查原因
    pkg("teamR", [
        review("clause", [[28, 31]]),
        review("annex", [[28, 34]]),
        frag("r1", "clause", "L2", 28, 31),
        frag("ra", "annex", "A1", 28, 34),
        relation("ref", "r1", "ra", "for"),
    ])
    # 反向仅一票：annex[28,34) -> clause[28,31)，单独核算不得通过
    pkg("teamS", [
        review("clause", [[28, 31]]),
        review("annex", [[28, 34]]),
        frag("s1", "clause", "L1", 28, 31),
        frag("sa", "annex", "A1", 28, 34),
        relation("reverse_ref", "sa", "s1", "for"),
    ])
    # 同团队对同一正式关系既赞成又反对 -> 冲突票（用第二个 clause 片段 [31,34)
    # 构造另一组关系，保证与上面组互不干扰）
    pkg("teamT", [
        review("clause", [[31, 34]]),
        review("annex", [[28, 34]]),
        frag("t1", "clause", "L1", 31, 34),
        frag("ta", "annex", "A1", 28, 34),
        relation("ref", "t1", "ta", "for"),
        relation("ref", "t1", "ta", "against"),
    ])
    # 前向引用：关系记录写在片段之前，验证允许先出现引用
    pkg("teamU", [
        review("clause", [[31, 34]]),
        review("annex", [[28, 34]]),
        relation("ref", "u1", "ua", "for"),
        frag("u1", "clause", "L1", 31, 34),
        frag("ua", "annex", "A1", 28, 34),
    ])

    # 手算参考表（tests 里据此断言）
    hand = {
        "regions": {
            # A/B/C 坐标不同 -> 三个不同候选各 1 票；D 投无片段；E 被排除
            "R1": {"envelope": [0, 9], "denominator": 4,
                   "winner": "pending",
                   "candidate_votes_each": 1,
                   "no_fragment_votes": 1, "excluded_team": "teamE"},
            "R2": {"envelope": [10, 18], "denominator": 4,
                   "winner": "pending", "L3_votes": 2,
                   "no_fragment_votes": 1, "self_contradiction": ["teamH"]},
            # L3 候选 2 票未达严格多数 3；无片段 3 票 -> 确认不入仓
            "R3": {"envelope": [19, 26], "denominator": 5,
                   "winner": "no_fragment",
                   "candidate_votes": 2, "no_fragment_votes": 3},
            "R4a": {"envelope": [28, 31], "winner_label": "L1"},
            "R4b": {"envelope": [31, 34], "winner_label": "L1"},
            "annex": {"envelope": [28, 34], "winner_label": "A1"},
        },
        "relations": {
            "forward_confirmed": {
                "type": "ref",
                "from": [28, 31, "L1"], "to": [28, 34, "A1"],
                "for": 2, "denominator": 2, "decision": "confirmed"},
            "reverse_pending": {
                "type": "reverse_ref",
                "from": [28, 34, "A1"], "to": [28, 31, "L1"],
                "for": 1, "denominator": 1, "decision": "pending"},
            "conflict_group": {
                "type": "ref",
                "from": [31, 34, "L1"], "to": [28, 34, "A1"],
                "denominator": 2, "conflict": 1, "for": 1,
                "decision": "pending"},
            "blocked_team": "teamR",
        },
        "doc_sha256": digest,
    }
    with open(os.path.join(root, "hand_calc.json"), "w", encoding="utf-8") as fh:
        json.dump(hand, fh, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"fixtures written under {root}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="fixtures")
    args = ap.parse_args()
    build_all(args.root)
