# -*- coding: utf-8 -*-
"""裁决工作区端到端测试（标准库 unittest，python -I 下运行）。

覆盖：
- 工作区初始化：原库只读、底稿快照、固定策略与端到端例子；
- 工单导入的稳定身份与幂等；
- 区域整组三方合并：一方改动取胜、相同业务值合并保留双方理由、
  不同业务值冲突、未改过不是删除；
- 引用判定：退役不是否定票、端点失效不得静默丢弃、不得借同区片段顶替；
- 阻断原始关系：按区域办结、不采纳、关联已确认引用（不新增团队票）；
- 发布：待办阻挡、按文档隔离、发布快照不可变、出处可回查；
- 进程竞争与中断重试：不覆盖、不出现半版、重试得原结果；
- 高扇出端点撤除：依赖不漏报、不跨文档扫描。
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.build import build  # noqa: E402
from qc.policy import parse_policy  # noqa: E402
from qc.ws_common import WsError, connect, resolve_revision  # noqa: E402
from qc.ws_init import init_workspace  # noqa: E402
from qc.ws_merge import merge, preview  # noqa: E402
from qc.ws_release import (  # noqa: E402
    blockers,
    get_release,
    release,
    stream_release,
)
from qc.ws_source import WorkspaceSources  # noqa: E402
from qc.ws_view import view_revision  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 40+ 字符的 ASCII 原文；所有坐标天然是 UTF-8 边界，切片可逐字节核对
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


def _write_jsonl(path, header, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(header, sort_keys=True) + "\n")
        for rec in records:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")


class Scenario:
    """建立间距明确的文档与包：X=[0,10)，Y=[20,26)，一条被阻断的记录。"""

    def __init__(self, root):
        self.root = root
        os.makedirs(os.path.join(root, "sources"), exist_ok=True)
        os.makedirs(os.path.join(root, "packages"), exist_ok=True)
        self.text = TEXT.encode("utf-8")
        self.other_text = b"HELLOWORLD" * 5
        self.digest = hashlib.sha256(self.text).hexdigest()
        self.other_digest = hashlib.sha256(self.other_text).hexdigest()
        self._write_sources()
        self._write_policy()
        self._write_packages()
        self.manifest = os.path.join(root, "manifest.json")
        self.policy = os.path.join(root, "policy.json")
        self.warehouse = os.path.join(root, "warehouse.db")

    def _write_sources(self):
        with open(os.path.join(self.root, "sources/doc1.txt"), "wb") as fh:
            fh.write(self.text)
        with open(os.path.join(self.root, "sources/doc2.txt"), "wb") as fh:
            fh.write(self.other_text)
        with open(os.path.join(self.root, "manifest.json"), "w",
                  encoding="utf-8") as fh:
            json.dump({"documents": [
                {"doc_id": "D1", "path": "sources/doc1.txt",
                 "sha256": self.digest},
                {"doc_id": "D2", "path": "sources/doc2.txt",
                 "sha256": self.other_digest},
            ]}, fh)

    def _write_policy(self):
        with open(os.path.join(self.root, "policy.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(POLICY, fh)

    def _hdr(self, team, doc="D1"):
        if doc == "D1":
            return {"kind": "package", "package_id": f"PKG-{team}",
                    "doc_id": "D1", "team_id": team,
                    "source_digest": self.digest}
        return {"kind": "package", "package_id": f"PKG2-{team}",
                "doc_id": "D2", "team_id": team,
                "source_digest": self.other_digest}

    def _frag(self, lid, layer, label, s, e, doc="D1"):
        text = self.text if doc == "D1" else self.other_text
        return {"kind": "fragment", "id": lid, "layer": layer,
                "label": label, "start": s, "end": e,
                "seen_text": text[s:e].decode()}

    def _write_packages(self):
        pkg = os.path.join(self.root, "packages")

        def review(layer, ranges):
            return {"kind": "review", "layer": layer, "ranges": ranges}

        def relation(t, f, to, sign="for"):
            return {"kind": "relation", "type": t, "from_id": f, "to_id": to,
                    "sign": sign}

        # X=[0,10) L1 两票胜出；Y=[20,26) A1 两票胜出；X->Y 两票确认
        _write_jsonl(os.path.join(pkg, "a.jsonl"), self._hdr("tA"), [
            review("clause", [[0, 10]]), review("annex", [[20, 26]]),
            relation("ref", "x", "y"),
            self._frag("x", "clause", "L1", 0, 10),
            self._frag("y", "annex", "A1", 20, 26),
        ])
        _write_jsonl(os.path.join(pkg, "b.jsonl"), self._hdr("tB"), [
            review("clause", [[0, 10]]), review("annex", [[20, 26]]),
            self._frag("x", "clause", "L1", 0, 10),
            self._frag("y", "annex", "A1", 20, 26),
            relation("ref", "x", "y"),
        ])
        # 第三个团队提出同区不同标签 L2 -> 落败；其 X'->Y 关系被阻断
        _write_jsonl(os.path.join(pkg, "c.jsonl"), self._hdr("tC"), [
            review("clause", [[0, 10]]), review("annex", [[20, 26]]),
            self._frag("c1", "clause", "L2", 0, 10),
            self._frag("cy", "annex", "A1", 20, 26),
            relation("ref", "c1", "cy"),
        ])
        # D2 独立的小包
        _write_jsonl(os.path.join(pkg, "d2a.jsonl"), self._hdr("uA", "D2"), [
            review("clause", [[0, 10]]),
            self._frag("p", "clause", "L1", 0, 10, doc="D2"),
        ])
        _write_jsonl(os.path.join(pkg, "d2b.jsonl"), self._hdr("uB", "D2"), [
            review("clause", [[0, 10]]),
            self._frag("p", "clause", "L1", 0, 10, doc="D2"),
        ])

    def build(self):
        result = build(
            manifest_path=self.manifest,
            packages_root=os.path.join(self.root, "packages"),
            output_db=self.warehouse,
            policy=parse_policy(POLICY),
        )
        assert not result.errors, [e.to_dict() for e in result.errors[:5]]
        return self.warehouse


class WorkspaceTestCase(unittest.TestCase):
    """共享一个初始化好的场景与若干工作区。"""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="qcws-")
        cls.scenario = Scenario(os.path.join(cls.tmpdir, "src"))
        cls.warehouse = cls.scenario.build()
        cls.manifest = cls.scenario.manifest
        cls.policy = cls.scenario.policy

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # -- helpers -----------------------------------------------------

    def workspace(self, name):
        ws = os.path.join(self.tmpdir, name)
        shutil.rmtree(ws, ignore_errors=True)
        init_workspace(ws, self.warehouse, self.policy)
        return ws

    def conn(self, ws):
        return connect(ws)

    def merge(self, ws, ticket):
        conn = connect(ws)
        try:
            return merge(conn, WorkspaceSources(conn, manifest_path=self.manifest),
                         ws, data=ticket)
        finally:
            conn.close()

    def ids(self, conn, rev="HEAD", doc="D1"):
        view = view_revision(conn, rev, doc_id=doc)
        regions = {r["layer"]: r["region_uid"]
                   for r in view["documents"][doc]["regions"]}
        fragments = {(f["layer"], f["start"]): f["fragment_uid"]
                     for f in view["documents"][doc]["fragments"]}
        return view, regions, fragments

    def base_tickets(self, conn):
        view, regions, frags = self.ids(conn)
        base = resolve_revision(conn, "HEAD")["rev_id"]
        x, y = frags[("clause", 0)], frags[("annex", 20)]
        alice = {
            "reviewer": "alice", "title": "拆分 X 并退役 X->Y", "base": base,
            "regions": [{
                "region": regions["clause"], "decision": "fragments",
                "reason": "X 拆为两段",
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
        return base, x, y, alice, bob

    # -- 1. 初始化 ---------------------------------------------------

    def test_01_init_isolation_and_snapshot(self):
        ws = self.workspace("init")
        # 原库只读：工作区副本不可写
        copy = os.path.join(ws, "base.db")
        self.assertTrue(os.path.isfile(copy))
        self.assertEqual(os.stat(copy).st_mode & 0o222, 0)
        conn = self.conn(ws)
        try:
            # 底稿快照原样保留自动判定
            rows = conn.execute(
                "SELECT kind, COUNT(*) c FROM region_state GROUP BY kind"
            ).fetchall()
            kinds = {r["kind"]: r["c"] for r in rows}
            # D1 的两个胜出区域 + D2 的一个，全部自动承接
            self.assertEqual(kinds.get("candidate"), 3)
            rels = conn.execute(
                "SELECT kind, COUNT(*) c FROM relation_state GROUP BY kind"
            ).fetchall()
            self.assertEqual({r["kind"]: r["c"] for r in rels}, {"present": 1})
            blocked = conn.execute(
                "SELECT COUNT(*) c FROM base_blocked").fetchone()["c"]
            self.assertEqual(blocked, 1)
            # 固定策略在初始化时写入，旧库不必保存策略全文
            pol = conn.execute("SELECT raw_json FROM ws_policy").fetchone()
            self.assertIn("min_support", pol["raw_json"])
        finally:
            conn.close()

    def test_02_policy_must_cover_base(self):
        ws = os.path.join(self.tmpdir, "badpol")
        shutil.rmtree(ws, ignore_errors=True)
        narrow = {
            "layers": {"clause": {"labels": ["L1"], "min_support": 2}},
            "relation_types": {},
        }
        path = os.path.join(self.tmpdir, "narrow.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(narrow, fh)
        with self.assertRaises(WsError) as ctx:
            init_workspace(ws, self.warehouse, path)
        self.assertEqual(ctx.exception.code, "POLICY_BINDING_MISMATCH")

    # -- 2. 交换合批：首份成功，次份冲突且不留变更 ------------------

    def test_03_swap_order_alice_then_bob(self):
        ws = self.workspace("swap1")
        conn = self.conn(ws)
        try:
            base, x, y, alice, bob = self.base_tickets(conn)
        finally:
            conn.close()
        r1 = self.merge(ws, alice)
        self.assertEqual(r1["status"], "merged")
        r2 = self.merge(ws, bob)
        self.assertEqual(r2["status"], "conflict")
        self.assertTrue(r2["conflicts"])
        kinds = {c["kind"] for c in r2["conflicts"]}
        self.assertIn("endpoint", kinds)
        # 冲突的次份不留下任何变更
        conn = self.conn(ws)
        try:
            head = resolve_revision(conn, "HEAD")
            self.assertEqual(head["seq"], 1)
            view, _regions, frags = self.ids(conn)
            clause = [f for f in view["documents"]["D1"]["fragments"]
                      if f["layer"] == "clause"]
            self.assertEqual(sorted((f["start"], f["end"]) for f in clause),
                             [(0, 4), (4, 10)])
            rels = {(r["rel_type"], r["kind"])
                    for r in view["documents"]["D1"]["relations"]}
            self.assertEqual(rels, {("ref", "retired")})
        finally:
            conn.close()

    def test_04_swap_order_bob_then_alice(self):
        ws = self.workspace("swap2")
        conn = self.conn(ws)
        try:
            base, x, y, alice, bob = self.base_tickets(conn)
        finally:
            conn.close()
        self.assertEqual(self.merge(ws, bob)["status"], "merged")
        r2 = self.merge(ws, alice)
        self.assertEqual(r2["status"], "conflict")
        conn = self.conn(ws)
        try:
            head = resolve_revision(conn, "HEAD")
            self.assertEqual(head["seq"], 1)
            view, _regions, _frags = self.ids(conn)
            rels = {(r["rel_type"], r["kind"])
                    for r in view["documents"]["D1"]["relations"]}
            self.assertEqual(rels, {("ref", "present"),
                                    ("reverse_ref", "present")})
            clause = [f for f in view["documents"]["D1"]["fragments"]
                      if f["layer"] == "clause"]
            self.assertEqual([(f["start"], f["end"]) for f in clause], [(0, 10)])
        finally:
            conn.close()

    # -- 3. 区域整组的三方合并 ---------------------------------------

    def test_05_same_value_merges_and_keeps_both_reasons(self):
        ws = self.workspace("same")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, _bob = self.base_tickets(conn)
            _view, regions, _frags = self.ids(conn)
        finally:
            conn.close()

        def ticket(reviewer, reason):
            return {
                "reviewer": reviewer, "base": base,
                "regions": [{
                    "region": regions["clause"], "decision": "fragments",
                    "reason": reason,
                    "fragments": [
                        {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
                        {"ref": "X2", "start": 4, "end": 10, "label": "L1"},
                    ],
                }],
                "relations": [{
                    "doc": "D1", "type": "ref", "from": x, "to": y,
                    "decision": "retired", "reason": reason,
                }],
            }

        self.assertEqual(self.merge(ws, ticket("carol", "carol 的理由"))["status"],
                         "merged")
        r2 = self.merge(ws, ticket("dave", "dave 的理由"))
        self.assertEqual(r2["status"], "merged")
        conn = self.conn(ws)
        try:
            seq = resolve_revision(conn, "HEAD")["seq"]
            reasons = {
                r["reviewer_id"]: r["reason"]
                for r in conn.execute(
                    "SELECT reviewer_id, reason FROM state_evidence"
                    " WHERE subject_kind='region' AND seq<=?", (seq,))
            }
            self.assertEqual(reasons.get("carol"), "carol 的理由")
            self.assertEqual(reasons.get("dave"), "dave 的理由")
            # 相同裁决的证据不因重试重复追加
            n_before = conn.execute(
                "SELECT COUNT(*) c FROM state_evidence").fetchone()["c"]
        finally:
            conn.close()
        retry = self.merge(ws, ticket("dave", "dave 的理由"))
        self.assertTrue(retry["replayed"])
        conn = self.conn(ws)
        try:
            n_after = conn.execute(
                "SELECT COUNT(*) c FROM state_evidence").fetchone()["c"]
            self.assertEqual(n_before, n_after)
        finally:
            conn.close()

    def test_06_different_value_conflicts(self):
        ws = self.workspace("diffvalue")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            _view, regions, _frags = self.ids(conn)
        finally:
            conn.close()
        # alice：把 X 拆成 [0,4)+[4,10)
        self.assertEqual(self.merge(ws, alice)["status"], "merged")
        # frank 从同一基版给出另一个拆分方案 -> 同区域不同业务值 -> 冲突
        frank = {
            "reviewer": "frank", "base": base,
            "regions": [{
                "region": regions["clause"], "decision": "fragments",
                "reason": "另一种拆分",
                "fragments": [{"ref": "F1", "start": 0, "end": 6, "label": "L1"},
                              {"ref": "F2", "start": 6, "end": 10, "label": "L1"}],
            }],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "旧边失效",
            }],
        }
        result = self.merge(ws, frank)
        self.assertEqual(result["status"], "conflict")
        c = next(x for x in result["conflicts"]
                 if x["kind"] == "region")
        self.assertEqual(c["subject_key"], regions["clause"])
        self.assertNotEqual(c["current"], c["proposal"])

    def test_07_untouched_is_not_deleted(self):
        ws = self.workspace("untouched")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, _bob = self.base_tickets(conn)
            _view, regions, frags = self.ids(conn)
        finally:
            conn.close()
        # 只裁决 clause 区域；annex 区域与片段应保持
        ticket = {
            "reviewer": "gina", "base": base,
            "regions": [{
                "region": regions["clause"], "decision": "none",
                "reason": "确认该区域无片段",
            }],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "端点已移除，引用退役",
            }],
        }
        self.assertEqual(self.merge(ws, ticket)["status"], "merged")
        conn = self.conn(ws)
        try:
            view, _regions, frags2 = self.ids(conn)
            self.assertIn(("annex", 20), frags2)
            self.assertNotIn(("clause", 0), frags2)
            reg = next(r for r in view["documents"]["D1"]["regions"]
                       if r["layer"] == "clause")
            self.assertEqual(reg["kind"], "none")
            annex = next(r for r in view["documents"]["D1"]["regions"]
                         if r["layer"] == "annex")
            self.assertEqual(annex["kind"], "candidate")
        finally:
            conn.close()

    # -- 4. 引用：退役与端点依赖 -------------------------------------

    def test_08_retirement_is_not_a_negative_vote(self):
        ws = self.workspace("retire")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        self.assertEqual(self.merge(ws, alice)["status"], "merged")
        conn = self.conn(ws)
        try:
            seq = resolve_revision(conn, "HEAD")["seq"]
            row = conn.execute(
                "SELECT kind FROM relation_state WHERE from_fragment=? AND"
                " to_fragment=? AND kind='retired' AND seq_removed IS NULL",
                (x, y)).fetchone()
            self.assertIsNotNone(row)
            # 退役不是否定票：不存在 absent 行
            absent = conn.execute(
                "SELECT COUNT(*) c FROM relation_state WHERE kind='absent'"
            ).fetchone()["c"]
            self.assertEqual(absent, 0)
        finally:
            conn.close()

    def test_09_endpoint_removal_requires_explicit_retirement(self):
        ws = self.workspace("mustretire")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            _view, regions, _frags = self.ids(conn)
        finally:
            conn.close()
        # 只拆 X，不提退役 -> 基版自洽预检就应拒绝
        ticket = {
            "reviewer": "hank", "base": base,
            "regions": [alice["regions"][0]],
        }
        conn = self.conn(ws)
        try:
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            with self.assertRaises(WsError) as ctx:
                merge(conn, src, ws, data=ticket)
            self.assertEqual(ctx.exception.code,
                             "TICKET_RELATION_MUST_RETIRE")
        finally:
            conn.close()
        # 未合批：HEAD 仍为初版
        conn = self.conn(ws)
        try:
            self.assertEqual(resolve_revision(conn, "HEAD")["seq"], 0)
        finally:
            conn.close()

    def test_10_cannot_substitute_sibling_fragment_for_endpoint(self):
        ws = self.workspace("substitute")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, bob = self.base_tickets(conn)
            _view, regions, _frags = self.ids(conn)
        finally:
            conn.close()
        self.assertEqual(self.merge(ws, bob)["status"], "merged")
        # alice 拆分 X，并把旧 ref 退役；但 bob 的新反向引用仍指向 X，
        # 若允许用同区 X1/X2 顶替就会掩盖悬空 -> 必须冲突。
        alice = {
            "reviewer": "alice", "base": base,
            "regions": [{
                "region": regions["clause"], "decision": "fragments",
                "reason": "拆分",
                "fragments": [
                    {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
                    {"ref": "X2", "start": 4, "end": 10, "label": "L1"},
                ],
            }],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "旧边失效",
            }],
        }
        result = self.merge(ws, alice)
        self.assertEqual(result["status"], "conflict")
        self.assertTrue(any(c["kind"] == "endpoint"
                            for c in result["conflicts"]))

    def test_24_same_ticket_fragment_refs_and_absent(self):
        ws = self.workspace("selfref")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            record = conn.execute(
                "SELECT record_uid FROM base_blocked").fetchone()["record_uid"]
        finally:
            conn.close()
        # 同一批里：先拆 X，再把新划的 X1 作为引用端点；同时确认一条
        # 现存端点但基版无判定的反向引用为「不存在」。
        ticket = {
            "reviewer": "owen", "base": base,
            "regions": [alice["regions"][0]],
            "relations": [
                {"doc": "D1", "type": "ref", "from": x, "to": y,
                 "decision": "retired", "reason": "旧边退役"},
                {"doc": "D1", "type": "ref", "from": "X1", "to": y,
                 "decision": "present", "reason": "以本工单新片段为端点"},
                {"doc": "D1", "type": "reverse_ref", "from": y, "to": x,
                 "decision": "absent", "reason": "人工确认不存在"},
            ],
            "blocked": [{"record": record, "disposition": "rejected",
                         "reason": "不采纳"}],
        }
        result = self.merge(ws, ticket)
        self.assertEqual(result["status"], "merged", result.get("conflicts"))
        conn = self.conn(ws)
        try:
            view, _regions, frags = self.ids(conn)
            rels = {(r["rel_type"], r["kind"], r["from_fragment"])
                    for r in view["documents"]["D1"]["relations"]}
            self.assertIn(("ref", "present", frags[("clause", 0)]), rels)
            self.assertIn(("reverse_ref", "absent", frags[("annex", 20)]), rels)
        finally:
            conn.close()

    def test_25_retire_requires_base_adjudication(self):
        ws = self.workspace("retirebase")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        # reverse_ref Y->X 在基版并无判定，不能直接「退役」
        ticket = {
            "reviewer": "pia", "base": base,
            "relations": [{
                "doc": "D1", "type": "reverse_ref", "from": y, "to": x,
                "decision": "retired", "reason": "无中生有地退役",
            }],
        }
        conn = self.conn(ws)
        try:
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            with self.assertRaises(WsError) as ctx:
                merge(conn, src, ws, data=ticket)
            self.assertEqual(ctx.exception.code, "TICKET_RETIRE_UNKNOWN")
        finally:
            conn.close()

    def test_26_evidence_pointer_must_exist(self):
        ws = self.workspace("evidence")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        ticket = {
            "reviewer": "quinn", "base": base,
            "relations": [{
                "doc": "D1", "type": "reverse_ref", "from": y, "to": x,
                "decision": "absent", "reason": "人工确认不存在",
                "evidence": [{"kind": "candidate", "candidate": "cand_ghost"}],
            }],
        }
        conn = self.conn(ws)
        try:
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            with self.assertRaises(WsError) as ctx:
                merge(conn, src, ws, data=ticket)
            self.assertEqual(ctx.exception.code, "TICKET_EVIDENCE_UNKNOWN")
        finally:
            conn.close()

    # -- 5. 阻断原始关系 ---------------------------------------------

    def test_11_adopt_blocked_requires_confirmed_target(self):
        ws = self.workspace("blocked")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            blocked_row = conn.execute(
                "SELECT * FROM base_blocked").fetchone()
        finally:
            conn.close()
        ticket = {
            "reviewer": "ivy", "base": base,
            "regions": [alice["regions"][0]],
            "relations": [
                {"doc": "D1", "type": "ref", "from": x, "to": y,
                 "decision": "retired", "reason": "旧边失效"},
                {"doc": "D1", "type": "ref", "from": "X2", "to": y,
                 "decision": "present", "reason": "人工确认 X2->Y"},
            ],
            "blocked": [{
                "record": blocked_row["record_uid"], "disposition": "adopted",
                "relation": {"doc": "D1", "type": "ref", "from": "X2", "to": y},
                "reason": "端点区域一致，采用",
            }],
        }
        result = self.merge(ws, ticket)
        self.assertEqual(result["status"], "merged")
        conn = self.conn(ws)
        try:
            view, _regions, frags = self.ids(conn)
            state = conn.execute(
                "SELECT * FROM blocked_state WHERE record_uid=? AND"
                " seq_removed IS NULL", (blocked_row["record_uid"],)
            ).fetchone()
            self.assertEqual(state["disposition"], "adopted")
            self.assertEqual(state["from_fragment"], frags[("clause", 4)])
            # 原始阻断记录仍可查
            trace = conn.execute(
                "SELECT * FROM base_blocked WHERE record_uid=?",
                (blocked_row["record_uid"],)).fetchone()
            self.assertEqual(trace["rel_type"], "ref")
            # 发布只含有人工确认的 X2->Y，不为 X1 复制旧边
            rel = release(conn, ws, rev="HEAD", doc_ids=["D1"])
            self.assertTrue(rel["released"])
            rows = [json.loads(line) for line in
                    stream_release(conn, rel["release_id"], what="relations")]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["from_fragment"], frags[("clause", 4)])
        finally:
            conn.close()

    def test_12_adopt_blocked_region_mismatch_rejected(self):
        ws = self.workspace("blockedmismatch")
        conn = self.conn(ws)
        try:
            base, x, y, _alice, _bob = self.base_tickets(conn)
            _view, regions, frags = self.ids(conn)
            record = conn.execute(
                "SELECT * FROM base_blocked").fetchone()["record_uid"]
        finally:
            conn.close()
        # X2 在 [4,10)：与阻断记录的原端点区域一致（同一 clause 区域）；
        # 这里改用 annex 层端点作为 from，层约束不符 -> 拒绝
        ticket = {
            "reviewer": "jane", "base": base,
            "regions": [{
                "region": regions["clause"], "decision": "fragments",
                "reason": "拆分",
                "fragments": [
                    {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
                    {"ref": "X2", "start": 4, "end": 10, "label": "L1"},
                ],
            }],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "旧边失效"}],
            "blocked": [{
                "record": record, "disposition": "adopted",
                "relation": {"doc": "D1", "type": "ref", "from": y, "to": x},
                "reason": "端点方向/层不符",
            }],
        }
        conn = self.conn(ws)
        try:
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            with self.assertRaises(WsError) as ctx:
                merge(conn, src, ws, data=ticket)
            self.assertEqual(ctx.exception.code, "RELATION_LAYER_MISMATCH")
        finally:
            conn.close()

    def test_13_rejected_blocked_left_out_of_delivery(self):
        ws = self.workspace("rejected")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            record = conn.execute(
                "SELECT * FROM base_blocked").fetchone()["record_uid"]
        finally:
            conn.close()
        ticket = {
            "reviewer": "kyle", "base": base,
            "regions": [alice["regions"][0]],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "旧边失效"}],
            "blocked": [{
                "record": record, "disposition": "rejected",
                "reason": "经核对不采纳该原始关系",
            }],
        }
        self.assertEqual(self.merge(ws, ticket)["status"], "merged")
        conn = self.conn(ws)
        try:
            rel = release(conn, ws, rev="HEAD", doc_ids=["D1"])
            self.assertTrue(rel["released"])
            delivered = conn.execute(
                "SELECT COUNT(*) c FROM release_blocked WHERE release_id=?"
                " AND disposition='rejected'", (rel["release_id"],)
            ).fetchone()["c"]
            self.assertEqual(delivered, 0)
            # 但审定历史里仍可查
            state = conn.execute(
                "SELECT disposition FROM blocked_state WHERE record_uid=?"
                " AND seq_removed IS NULL", (record,)).fetchone()
            self.assertEqual(state["disposition"], "rejected")
        finally:
            conn.close()

    def test_14_blocked_adoption_reopens_on_invalidating_change(self):
        ws = self.workspace("reopen")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            record = conn.execute(
                "SELECT * FROM base_blocked").fetchone()["record_uid"]
        finally:
            conn.close()
        # 先办结为 adopted（关联 X->Y 已有的 present）
        ticket = {
            "reviewer": "liam", "base": base,
            "blocked": [{
                "record": record, "disposition": "adopted",
                "relation": {"doc": "D1", "type": "ref", "from": x, "to": y},
                "reason": "关联已确认的 X->Y",
            }],
        }
        self.assertEqual(self.merge(ws, ticket)["status"], "merged")
        # 再拆 X 但不处理该办结 -> 必须冲突（不得静默悬空）
        bad = {
            "reviewer": "liam", "base": base,
            "regions": [alice["regions"][0]],
            "relations": [{
                "doc": "D1", "type": "ref", "from": x, "to": y,
                "decision": "retired", "reason": "旧边失效",
            }],
        }
        conn = self.conn(ws)
        try:
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            result = merge(conn, src, ws, data=bad)
            self.assertEqual(result["status"], "conflict")
            # 被点名的或为依赖对象，或为受影响端点
            self.assertTrue(any(
                c["kind"] in ("dependency", "endpoint")
                and c["subject_key"] == record
                for c in result["conflicts"]))
        finally:
            conn.close()
        # 同一工单显式 reopen 后可以合批
        good = dict(bad)
        good["blocked"] = [{"record": record, "disposition": "reopen",
                            "reason": "端点失效，退回待办"}]
        self.assertEqual(self.merge(ws, good)["status"], "merged")
        conn = self.conn(ws)
        try:
            spent = conn.execute(
                "SELECT COUNT(*) c FROM blocked_state WHERE record_uid=?"
                " AND seq_removed IS NULL", (record,)).fetchone()["c"]
            self.assertEqual(spent, 0)
            pending = blockers(conn, rev="HEAD", doc_ids=["D1"])
            self.assertTrue(pending["blocked"])
            self.assertTrue(any(i["reason"] == "blocked_relation_open"
                                for i in pending["items"]))
        finally:
            conn.close()

    # -- 6. 发布语义 -------------------------------------------------

    def test_15_release_blocked_by_pending_doc(self):
        ws = self.workspace("release")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        conn = self.conn(ws)
        try:
            result = release(conn, ws, rev="HEAD", doc_ids=["D1"])
            self.assertFalse(result["released"])
            self.assertTrue(result["blockers"])
            # D2 独立，不受 D1 未办结影响
            ok = release(conn, ws, rev="HEAD", doc_ids=["D2"])
            self.assertTrue(ok["released"])
        finally:
            conn.close()

    def test_16_release_snapshot_is_immutable(self):
        ws = self.workspace("immutable")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        conn = self.conn(ws)
        try:
            rel = release(conn, ws, rev="HEAD", doc_ids=["D2"])
            self.assertTrue(rel["released"])
            before = list(stream_release(conn, rel["release_id"],
                                         what="fragments"))
        finally:
            conn.close()
        # 后续合批 D1 不改变已登记发布
        self.assertEqual(self.merge(ws, alice)["status"], "merged")
        conn = self.conn(ws)
        try:
            after = list(stream_release(conn, rel["release_id"],
                                        what="fragments"))
            self.assertEqual(before, after)
            # 同一文档 D2 的新发布才反映后续版本（此处 D2 未改，内容相同）
            info = get_release(conn, rel["release_id"])
            self.assertEqual(info["doc_ids"], ["D2"])
        finally:
            conn.close()

    def test_17_release_provenance_auto_and_manual(self):
        ws = self.workspace("prov")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            record = conn.execute(
                "SELECT * FROM base_blocked").fetchone()["record_uid"]
            _view, regions, frags = self.ids(conn)
        finally:
            conn.close()
        ticket = {
            "reviewer": "nina", "base": base,
            "regions": [alice["regions"][0]],
            "relations": [
                {"doc": "D1", "type": "ref", "from": x, "to": y,
                 "decision": "retired", "reason": "旧边退役"},
                {"doc": "D1", "type": "ref", "from": "X2", "to": y,
                 "decision": "present", "reason": "人工确认 X2->Y"},
            ],
            "blocked": [{
                "record": record, "disposition": "adopted",
                "relation": {"doc": "D1", "type": "ref", "from": "X2", "to": y},
                "reason": "采用",
            }],
        }
        self.assertEqual(self.merge(ws, ticket)["status"], "merged")
        conn = self.conn(ws)
        try:
            rel = release(conn, ws, rev="HEAD", doc_ids=["D1"])
            self.assertTrue(rel["released"])
            prov = [json.loads(line) for line in
                    stream_release(conn, rel["release_id"], what="provenance")]
            by_kind = {}
            for row in prov:
                by_kind.setdefault(row["record_kind"], []).append(row)
            # annex Y 是自动承接；clause X1/X2 是人工；relation 是人工
            self.assertTrue(any(r["source_kind"] == "auto"
                                for r in by_kind["fragment"]))
            self.assertTrue(any(r["source_kind"] == "manual"
                                for r in by_kind["fragment"]))
            rel_row = by_kind["relation"][0]
            self.assertEqual(rel_row["source_kind"], "manual")
            self.assertEqual(rel_row["reviewer_id"], "nina")
            self.assertTrue(rel_row["reason"])
        finally:
            conn.close()

    # -- 7. 版本可重开与幂等 -----------------------------------------

    def test_18_reopenable_versions_and_lineage(self):
        ws = self.workspace("lineage")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        result = self.merge(ws, alice)
        conn = self.conn(ws)
        try:
            rev = resolve_revision(conn, result["result_rev"])
            self.assertEqual(rev["base_rev"], base)
            # 可按 ID 重开
            self.assertEqual(resolve_revision(conn, rev["rev_id"])["seq"],
                             rev["seq"])
            # 旧版本内容仍可读
            old = view_revision(conn, base, doc_id="D1")
            self.assertTrue(old["documents"]["D1"]["fragments"])
        finally:
            conn.close()

    def test_19_preview_does_not_change_head(self):
        ws = self.workspace("preview")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
            head_before = resolve_revision(conn, "HEAD")["rev_id"]
            view = preview(conn, WorkspaceSources(conn, manifest_path=self.manifest),
                           data=alice)
            self.assertEqual(view["base_rev"], head_before)
            self.assertIn("regions", view["proposal"])
            self.assertIn("new_fragments", view["proposal"])
            self.assertEqual(resolve_revision(conn, "HEAD")["rev_id"],
                             head_before)
        finally:
            conn.close()

    # -- 8. 并发、中断与重试 -----------------------------------------

    def test_20_concurrent_same_ticket_yields_one_revision(self):
        ws = self.workspace("race")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        q = mp.Queue()
        procs = [mp.Process(target=_race_worker,
                            args=(ws, self.manifest, alice, q))
                 for _ in range(2)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(30)
        results = [q.get() for _ in procs]
        statuses = sorted(r[0] for r in results)
        self.assertEqual(statuses, ["merged", "merged"])
        revs = {r[1] for r in results}
        self.assertEqual(len(revs), 1)
        conn = self.conn(ws)
        try:
            self.assertEqual(resolve_revision(conn, "HEAD")["seq"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM revision").fetchone()["c"], 2)
        finally:
            conn.close()

    def test_21_interrupted_merge_leaves_no_partial_revision(self):
        ws = self.workspace("interrupt")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        # 子进程在写入新版本后、提交前被 SIGKILL
        proc = subprocess.Popen([
            sys.executable, "-I", "-c", _KILL_SCRIPT,
            ws, self.manifest, json.dumps(alice),
        ], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(1.0)
        proc.kill()
        proc.communicate(timeout=30)
        conn = self.conn(ws)
        try:
            # 不得出现半版：HEAD 仍为初版，且没有孤儿 revision
            head = resolve_revision(conn, "HEAD")
            self.assertEqual(head["seq"], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM revision").fetchone()["c"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM fragment_state WHERE origin='manual'"
            ).fetchone()["c"], 0)
        finally:
            conn.close()
        # 重试同一工单成功
        result = self.merge(ws, alice)
        self.assertEqual(result["status"], "merged")
        conn = self.conn(ws)
        try:
            self.assertEqual(resolve_revision(conn, "HEAD")["seq"], 1)
        finally:
            conn.close()

    def test_22_success_without_receipt_replays_same_result(self):
        ws = self.workspace("receipt")
        conn = self.conn(ws)
        try:
            base, x, y, alice, _bob = self.base_tickets(conn)
        finally:
            conn.close()
        first = self.merge(ws, alice)
        second = self.merge(ws, alice)
        self.assertTrue(second["replayed"])
        self.assertEqual(first["result_rev"], second["result_rev"])
        conn = self.conn(ws)
        try:
            self.assertEqual(resolve_revision(conn, "HEAD")["seq"], 1)
        finally:
            conn.close()


def _race_worker(ws, manifest, ticket, q):
    try:
        conn = connect(ws)
        result = merge(conn, WorkspaceSources(conn, manifest_path=manifest),
                       ws, data=ticket)
        conn.close()
        q.put((result["status"], result.get("result_rev")))
    except Exception as exc:  # pragma: no cover
        q.put(("error", repr(exc)))


_KILL_SCRIPT = r"""
import json, os, sys, time
sys.path.insert(0, '.')
from qc.ws_common import connect
from qc.ws_merge import merge
from qc.ws_source import WorkspaceSources
ws, manifest, ticket = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
conn = connect(ws, create=False)
# 在事务里执行合并，并在提交前自杀，验证不出现半版
import qc.ws_merge as m
orig = m._apply_plan
def slow_apply(*a, **kw):
    conn.execute("SELECT 1")
    time.sleep(5)
    return orig(*a, **kw)
m._apply_plan = slow_apply
try:
    merge(conn, WorkspaceSources(conn, manifest_path=manifest), ws, data=ticket)
except BaseException:
    pass
"""


class FanoutTestCase(unittest.TestCase):
    """高扇出端点撤除：依赖不漏报、不跨文档扫描。"""

    def test_23_high_fanout_endpoint_removal(self):
        from tools import fanoutgen

        tmp = tempfile.mkdtemp(prefix="qcfan-")
        try:
            result = fanoutgen.run_fanout(tmp, fanout=2000)
            self.assertEqual(result["status"], "conflict")
            self.assertEqual(result["missing_relations"], 0)
            self.assertEqual(result["affected_relations_reported"], 2000)
            self.assertFalse(result["cross_document_scanned"])
            self.assertTrue(result["head_unchanged"])
            self.assertLess(result["merge_seconds"], 60)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
