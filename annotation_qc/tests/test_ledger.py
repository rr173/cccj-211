# -*- coding: utf-8 -*-
"""分仓台账单元测试（标准库 unittest，python -I 下运行）。

覆盖：建账配置、选入校验与整批原子性、哈希分仓、摘要/人工证据同组、
豁免与最短字节数、底稿相同无片段同组、交付落锁与历史摘要、隔离与
证据撤销、改选/撤出语义、旧版拒绝、请求幂等、写入中断恢复、
全量重算对照与诊断局部性。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.ledger import Ledger, init_ledger  # noqa: E402
from qc.ledger_common import LedgerError  # noqa: E402
from tools.ledger_fixture import write_synthetic_workspace  # noqa: E402

CLAUSE = "shared-clause-text-0001"     # 非豁免共享条款
SIGNOFF = "signoff-boilerplate-text"   # 豁免落款
SHORT = "tiny"                         # 不足最短字节数
EXEMPT = hashlib.sha256(SIGNOFF.encode("utf-8")).hexdigest()


def bin_of(doc_id: str) -> int:
    return hashlib.sha256(doc_id.encode("utf-8")).digest()[0] & 1


def pick(base: str, want: int) -> str:
    i = 0
    while True:
        cand = base if i == 0 else f"{base}-{i}"
        if bin_of(cand) == want:
            return cand
        i += 1


def frag(uid, text, layer="clause"):
    return {"uid": uid, "layer": layer, "text": text}


class LedgerCase(unittest.TestCase):
    """每个用例一套全新上游与台账。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ledger-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.A = pick("A", 0)   # 哈希分仓 -> train
        self.B = pick("B", 1)   # 哈希分仓 -> eval
        docs = {
            self.A: "draft-a", self.B: "draft-b", "C": "draft-c",
            "D": "draft-d", "E": "draft-e", "F": "draft-f",
            "G": "draft-shared", "H": "draft-shared",
            "I": "draft-i", "J": "draft-j",
        }
        self.releases = [
            {"release_id": "R1", "rev_id": "rev-1", "rev_seq": 1,
             "docs": {
                 self.A: [frag("a1", "unique-a-fragment"), frag("a2", CLAUSE),
                          frag("a3", SIGNOFF),
                          frag("a4", "unique-b1-fragment", layer="annex")],
                 self.B: [frag("b1", "unique-b1-fragment"),
                          frag("b2", "unique-b2-fragment"),
                          frag("b3", SIGNOFF)],
                 "C": [frag("c1", "unique-c-fragment"), frag("c2", CLAUSE),
                       frag("c3", SIGNOFF)],
                 "D": [frag("d1", CLAUSE), frag("d2", SIGNOFF)],
                 "E": [frag("e1", "unique-e-fragment"), frag("e2", SIGNOFF),
                       frag("e3", SHORT)],
                 "F": [frag("f1", "unique-f-fragment"), frag("f2", SIGNOFF),
                       frag("f3", SHORT)],
                 "G": [],
                 "H": [],
                 "I": [frag("i1", "unique-i-fragment"), frag("i2", SIGNOFF)],
                 "J": [frag("j1", "unique-j-fragment"), frag("j2", SIGNOFF)],
             }},
            {"release_id": "R2", "rev_id": "rev-2", "rev_seq": 2,
             "docs": {
                 self.A: [frag("a5", "unique-a2-fragment"),
                          frag("a6", SIGNOFF)],
                 "C": [frag("c4", "unique-c2-fragment"), frag("c5", SIGNOFF)],
             }},
        ]
        self.ws = os.path.join(self.tmp, "ws")
        write_synthetic_workspace(self.ws, docs, self.releases)
        self.ledger_dir = os.path.join(self.tmp, "ledger")
        init_ledger(self.ledger_dir, self.ws, layer="clause",
                    min_segment_bytes=8, exempt_digests=[EXEMPT])
        self.lg = Ledger(self.ledger_dir)
        self.addCleanup(self.lg.close)

    # -- 小助手 -------------------------------------------------------------
    def apply(self, rid, **ops):
        return self.lg.apply_change(
            {"request_id": rid, "base_version": self.lg.version, **ops})

    def confirm(self, rid, doc_ids):
        return self.lg.confirm(
            {"request_id": rid, "base_version": self.lg.version,
             "list_version": self.lg.version, "doc_ids": doc_ids})

    def listed(self, doc_id):
        for pos in ("train", "eval", "quarantine"):
            for row in self.lg.lists()[pos]:
                if row["doc_id"] == doc_id:
                    return pos
        return None

    def component_of(self, doc_id):
        row = self.lg.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_id,)).fetchone()
        return None if row is None else row["component_id"]

    def select(self, rid, *doc_ids, release="R1"):
        return self.apply(rid, select=[
            {"doc_id": d, "release_id": release} for d in doc_ids])

    def expect_error(self, fn, code):
        with self.assertRaises(LedgerError) as ctx:
            fn()
        self.assertEqual(ctx.exception.code, code)
        return ctx.exception

    # -- 建账与选入校验 ------------------------------------------------------
    def test_init_config_stored(self):
        st = self.lg.status()
        self.assertEqual(st["ledger_version"], 0)
        self.assertEqual(st["layer"], "clause")
        self.assertEqual(st["min_segment_bytes"], 8)
        self.assertEqual(st["exempt_digests"], 1)

    def test_unknown_release_rejected(self):
        exc = self.expect_error(
            lambda: self.apply("r1", select=[
                {"doc_id": self.A, "release_id": "RX"}]),
            "BATCH_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "UNKNOWN_RELEASE")
        self.assertEqual(self.lg.version, 0)

    def test_doc_not_in_release_rejected(self):
        exc = self.expect_error(
            lambda: self.apply("r1", select=[
                {"doc_id": "I", "release_id": "R2"}]),
            "BATCH_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "DOC_NOT_IN_RELEASE")

    def test_batch_atomic_no_partial_commit(self):
        self.expect_error(
            lambda: self.apply("r1", select=[
                {"doc_id": "I", "release_id": "R1"},
                {"doc_id": self.A, "release_id": "RX"}]),
            "BATCH_REJECTED")
        self.assertIsNone(self.component_of("I"))
        self.assertEqual(self.lg.version, 0)

    def test_conflicting_ops_in_one_batch(self):
        self.expect_error(
            lambda: self.apply("r1",
                               select=[{"doc_id": "I", "release_id": "R1"}],
                               withdraw=["I"]),
            "BATCH_REJECTED")

    def test_empty_batch_rejected(self):
        self.expect_error(lambda: self.apply("r1"), "EMPTY_BATCH")

    # -- 分仓与同组 ----------------------------------------------------------
    def test_hash_partition_and_layer_filter(self):
        self.select("s1", self.A, self.B)
        self.assertEqual(self.listed(self.A), "train")
        self.assertEqual(self.listed(self.B), "eval")
        # A 的 annex 片段与 B 的 clause 片段文本相同：只看 clause 层，不同组
        self.assertNotEqual(self.component_of(self.A),
                            self.component_of(self.B))

    def test_shared_clause_and_transitive_manual_evidence(self):
        self.select("s1", self.A, self.B)
        self.apply("s2", select=[{"doc_id": "C", "release_id": "R1"}],
                   evidence_register=[{"evidence_id": "E1", "doc_a": self.B,
                                       "doc_b": "C", "provenance": "档案盒12"}])
        # A-C 共享条款，B-C 人工证据 -> 传递连通
        self.assertEqual(self.component_of(self.A),
                         self.component_of(self.B))
        self.assertEqual(self.component_of(self.B),
                         self.component_of("C"))
        pos = self.listed(self.A)
        self.assertEqual(self.listed(self.B), pos)
        self.assertEqual(self.listed("C"), pos)

    def test_exempt_and_short_fragments_do_not_group(self):
        self.select("s1", "E", "F")
        self.assertNotEqual(self.component_of("E"), self.component_of("F"))

    def test_same_draft_no_fragments_still_grouped_and_listed(self):
        self.select("s1", "G", "H")
        self.assertEqual(self.component_of("G"), self.component_of("H"))
        self.assertIsNotNone(self.listed("G"))
        self.assertIsNotNone(self.listed("H"))
        row = [r for r in self.lg.lists()[self.listed("G")]
               if r["doc_id"] == "G"][0]
        self.assertEqual((row["release_id"], row["rev_id"]), ("R1", "rev-1"))

    def test_evidence_dormant_until_both_endpoints_in_ledger(self):
        self.select("s1", self.A)
        self.apply("s2", evidence_register=[
            {"evidence_id": "E1", "doc_a": self.A, "doc_b": "I",
             "provenance": "待激活"}])
        self.assertIsNone(self.component_of("I"))
        self.select("s3", "I")
        self.assertEqual(self.component_of(self.A), self.component_of("I"))

    def test_withdrawn_never_delivered_endpoint_disables_evidence(self):
        self.select("s1", "I", "J")
        self.apply("s2", evidence_register=[
            {"evidence_id": "E1", "doc_a": "I", "doc_b": "J",
             "provenance": "x"}])
        self.assertEqual(self.component_of("I"), self.component_of("J"))
        self.apply("s3", withdraw=["I"])
        self.assertNotEqual(self.component_of("I"), self.component_of("J"))
        self.assertIsNone(self.listed("I"))
        self.assertIsNotNone(self.listed("J"))

    def test_evidence_unknown_doc_rejected(self):
        self.expect_error(
            lambda: self.apply("r1", evidence_register=[
                {"evidence_id": "E1", "doc_a": self.A, "doc_b": "Z",
                 "provenance": "x"}]),
            "BATCH_REJECTED")

    def test_evidence_id_not_reusable_and_revoke_checks(self):
        self.select("s1", "I", "J")
        self.apply("s2", evidence_register=[
            {"evidence_id": "E1", "doc_a": "I", "doc_b": "J",
             "provenance": "x"}])
        self.apply("s3", evidence_revoke=["E1"])
        exc = self.expect_error(
            lambda: self.apply("s4", evidence_register=[
                {"evidence_id": "E1", "doc_a": "I", "doc_b": "J",
                 "provenance": "y"}]),
            "BATCH_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "DUPLICATE_EVIDENCE_ID")
        exc = self.expect_error(
            lambda: self.apply("s5", evidence_revoke=["E1"]),
            "BATCH_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "EVIDENCE_NOT_ACTIVE")
        exc = self.expect_error(
            lambda: self.apply("s6", evidence_revoke=["E9"]),
            "BATCH_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "UNKNOWN_EVIDENCE")

    # -- 交付、锁定与隔离 ----------------------------------------------------
    def _lock_a_b(self):
        self.select("s1", self.A, self.B)
        return self.confirm("c1", [self.A, self.B])

    def test_confirm_locks_and_credentials(self):
        rec = self._lock_a_b()
        creds = {d["doc_id"]: d for d in rec["deliveries"]}
        self.assertEqual(creds[self.A]["position"], "train")
        self.assertEqual(creds[self.B]["position"], "eval")
        got = self.lg.credentials(self.A)
        self.assertEqual(len(got), 1)
        kinds = {d["kind"] for d in got[0]["digests"]}
        self.assertEqual(kinds, {"whole", "segment"})

    def test_confirm_not_listed_rejected(self):
        self.select("s1", self.A)
        exc = self.expect_error(lambda: self.confirm("cX", ["I"]),
                                "CONFIRM_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "NOT_LISTED")

    def test_quarantine_then_revoke_restores_locked_bins(self):
        self._lock_a_b()
        self.apply("s2", select=[{"doc_id": "C", "release_id": "R1"}],
                   evidence_register=[
                       {"evidence_id": "E1", "doc_a": "C", "doc_b": self.B,
                        "provenance": "p1"},
                       {"evidence_id": "E2", "doc_a": "C", "doc_b": self.B,
                        "provenance": "p2"}])
        for d in (self.A, self.B, "C"):
            self.assertEqual(self.listed(d), "quarantine")
        exc = self.expect_error(lambda: self.confirm("cX", ["C"]),
                                "CONFIRM_REJECTED")
        self.assertEqual(exc.errors[0]["code"], "QUARANTINED")
        qr = self.lg.quarantine_reason("C")
        self.assertTrue(qr["quarantined"])
        self.assertEqual({l["position"] for l in qr["locks"]},
                         {"train", "eval"})
        self.apply("s3", evidence_revoke=["E1"])
        self.assertEqual(self.listed("C"), "quarantine")  # 撤一条仍隔离
        self.apply("s4", evidence_revoke=["E2"])
        self.assertEqual(self.listed(self.A), "train")
        self.assertEqual(self.listed("C"), "train")   # 服从 A 的锁定仓位
        self.assertEqual(self.listed(self.B), "eval")
        # 旧交付凭据未被改写
        self.assertEqual(len(self.lg.credentials(self.A)), 1)

    def test_reselect_and_withdraw_keep_delivered_digests(self):
        rec = self._lock_a_b()
        delivery_id = rec["deliveries"][0]["delivery_id"]
        self.apply("s2", select=[{"doc_id": self.A, "release_id": "R2"}])
        inuse = {r["digest"] for r in self.lg.conn.execute(
            "SELECT digest FROM lg_digest WHERE doc_id=? AND source='inuse'",
            (self.A,))}
        self.assertNotIn(hashlib.sha256(CLAUSE.encode()).hexdigest(), inuse)
        cred = self.lg.credentials(self.A)[0]
        self.assertEqual(cred["delivery_id"], delivery_id)
        self.assertIn(hashlib.sha256(CLAUSE.encode()).hexdigest(),
                      {d["digest"] for d in cred["digests"]})
        self.apply("s3", withdraw=[self.A])
        self.assertIsNone(self.listed(self.A))
        # 新选入的 D 只复用旧条款：经历史摘要随 A 的锁定仓位
        self.apply("s4", select=[{"doc_id": "D", "release_id": "R1"}])
        self.assertEqual(self.listed("D"), "train")
        path = self.lg.path_between("D", self.A)
        side = next(s for st in path for s in st["via"].get("sides", [])
                    if s["doc_id"] == self.A)
        self.assertEqual(side["source"], "delivered")
        self.assertEqual(side["release_id"], "R1")
        self.assertEqual(side["delivery"]["delivery_id"], delivery_id)

    def test_second_delivery_same_position_new_digests(self):
        self._lock_a_b()
        self.apply("s2", select=[{"doc_id": "C", "release_id": "R1"}],
                   evidence_register=[])
        # C 与 A 共享条款 -> 同组服从 A 的 train 锁
        self.confirm("c2", ["C"])
        self.apply("s3", select=[{"doc_id": "C", "release_id": "R2"}])
        self.confirm("c3", ["C"])
        creds = self.lg.credentials("C")
        self.assertEqual(len(creds), 2)
        self.assertEqual({c["position"] for c in creds}, {"train"})
        self.assertNotEqual(
            {d["digest"] for d in creds[0]["digests"]},
            {d["digest"] for d in creds[1]["digests"]})

    # -- 版本前提与幂等 ------------------------------------------------------
    def test_stale_base_version_rejected(self):
        self.select("s1", self.A)
        self.expect_error(
            lambda: self.lg.apply_change(
                {"request_id": "s2", "base_version": 0,
                 "select": [{"doc_id": "I", "release_id": "R1"}]}),
            "STALE_BASE_VERSION")

    def test_stale_list_version_rejected(self):
        self.select("s1", self.A)
        v = self.lg.version
        self.confirm("c1", [self.A])
        self.expect_error(
            lambda: self.lg.confirm(
                {"request_id": "c2", "base_version": self.lg.version,
                 "list_version": v, "doc_ids": [self.A]}),
            "STALE_LIST_VERSION")

    def test_request_replay_returns_original_receipt(self):
        rec1 = self.select("s1", self.A)
        self.select("s2", "I")  # 版本前进
        replay = self.lg.apply_change(
            {"request_id": "s1", "base_version": 0,
             "select": [{"doc_id": self.A, "release_id": "R1"}]})
        self.assertEqual(replay, rec1)
        self.assertEqual(self.lg.version, rec1["version"] + 1)

    def test_request_id_conflict(self):
        self.select("s1", self.A)
        self.expect_error(
            lambda: self.lg.apply_change(
                {"request_id": "s1", "base_version": 0, "withdraw": [self.A]}),
            "REQUEST_ID_CONFLICT")

    # -- 诊断、对照与崩溃恢复 -------------------------------------------------
    def test_diagnostics_locality(self):
        self.select("s1", self.A, self.B)
        self.select("s2", "E", "F")
        rec = self.apply("s3", evidence_register=[
            {"evidence_id": "E1", "doc_a": self.A, "doc_b": self.B,
             "provenance": "x"}])
        diag = rec["diagnostics"]
        self.assertEqual(diag["mode"], "incremental")
        self.assertEqual(diag["upstream_releases_read"], [])
        # 证据登记需点查两个端点是已知档号；不读取任何发布明细
        self.assertEqual(diag["upstream_docs_read"],
                         sorted([self.A, self.B]))
        self.assertEqual(len(diag["components_touched"]), 2)
        self.assertEqual(diag["members_recomputed"], 2)

    def test_verify_consistent_after_mixed_ops(self):
        self._lock_a_b()
        self.apply("s2", select=[{"doc_id": "C", "release_id": "R1"}],
                   evidence_register=[
                       {"evidence_id": "E1", "doc_a": "C", "doc_b": self.B,
                        "provenance": "p"}])
        self.apply("s3", evidence_revoke=["E1"])
        self.apply("s4", select=[{"doc_id": self.A, "release_id": "R2"}])
        self.apply("s5", withdraw=[self.A])
        self.apply("s6", select=[{"doc_id": "D", "release_id": "R1"},
                                 {"doc_id": "G", "release_id": "R1"},
                                 {"doc_id": "H", "release_id": "R1"}])
        result = self.lg.verify()
        self.assertTrue(result["consistent"], result["differences"])
        full = result["full_rebuild_diagnostics"]
        self.assertEqual(full["mode"], "full")
        # 在账档号共 6 篇（A/B/C/D/G/H），全量重算覆盖全部成员
        self.assertEqual(full["members_recomputed"], 6)

    def test_crash_mid_write_leaves_no_partial_state(self):
        self.select("s1", self.A)
        v = self.lg.version
        db = os.path.join(self.ledger_dir, "ledger.db")
        code = (
            "import os, sqlite3\n"
            f"conn = sqlite3.connect({db!r})\n"
            "conn.isolation_level = None\n"
            "conn.execute('BEGIN IMMEDIATE')\n"
            "conn.execute(\"INSERT INTO lg_selection VALUES "
            "('GHOST','R1','rev-1',1,'selected','x',999)\")\n"
            "conn.execute(\"UPDATE lg_meta SET value='999' "
            "WHERE key='ledger_version'\")\n"
            "os._exit(1)\n"
        )
        subprocess.run([sys.executable, "-I", "-c", code], check=False,
                       capture_output=True)
        with Ledger(self.ledger_dir) as lg:
            self.assertEqual(lg.version, v)
            self.assertIsNone(lg.conn.execute(
                "SELECT 1 FROM lg_selection WHERE doc_id='GHOST'").fetchone())
            self.assertEqual(lg.status()["docs_selected"], 1)

    def test_upstream_digest_mismatch_detected(self):
        ws2 = os.path.join(self.tmp, "ws2")
        write_synthetic_workspace(ws2, {"K": "draft-k"}, [
            {"release_id": "RK", "rev_id": "rev-k", "rev_seq": 1,
             "docs": {"K": [frag("k1", "some-fragment-text")]}}])
        import sqlite3
        conn = sqlite3.connect(os.path.join(ws2, "workspace.db"))
        conn.execute("UPDATE release_fragment SET text_sha256='0'*64")
        conn.commit()
        conn.close()
        ledger2 = os.path.join(self.tmp, "ledger2")
        init_ledger(ledger2, ws2, layer="clause", min_segment_bytes=8,
                    exempt_digests=[])
        with Ledger(ledger2) as lg2:
            with self.assertRaises(LedgerError) as ctx:
                lg2.apply_change(
                    {"request_id": "k1", "base_version": 0,
                     "select": [{"doc_id": "K", "release_id": "RK"}]})
            self.assertEqual(ctx.exception.code, "UPSTREAM_DIGEST_MISMATCH")


class LedgerCliCase(unittest.TestCase):
    """可脚本化入口：qc-ledger 命令行全流程。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ledger-cli-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        docs = {"D1": "draft-d1", "D2": "draft-d2"}
        write_synthetic_workspace(os.path.join(self.tmp, "ws"), docs, [
            {"release_id": "R1", "rev_id": "rev-1", "rev_seq": 1,
             "docs": {"D1": [frag("u1", "d1-fragment-text")],
                      "D2": [frag("u2", "d2-fragment-text")]}},
        ])
        self.ledger_dir = os.path.join(self.tmp, "ledger")

    def _run_cli(self, argv):
        from qc.ledger_cli import main
        out = StringIO()
        with redirect_stdout(out):
            rc = main(argv)
        return rc, json.loads(out.getvalue())

    def test_cli_flow(self):
        rc, init_out = self._run_cli([
            "init", "--ledger", self.ledger_dir,
            "--workspace", os.path.join(self.tmp, "ws"),
            "--layer", "clause", "--min-bytes", "8",
            "--exempt", self._write_json("exempt.json",
                                         {"texts": [SIGNOFF]})])
        self.assertEqual(rc, 0)
        self.assertEqual(init_out["exempt_digests"], [EXEMPT])

        batch = self._write_json("b1.json", {
            "request_id": "b1", "base_version": 0,
            "select": [{"doc_id": "D1", "release_id": "R1"},
                       {"doc_id": "D2", "release_id": "R1"}]})
        rc, rec = self._run_cli(
            ["apply", "--ledger", self.ledger_dir, "--batch", batch])
        self.assertEqual(rc, 0)
        self.assertEqual(rec["version"], 1)
        # 重放同一批：取回原回执
        rc, replay = self._run_cli(
            ["apply", "--ledger", self.ledger_dir, "--batch", batch])
        self.assertEqual(rc, 0)
        self.assertEqual(replay, rec)

        confirm = self._write_json("c1.json", {
            "request_id": "c1", "base_version": 1, "list_version": 1,
            "doc_ids": ["D1"]})
        rc, crec = self._run_cli(
            ["confirm", "--ledger", self.ledger_dir, "--request", confirm])
        self.assertEqual(rc, 0)
        self.assertEqual(crec["deliveries"][0]["doc_id"], "D1")

        out_dir = os.path.join(self.tmp, "out")
        rc, manifest = self._run_cli(
            ["lists", "--ledger", self.ledger_dir, "--out", out_dir])
        self.assertEqual(rc, 0)
        # D1/D2 各自独立分量，按档号哈希分仓；D1 已确认交付并落锁
        bins = {d: bin_of(d) for d in ("D1", "D2")}
        expect = {"train": 0, "eval": 0, "quarantine": 0}
        for d, b in bins.items():
            expect["train" if b == 0 else "eval"] += 1
        self.assertEqual(manifest["counts"], expect)
        first = "train" if bins["D1"] == 0 else "eval"
        with open(os.path.join(out_dir, f"{first}.jsonl"),
                  encoding="utf-8") as fh:
            rows = [json.loads(line) for line in fh if line.strip()]
        d1_rows = [r for r in rows if r["doc_id"] == "D1"]
        self.assertEqual(len(d1_rows), 1)
        self.assertEqual(d1_rows[0]["release_id"], "R1")
        self.assertEqual(d1_rows[0]["rev_id"], "rev-1")

        rc, status = self._run_cli(["status", "--ledger", self.ledger_dir])
        self.assertEqual(rc, 0)
        self.assertEqual(status["ledger_version"], 2)
        self.assertEqual(status["locks"], 1)

        rc, cred = self._run_cli(
            ["credentials", "--ledger", self.ledger_dir, "--doc", "D1"])
        self.assertEqual(rc, 0)
        self.assertEqual(len(cred["deliveries"]), 1)

        rc, verify = self._run_cli(["verify", "--ledger", self.ledger_dir])
        self.assertEqual(rc, 0)
        self.assertTrue(verify["consistent"])

    def _write_json(self, name, obj):
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False)
        return path


if __name__ == "__main__":
    unittest.main()
