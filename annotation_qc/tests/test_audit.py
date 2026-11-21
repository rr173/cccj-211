# -*- coding: utf-8 -*-
"""发布后独立盲审单元测试（标准库 unittest，python -I 下运行）。

覆盖：
- 抽样总体 = 发布内含正式片段/引用的不同公文；不放回抽取恰好 k 篇；
  总体不足整次拒绝且不生成空壳批次；样本不随 SQLite 行序/输入排列漂移；
- 同发布/样本量/种子同一批次；请求 ID 幂等与参数冲突；
- 批次快照可重开：发布绑定、抽样名单、抽中公文全部正式片段与引用；
- 盲审导出不含团队/投票/裁决来源/理由/正式身份；引用端点用本批盲审标识；
- 提交按本批盲审标识：符合/发现问题，问题证据按底稿摘要核对边界与原文；
- 越批标识、重复主体、非法坐标、文本不符整批拒绝且不部分写入；
- 成功提交幂等重放；并发提交只落一次；中断提交不留半批；
- 审计只读，不回写发布/工作区；旧批次在后续发布后仍显示原内容。
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.audit import (  # noqa: E402
    batch_coverage,
    blind_leaks,
    blind_source,
    create_batch,
    get_batch,
    list_batches,
    sample_score,
    stream_blind,
    stream_results,
    stream_snapshot,
    stream_unreviewed,
    submit_results,
)
from qc.audit_common import AuditError, open_audit  # noqa: E402
from qc.ws_common import connect, resolve_revision  # noqa: E402
from qc.ws_merge import merge  # noqa: E402
from qc.ws_release import release  # noqa: E402
from qc.ws_source import WorkspaceSources  # noqa: E402
from qc.ws_view import view_revision  # noqa: E402
from tools import audit_fixture, audit_reference  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _rows(stream):
    return [json.loads(line) for line in stream]


class AuditTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="qcaudit-")
        cls.info = audit_fixture.generate(os.path.join(cls.tmpdir, "src"))
        cls.ws = cls.info["workspace"]
        cls.manifest = cls.info["manifest"]
        cls.release_id = cls.info["release"]["release_id"]

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def audit_dir(self, name):
        path = os.path.join(self.tmpdir, "audit-" + name)
        shutil.rmtree(path, ignore_errors=True)
        return path

    def store(self, name):
        path = self.audit_dir(name)
        return path, open_audit(path)

    def create(self, conn, audit_dir, rid="r1", *, size=3, seed=7,
               release_id=None):
        return create_batch(audit_dir, conn, self.ws, {
            "request_id": rid,
            "release_id": release_id or self.release_id,
            "sample_size": size, "seed": seed})

    def submit(self, audit_dir, conn, rid, batch_id, results):
        return submit_results(
            audit_dir, conn,
            {"request_id": rid, "batch_id": batch_id, "results": results},
            manifest_path=self.manifest)

    # -- 1. 抽样 -----------------------------------------------------

    def test_01_sample_population_and_size(self):
        audit_dir, conn = self.store("pop")
        try:
            rec = self.create(conn, audit_dir, size=3)
            self.assertEqual(rec["population_size"], 4)
            self.assertEqual(len(rec["sample_doc_ids"]), 3)
            self.assertEqual(len(set(rec["sample_doc_ids"])), 3)
            # 参考实现独立复算
            ref = audit_reference.sample(self.ws, self.release_id, 3, 7)
            self.assertEqual(sorted(ref["sample"]), sorted(rec["sample_doc_ids"]))
        finally:
            conn.close()

    def test_02_sample_size_exceeds_population_rejected(self):
        audit_dir, conn = self.store("too-big")
        try:
            with self.assertRaises(AuditError) as ctx:
                self.create(conn, audit_dir, size=5)
            self.assertEqual(ctx.exception.code, "SAMPLE_TOO_LARGE")
            self.assertEqual(list_batches(conn), [])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_request").fetchone()["c"], 0)
        finally:
            conn.close()

    def test_03_invalid_sample_size_rejected(self):
        audit_dir, conn = self.store("bad-size")
        try:
            for size in (0, -1):
                with self.assertRaises(AuditError) as ctx:
                    self.create(conn, audit_dir, size=size)
                self.assertEqual(ctx.exception.code, "BAD_REQUEST")
            with self.assertRaises(AuditError) as ctx:
                create_batch(audit_dir, conn, self.ws, {
                    "request_id": "x", "release_id": self.release_id,
                    "sample_size": "3", "seed": 7})
            self.assertEqual(ctx.exception.code, "BAD_REQUEST")
            self.assertEqual(list_batches(conn), [])
        finally:
            conn.close()

    def test_04_seed_changes_sample_but_same_shape(self):
        audit_dir, conn = self.store("seed")
        try:
            a = self.create(conn, audit_dir, "r1", seed=11)
            b = self.create(conn, audit_dir, "r2", seed=12)
            self.assertNotEqual(a["batch_id"], b["batch_id"])
            self.assertEqual(a["population_size"], b["population_size"])
            self.assertEqual(len(a["sample_doc_ids"]), 3)
            self.assertEqual(len(b["sample_doc_ids"]), 3)
        finally:
            conn.close()

    def test_05_same_params_same_batch_and_request_idempotent(self):
        audit_dir, conn = self.store("idem")
        try:
            a = self.create(conn, audit_dir, "r1")
            replay = self.create(conn, audit_dir, "r1")
            self.assertEqual(replay["batch_id"], a["batch_id"])
            self.assertTrue(replay["replayed"])
            again = self.create(conn, audit_dir, "r1-different-id")
            self.assertEqual(again["batch_id"], a["batch_id"])
            # 不同请求 ID、同一参数：重开同一批次（不重抽、不新建）
            self.assertTrue(again["replayed"])
            self.assertEqual(len(list_batches(conn)), 1)
            with self.assertRaises(AuditError) as ctx:
                self.create(conn, audit_dir, "r1", seed=99)
            self.assertEqual(ctx.exception.code, "REQUEST_ID_CONFLICT")
        finally:
            conn.close()

    def test_06_sample_stable_under_reversed_source_order(self):
        rev_root = os.path.join(self.tmpdir, "src-rev")
        rev = audit_fixture.generate(rev_root, reverse=True)
        audit_dir, conn = self.store("rev")
        try:
            rec = create_batch(audit_dir, conn, rev["workspace"], {
                "request_id": "r1",
                "release_id": rev["release"]["release_id"],
                "sample_size": 3, "seed": 7})
            # 逆序构建库的发布 ID 会变（构建库摘要不同），但选中集合按
            # 档号稳定：与正向库上的参考实现结果一致
            ref = audit_reference.sample(self.ws, self.release_id, 3, 7)
            self.assertEqual(sorted(ref["sample"]),
                             sorted(rec["sample_doc_ids"]))
            ref_rev = audit_reference.sample(
                rev["workspace"], rev["release"]["release_id"], 3, 7)
            self.assertEqual(sorted(ref_rev["sample"]),
                             sorted(rec["sample_doc_ids"]))
        finally:
            conn.close()

    def test_07_scores_only_depend_on_seed_and_doc(self):
        self.assertEqual(sample_score(5, "D1"), sample_score(5, "D1"))
        self.assertNotEqual(sample_score(5, "D1"), sample_score(6, "D1"))
        self.assertNotEqual(sample_score(5, "D1"), sample_score(5, "D2"))

    # -- 2. 批次快照与盲审导出 --------------------------------------

    def _batch_with_all(self, name):
        audit_dir, conn = self.store(name)
        rec = self.create(conn, audit_dir, "r1", size=4)
        return audit_dir, conn, rec

    def test_08_snapshot_covers_all_official_records_of_sampled_docs(self):
        audit_dir, conn, rec = self._batch_with_all("snap")
        try:
            snapshot = _rows(stream_snapshot(conn, rec["batch_id"]))
            header = snapshot[0]
            self.assertEqual(header["release_id"], self.release_id)
            docs = [r for r in snapshot if r["record_kind"] == "document"]
            self.assertEqual(sorted(r["doc_id"] for r in docs),
                             ["D1", "D2", "D4", "D5"])
            # D1 在发布中有 3 条片段、2 条引用；必须全部列出
            d1_frags = [r for r in snapshot if r["record_kind"] == "fragment"
                        and r["doc_id"] == "D1"]
            d1_rels = [r for r in snapshot if r["record_kind"] == "relation"
                       and r["doc_id"] == "D1"]
            self.assertEqual(len(d1_frags), 3)
            self.assertEqual(len(d1_rels), 2)
            # 抽中公文有正式片段但无引用时也不能丢失
            self.assertTrue(any(r["record_kind"] == "document"
                                and r["doc_id"] == "D4" for r in snapshot))
        finally:
            conn.close()

    def test_09_blind_export_hides_identity_and_provenance(self):
        audit_dir, conn, rec = self._batch_with_all("blind")
        try:
            rows = _rows(stream_blind(conn, rec["batch_id"]))
            for row in rows:
                self.assertEqual(blind_leaks(row), [])
            text = "".join(stream_blind(conn, rec["batch_id"]))
            for forbidden in ("D1", "D2", "release_", "rev_", "team",
                              "\"reason\"", "\"origin\"", "\"ticket_uid\"",
                              "\"doc_id\"", "\"fragment_uid\""):
                self.assertNotIn(forbidden, text)
            # 引用端点用本批盲审片段标识，且能映射到片段
            relations = [r for r in rows if r["record_kind"] == "relation"]
            self.assertTrue(relations)
            frag_ids = {r["blind_fragment_id"] for r in rows
                        if r["record_kind"] == "fragment"}
            for rel in relations:
                self.assertIn(rel["from"], frag_ids)
                self.assertIn(rel["to"], frag_ids)
        finally:
            conn.close()

    def test_10_blind_ids_are_batch_local(self):
        audit_dir, conn = self.store("local")
        try:
            a = self.create(conn, audit_dir, "r1", seed=7)
            b = self.create(conn, audit_dir, "r2", seed=8)
            a_ids = {r["blind_doc_id"] for r in
                     _rows(stream_blind(conn, a["batch_id"]))
                     if r["record_kind"] == "document"}
            b_ids = {r["blind_doc_id"] for r in
                     _rows(stream_blind(conn, b["batch_id"]))
                     if r["record_kind"] == "document"}
            self.assertFalse(a_ids & b_ids)
            # 同一正式公文的盲审标识在不同批次不同
            self.assertNotEqual(
                next(iter(a_ids)), next(iter(b_ids)))
        finally:
            conn.close()

    def test_11_blind_span_is_exact(self):
        audit_dir, conn, rec = self._batch_with_all("span")
        try:
            rows = _rows(stream_blind(conn, rec["batch_id"]))
            frag = next(r for r in rows if r["record_kind"] == "fragment")
            got = blind_source(conn, rec["batch_id"], frag["blind_doc_id"],
                               frag["start"], frag["end"],
                               manifest_path=self.manifest)
            self.assertEqual(got["text"], frag["text"])
            self.assertEqual(got["text_sha256"], frag["text_sha256"])
        finally:
            conn.close()

    # -- 3. 提交 -----------------------------------------------------

    def _sample_subjects(self, conn, rec):
        rows = _rows(stream_blind(conn, rec["batch_id"]))
        return rows

    def test_12_submit_conforming_and_issue_with_evidence(self):
        audit_dir, conn, rec = self._batch_with_all("submit")
        try:
            rows = self._sample_subjects(conn, rec)
            doc = next(r for r in rows if r["record_kind"] == "document")
            frag = next(r for r in rows if r["record_kind"] == "fragment"
                        and r["blind_doc_id"] == doc["blind_doc_id"])
            receipt = self.submit(audit_dir, conn, "s1", rec["batch_id"], [
                {"subject": {"kind": "document",
                             "blind_id": doc["blind_doc_id"]},
                 "verdict": "conforming", "note": "通读无异常"},
                {"subject": {"kind": "fragment",
                             "blind_id": frag["blind_fragment_id"]},
                 "verdict": "issue", "note": "标签与正文不符",
                 "evidence": {"blind_doc_id": doc["blind_doc_id"],
                              "start": frag["start"], "end": frag["end"],
                              "text": frag["text"]}},
            ])
            self.assertEqual(receipt["result_count"], 2)
            self.assertEqual(receipt["issue_count"], 1)
            results = _rows(stream_results(conn, rec["batch_id"]))
            issue = next(r for r in results if r["verdict"] == "issue")
            self.assertIsNotNone(issue["evidence"])
            self.assertEqual(issue["evidence"]["text"], frag["text"])
            self.assertNotEqual(issue["real_id"], frag["blind_fragment_id"])
        finally:
            conn.close()

    def test_13_submit_idempotent_replay(self):
        audit_dir, conn, rec = self._batch_with_all("replay")
        try:
            doc = next(r for r in _rows(stream_blind(conn, rec["batch_id"]))
                       if r["record_kind"] == "document")
            results = [{"subject": {"kind": "document",
                                    "blind_id": doc["blind_doc_id"]},
                        "verdict": "conforming", "note": "无异常"}]
            first = self.submit(audit_dir, conn, "s1", rec["batch_id"],
                                results)
            second = self.submit(audit_dir, conn, "s1", rec["batch_id"],
                                 results)
            self.assertTrue(second["replayed"])
            self.assertFalse(first["replayed"])
            self.assertEqual(
                {k: v for k, v in first.items() if k != "replayed"},
                {k: v for k, v in second.items() if k != "replayed"})
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_result").fetchone()["c"], 1)
            # 同一请求 ID 不同内容 -> 冲突
            with self.assertRaises(AuditError) as ctx:
                self.submit(audit_dir, conn, "s1", rec["batch_id"], [
                    {"subject": {"kind": "document",
                                 "blind_id": doc["blind_doc_id"]},
                     "verdict": "issue", "note": "改判",
                     "evidence": {"blind_doc_id": doc["blind_doc_id"],
                                  "start": 0, "end": 4, "text": "XXXX"}}])
            self.assertEqual(ctx.exception.code, "REQUEST_ID_CONFLICT")
        finally:
            conn.close()

    def test_14_submit_rejects_and_writes_nothing(self):
        audit_dir, conn, rec = self._batch_with_all("reject")
        try:
            rows = _rows(stream_blind(conn, rec["batch_id"]))
            docs = [r for r in rows if r["record_kind"] == "document"]
            frag = next(r for r in rows if r["record_kind"] == "fragment")
            good = {"subject": {"kind": "document",
                                "blind_id": docs[0]["blind_doc_id"]},
                    "verdict": "conforming", "note": "无异常"}
            # 找到抽中文档中第一个多字节字符的起始字节，构造切在字符中段
            full = blind_source(conn, rec["batch_id"],
                                docs[1]["blind_doc_id"],
                                manifest_path=self.manifest)["text"]
            split_start = None
            offset = 0
            for ch in full:
                if len(ch.encode("utf-8")) > 1:
                    split_start = offset + 1
                    break
                offset += len(ch.encode("utf-8"))
            self.assertIsNotNone(split_start, "fixture must contain CJK")
            split_start = split_start - 1  # 落在该字符首字节之后 1 字节
            before = conn.execute(
                "SELECT COUNT(*) c FROM aud_result").fetchone()["c"]
            cases = [
                ([good, good], "DUPLICATE_RESULT"),
                ([{"subject": {"kind": "fragment", "blind_id": "bf_oth"},
                   "verdict": "conforming", "note": "x"}],
                 "SUBJECT_NOT_IN_BATCH"),
                ([{"subject": {"kind": "document",
                               "blind_id": docs[1]["blind_doc_id"]},
                   "verdict": "issue", "note": "坏边界",
                   "evidence": {"blind_doc_id": docs[1]["blind_doc_id"],
                                "start": split_start, "end": split_start + 1,
                                "text": "?"}}],
                 "BAD_EVIDENCE_RANGE"),
                ([{"subject": {"kind": "document",
                               "blind_id": docs[1]["blind_doc_id"]},
                   "verdict": "issue", "note": "文本不符",
                   "evidence": {"blind_doc_id": docs[1]["blind_doc_id"],
                                "start": 0, "end": 4, "text": "ZZZZ"}}],
                 "EVIDENCE_TEXT_MISMATCH"),
                ([{"subject": {"kind": "document",
                               "blind_id": docs[1]["blind_doc_id"]},
                   "verdict": "issue", "note": "无证据"}],
                 "EVIDENCE_REQUIRED"),
                ([{"subject": {"kind": "document",
                               "blind_id": docs[1]["blind_doc_id"]},
                   "verdict": "maybe", "note": "x"}],
                 "BAD_VERDICT"),
                # 同批有效 + 无效 -> 整批不写入
                ([{"subject": {"kind": "fragment",
                               "blind_id": frag["blind_fragment_id"]},
                   "verdict": "conforming", "note": "有效"}, good,
                  {"subject": {"kind": "fragment", "blind_id": "bf_oth"},
                   "verdict": "conforming", "note": "越批"}],
                 "SUBJECT_NOT_IN_BATCH"),
            ]
            for i, (results, code) in enumerate(cases):
                with self.assertRaises(AuditError) as ctx:
                    self.submit(audit_dir, conn, f"bad{i}", rec["batch_id"],
                                results)
                self.assertEqual(ctx.exception.code, "SUBMISSION_REJECTED")
                self.assertTrue(any(e["code"] == code
                                    for e in ctx.exception.errors),
                                f"missing {code}: {ctx.exception.errors}")
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) c FROM aud_result").fetchone()["c"],
                    before)
        finally:
            conn.close()

    def test_15_cross_batch_blind_id_rejected(self):
        audit_dir, conn = self.store("cross")
        try:
            a = self.create(conn, audit_dir, "r1", seed=7)
            b = self.create(conn, audit_dir, "r2", seed=8)
            a_doc = next(r for r in _rows(stream_blind(conn, a["batch_id"]))
                         if r["record_kind"] == "document")
            with self.assertRaises(AuditError) as ctx:
                self.submit(audit_dir, conn, "s-cross", b["batch_id"], [
                    {"subject": {"kind": "document",
                                 "blind_id": a_doc["blind_doc_id"]},
                     "verdict": "conforming", "note": "越批"}])
            self.assertEqual(ctx.exception.code, "SUBMISSION_REJECTED")
            self.assertTrue(any(e["code"] == "SUBJECT_NOT_IN_BATCH"
                                for e in ctx.exception.errors))
        finally:
            conn.close()

    def test_16_reviewer_and_unreviewed_queries(self):
        audit_dir, conn, rec = self._batch_with_all("views")
        try:
            docs = [r for r in _rows(stream_blind(conn, rec["batch_id"]))
                    if r["record_kind"] == "document"]
            self.submit(audit_dir, conn, "s1", rec["batch_id"], [
                {"subject": {"kind": "document",
                             "blind_id": docs[0]["blind_doc_id"]},
                 "verdict": "conforming", "note": "无异常"}])
            cov = batch_coverage(conn, rec["batch_id"])
            self.assertEqual(cov["totals"]["documents_reviewed"], 1)
            self.assertGreaterEqual(cov["totals"]["unreviewed"], 1)
            unreviewed = _rows(stream_unreviewed(conn, rec["batch_id"]))
            self.assertTrue(unreviewed)
            self.assertTrue(all("blind_doc_id" in r for r in unreviewed))
            self.assertFalse(any(r.get("doc_id") == docs[0]["blind_doc_id"]
                                 for r in unreviewed))
        finally:
            conn.close()

    # -- 4. 独立性与不可回写 -----------------------------------------

    def test_17_old_batch_survives_new_release(self):
        audit_dir, conn, rec = self._batch_with_all("immutable")
        try:
            before_blind = "".join(stream_blind(conn, rec["batch_id"]))
            before_snap = "".join(stream_snapshot(conn, rec["batch_id"]))
            # 修改工作区并另发新发布（独立连接）
            self._evolve_workspace()
            conn.close()
            conn = open_audit(audit_dir)
            self.assertEqual("".join(stream_blind(conn, rec["batch_id"])),
                             before_blind)
            self.assertEqual("".join(stream_snapshot(conn, rec["batch_id"])),
                             before_snap)
            info = get_batch(conn, rec["batch_id"])
            self.assertEqual(info["release_id"], self.release_id)
            # 发布本身未被审计回写
            wsc = connect(self.ws)
            try:
                n = wsc.execute(
                    "SELECT COUNT(*) c FROM release WHERE release_id=?",
                    (self.release_id,)).fetchone()["c"]
                self.assertEqual(n, 1)
            finally:
                wsc.close()
        finally:
            conn.close()

    def test_18_upstream_release_not_modified_by_audit(self):
        audit_dir, conn = self.store("ro")
        try:
            wsc = connect(self.ws)
            try:
                before = wsc.execute(
                    "SELECT COUNT(*) c FROM release").fetchone()["c"]
            finally:
                wsc.close()
            self.create(conn, audit_dir)
            wsc = connect(self.ws)
            try:
                self.assertEqual(wsc.execute(
                    "SELECT COUNT(*) c FROM release").fetchone()["c"], before)
                # 审计库与原库不同文件
                self.assertNotEqual(
                    os.path.realpath(os.path.join(audit_dir, "audit.db")),
                    os.path.realpath(os.path.join(self.ws, "workspace.db")))
            finally:
                wsc.close()
        finally:
            conn.close()

    def _evolve_workspace(self):
        conn = connect(self.ws)
        try:
            docs = view_revision(conn, "HEAD", doc_id="D1")["documents"]["D1"]
            by_uid = {f["fragment_uid"]: f for f in docs["fragments"]}
            region0 = next(
                r["region_uid"] for r in docs["regions"]
                if r["layer"] == "clause" and r["fragments"]
                and any(f["fragment_uid"] in r["fragments"] and f["start"] == 0
                        for f in docs["fragments"]))
            region_frags = next(r["fragments"] for r in docs["regions"]
                                if r["region_uid"] == region0)
            start_frag = next(u for u in region_frags
                              if by_uid[u]["start"] == 0)
            y = next(f["fragment_uid"] for f in docs["fragments"]
                     if f["layer"] == "annex" and f["start"] == 20)
            other_clause = next(f["fragment_uid"] for f in docs["fragments"]
                                if f["layer"] == "clause" and f["start"] == 10)
            ticket = {
                "reviewer": "curator",
                "base": resolve_revision(conn, "HEAD")["rev_id"],
                "regions": [{"region": region0, "decision": "fragments",
                             "reason": "拆分为两段",
                             "fragments": [
                                 {"ref": "A", "start": 0, "end": 4,
                                  "label": "L1"},
                                 {"ref": "B", "start": 4, "end": 8,
                                  "label": "L1"}]}],
                "relations": [
                    {"doc": "D1", "type": "ref", "from": start_frag, "to": y,
                     "decision": "retired", "reason": "拆分后失效"},
                    {"doc": "D1", "type": "ref", "from": other_clause, "to": y,
                     "decision": "retired", "reason": "同批退役"},
                ],
            }
            result = merge(conn, WorkspaceSources(conn, manifest_path=self.manifest),
                           self.ws, data=ticket)
            self.assertEqual(result["status"], "merged")
            rel = release(conn, self.ws, rev="HEAD",
                          doc_ids=["D1", "D2", "D4", "D5"])
            self.assertTrue(rel["released"])
        finally:
            conn.close()

    # -- 5. 并发与中断 ----------------------------------------------

    def test_19_concurrent_create_same_request(self):
        audit_dir = self.audit_dir("race")
        open_audit(audit_dir).close()
        q = mp.Queue()
        procs = [mp.Process(target=_create_worker,
                            args=(audit_dir, self.ws, self.release_id, q))
                 for _ in range(2)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(30)
        results = [q.get() for _ in procs]
        self.assertEqual({r[0] for r in results}, {None})
        self.assertEqual(len({r[1] for r in results}), 1)
        conn = open_audit(audit_dir)
        try:
            self.assertEqual(len(list_batches(conn)), 1)
        finally:
            conn.close()

    def test_19b_concurrent_submit_yields_one_submission(self):
        audit_dir, conn = self.store("race-submit")
        try:
            rec = self.create(conn, audit_dir, "r1", size=2)
            rows = _rows(stream_blind(conn, rec["batch_id"]))
            doc = next(r for r in rows if r["record_kind"] == "document")
            batch_id = rec["batch_id"]
        finally:
            conn.close()
        payload = json.dumps([
            {"subject": {"kind": "document",
                         "blind_id": doc["blind_doc_id"]},
             "verdict": "conforming", "note": "无异常"}])
        q = mp.Queue()
        procs = [mp.Process(target=_submit_worker,
                            args=(audit_dir, self.manifest, batch_id,
                                  payload, q))
                 for _ in range(2)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(30)
        results = [q.get() for _ in procs]
        self.assertEqual({r[0] for r in results}, {None})
        # 两次都成功，但只留下一次提交与一条结果
        self.assertEqual(len({r[1] for r in results}), 1)
        conn = open_audit(audit_dir)
        try:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_submission").fetchone()["c"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_result").fetchone()["c"], 1)
        finally:
            conn.close()

    def test_20_interrupted_create_leaves_no_partial_batch(self):
        audit_dir, conn = self.store("crash")
        try:
            before = len(list_batches(conn))
        finally:
            conn.close()
        script = f"""
import os, sys, time
sys.path.insert(0, {ROOT!r})
from qc.audit_common import open_audit
from qc import audit
audit_dir, ws, release_id = {audit_dir!r}, {self.ws!r}, {self.release_id!r}
conn = open_audit(audit_dir)
orig = audit._snapshot_fragments
def slow(*a, **kw):
    conn.execute("SELECT 1")
    time.sleep(10)
    return orig(*a, **kw)
audit._snapshot_fragments = slow
try:
    audit.create_batch(audit_dir, conn, ws,
        {{"request_id": "crashed", "release_id": release_id,
          "sample_size": 2, "seed": 5}})
except BaseException:
    pass
os._exit(9)
"""
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                cwd=ROOT, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        time.sleep(1.0)
        proc.kill()
        proc.communicate(timeout=30)
        conn = open_audit(audit_dir)
        try:
            self.assertEqual(len(list_batches(conn)), before)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_request").fetchone()["c"], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_fragment").fetchone()["c"], 0)
        finally:
            conn.close()
        conn = open_audit(audit_dir)
        try:
            rec = self.create(conn, audit_dir, "crashed", size=2, seed=5)
            self.assertEqual(len(list_batches(conn)), before + 1)
            self.assertTrue(rec["batch_id"])
        finally:
            conn.close()

    def test_21_interrupted_submit_leaves_no_partial_results(self):
        audit_dir, conn = self.store("crash-submit")
        rec = self.create(conn, audit_dir, "r1", size=2)
        rows = _rows(stream_blind(conn, rec["batch_id"]))
        doc = next(r for r in rows if r["record_kind"] == "document")
        results = [{"subject": {"kind": "document",
                                "blind_id": doc["blind_doc_id"]},
                    "verdict": "conforming", "note": "无异常"}]
        conn.close()
        script = f"""
import json, os, sys, time
sys.path.insert(0, {ROOT!r})
from qc.audit_common import open_audit
from qc import audit
audit_dir = {audit_dir!r}
conn = open_audit(audit_dir)
req = {json.dumps({"request_id": "s-crash", "batch_id": rec["batch_id"],
                   "results": results})!r}
orig = audit._validate_results
def slow(*a, **kw):
    time.sleep(10)
    return orig(*a, **kw)
audit._validate_results = slow
try:
    audit.submit_results(audit_dir, conn, json.loads(req),
                         manifest_path={self.manifest!r})
except BaseException:
    pass
os._exit(9)
"""
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                cwd=ROOT, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        time.sleep(1.0)
        proc.kill()
        proc.communicate(timeout=30)
        conn = open_audit(audit_dir)
        try:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_result").fetchone()["c"], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_submission").fetchone()["c"], 0)
            # 重试成功
            ok = self.submit(audit_dir, conn, "s-crash", rec["batch_id"],
                             results)
            self.assertEqual(ok["result_count"], 1)
        finally:
            conn.close()

    # -- 5b. 边界情形 ------------------------------------------------

    def test_21b_empty_population_rejected(self):
        ws = _minimal_workspace(
            os.path.join(self.tmpdir, "empty-ws"), docs={"E1": "ABCDEFGH"},
            fragments={}, relations={})
        audit_dir, conn = self.store("empty")
        try:
            with self.assertRaises(AuditError) as ctx:
                create_batch(audit_dir, conn, ws, {
                    "request_id": "r1", "release_id": "release_empty",
                    "sample_size": 1, "seed": 1})
            self.assertEqual(ctx.exception.code, "EMPTY_POPULATION")
            self.assertEqual(list_batches(conn), [])
        finally:
            conn.close()

    def test_21c_document_with_relations_but_no_fragments_is_sampled(self):
        # 发布内某公文自身没有正式片段，但有正式引用（端点属于别的公文）：
        # 它仍属于总体，被抽中时引用不能丢失，端点用本批盲审标识表示。
        ws = _minimal_workspace(
            os.path.join(self.tmpdir, "relonly-ws"),
            docs={"A": "AAAA0000", "B": "BBBB1111"},
            fragments={"A": [("fa", "clause", "L1", 0, 8)]},
            # B 在本发布中没有任何 release_fragment 行，只有一条正式引用
            relations=[("B", "ref", "fb_in_B", "fa")],
            release_id="release_relonly")
        audit_dir, conn = self.store("relonly")
        try:
            rec = create_batch(audit_dir, conn, ws, {
                "request_id": "r1", "release_id": "release_relonly",
                "sample_size": 2, "seed": 3})
            self.assertEqual(rec["population_size"], 2)
            snap = _rows(stream_snapshot(conn, rec["batch_id"]))
            b_rels = [r for r in snap if r["record_kind"] == "relation"
                      and r["doc_id"] == "B"]
            self.assertEqual(len(b_rels), 1)
            self.assertTrue(b_rels[0]["blind_from"].startswith("bf_"))
            self.assertTrue(b_rels[0]["blind_to"].startswith("bf_"))
            blind = _rows(stream_blind(conn, rec["batch_id"]))
            # B 无片段但被抽中：其引用仍在盲审导出中出现
            self.assertTrue(any(r["record_kind"] == "relation" for r in blind))
            b_blind_doc = next(
                d["blind_doc_id"] for d in rec_blind_docs(conn, rec)
                if d["doc_id"] == "B")
            self.assertTrue(any(r.get("blind_doc_id") == b_blind_doc
                                and r["record_kind"] == "relation"
                                for r in blind))
        finally:
            conn.close()

    # -- 6. 大批文档 -------------------------------------------------

    def test_22_large_population_sample_and_stream(self):
        from tools import audit_scalegen

        root = os.path.join(self.tmpdir, "scale")
        info = audit_scalegen.generate(root, 4000)
        audit_dir = os.path.join(root, "audit")
        conn = open_audit(audit_dir)
        try:
            rec = create_batch(audit_dir, conn, info["workspace"], {
                "request_id": "big", "release_id": info["release_id"],
                "sample_size": 25, "seed": 99})
            self.assertEqual(rec["population_size"], 4000)
            self.assertEqual(len(rec["sample_doc_ids"]), 25)
            ref = audit_reference.sample(info["workspace"],
                                         info["release_id"], 25, 99)
            self.assertEqual(sorted(ref["sample"]),
                             sorted(rec["sample_doc_ids"]))
            # 抽样名单规模恰为 k，不随总体线性驻留
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM aud_sample_doc").fetchone()["c"], 25)
            rows = _rows(stream_blind(conn, rec["batch_id"]))
            self.assertGreater(len(rows), 25)
        finally:
            conn.close()


def rec_blind_docs(conn, rec):
    return [{"doc_id": r["doc_id"], "blind_doc_id": r["blind_doc_id"]}
            for r in conn.execute(
                "SELECT doc_id, blind_doc_id FROM aud_sample_doc"
                " WHERE batch_id=? ORDER BY sample_rank", (rec["batch_id"],))]


def _minimal_workspace(path, *, docs, fragments, relations,
                       release_id=None, extra_fragment=None):
    """写一个最小上游（release + release_fragment + release_relation +
    source_doc），供边界测试使用。所有片段落在 ASCII 前缀内。"""
    import hashlib
    import sqlite3

    os.makedirs(path, exist_ok=True)
    db = os.path.join(path, "workspace.db")
    if os.path.exists(db):
        os.remove(db)
    conn = sqlite3.connect(db)
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
            CREATE TABLE release_relation (
                release_id TEXT NOT NULL, doc_id TEXT NOT NULL,
                rel_type TEXT NOT NULL, from_fragment TEXT NOT NULL,
                to_fragment TEXT NOT NULL, source_kind TEXT NOT NULL,
                source_rev TEXT NOT NULL, ticket_uid TEXT, reviewer_id TEXT,
                reason TEXT, evidence_json TEXT NOT NULL,
                PRIMARY KEY (release_id, doc_id, rel_type, from_fragment,
                             to_fragment));
            """)
        os.makedirs(os.path.join(path, "sources"), exist_ok=True)
        for doc_id, text in docs.items():
            data = text.encode("utf-8")
            rel_path = f"sources/{doc_id}.txt"
            with open(os.path.join(path, rel_path), "wb") as fh:
                fh.write(data)
            conn.execute(
                "INSERT INTO source_doc(doc_id, rel_path, sha256, byte_length,"
                " verified) VALUES (?,?,?,?,1)",
                (doc_id, rel_path, hashlib.sha256(data).hexdigest(),
                 len(data)))
        rid = release_id or ("release_relonly" if extra_fragment
                             else "release_empty")
        conn.execute(
            "INSERT INTO release(release_id, rev_id, rev_seq, created_at,"
            " doc_ids_json, request_json) VALUES (?,?,?,?,?,?)",
            (rid, "rev_x", 0, "2026-01-01T00:00:00Z",
             json.dumps(sorted(docs)), "{}"))

        def add_fragment(doc_id, uid, layer, label, start, end):
            text = docs[doc_id].encode("utf-8")[start:end].decode("utf-8")
            conn.execute(
                "INSERT INTO release_fragment(release_id, fragment_uid,"
                " doc_id, layer, label, start_byte, end_byte, text_sha256,"
                " seen_text, origin, source_kind, source_rev, evidence_json)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rid, uid, doc_id, layer, label, start, end,
                 hashlib.sha256(text.encode()).hexdigest(), text, "auto",
                 "auto", "rev_x", "[]"))

        for doc_id, frags in fragments.items():
            for uid, layer, label, start, end in frags:
                add_fragment(doc_id, uid, layer, label, start, end)
        if extra_fragment:
            add_fragment(extra_fragment[0], extra_fragment[1],
                         extra_fragment[2], extra_fragment[3],
                         extra_fragment[4], extra_fragment[5])
        for rel in relations:
            conn.execute(
                "INSERT INTO release_relation(release_id, doc_id, rel_type,"
                " from_fragment, to_fragment, source_kind, source_rev,"
                " evidence_json) VALUES (?,?,?,?,?,?,?,?)",
                (rid, rel[0], rel[1], rel[2], rel[3], "auto", "rev_x", "[]"))
        conn.commit()
        return path
    finally:
        conn.close()


def _submit_worker(audit_dir, manifest, batch_id, payload, q):
    try:
        conn = open_audit(audit_dir)
        rec = submit_results(
            audit_dir, conn,
            {"request_id": "race-submit", "batch_id": batch_id,
             "results": json.loads(payload)},
            manifest_path=manifest)
        conn.close()
        q.put((None, rec["submission_id"]))
    except Exception as exc:  # pragma: no cover
        q.put((repr(exc), None))


def _create_worker(audit_dir, ws, release_id, q):
    try:
        conn = open_audit(audit_dir)
        rec = create_batch(audit_dir, conn, ws, {
            "request_id": "race", "release_id": release_id,
            "sample_size": 2, "seed": 3})
        conn.close()
        q.put((None, rec["batch_id"]))
    except Exception as exc:  # pragma: no cover
        q.put((repr(exc), None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
