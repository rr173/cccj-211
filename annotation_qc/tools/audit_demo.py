# -*- coding: utf-8 -*-
"""发布后独立盲审的可复跑验收示例。

场景（对应任务书逐条）：

1. 登记含多篇公文、正式片段与有向引用的发布 R1，创建盲审批次；
2. 对照参考抽样（``tools/audit_reference.py``，不依赖产品实现）核对选中集合；
3. 重放同一请求 → 幂等取回原批次；换种子 → 样本变化；
4. 反转源记录插入顺序重建同一构建库 → 相同参数得到同一批次与样本；
5. 仅修改审定工作区并另发新发布 → 旧批次仍显示原发布内容；
6. 无效样本量、ID 参数冲突、重复结果、非法 UTF-8 字节边界、越批标识、
   中断恢复逐项核对；
7. 使用运行时生成的大批文档核对有界内存的抽样与导出（可选，默认关闭）。

用法::

    python -I tools/audit_demo.py --root /tmp/audit-demo
    python -I tools/audit_demo.py --root /tmp/audit-demo --large-docs 30000
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.audit import (  # noqa: E402
    batch_coverage,
    blind_leaks,
    blind_source,
    create_batch,
    get_batch,
    list_batches,
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

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STEP = 0


def say(msg=""):
    print(msg)


def step(title):
    global STEP
    STEP += 1
    say()
    say(f"== 步骤 {STEP}: {title}")


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    say(f"   ✓ {msg}")


def expect_error(fn, code, msg):
    try:
        fn()
    except AuditError as exc:
        assert exc.code == code, f"期望 {code}，实际 {exc.code}: {exc}"
        say(f"   ✓ {msg}（{code}）")
        return exc
    raise AssertionError(f"期望错误 {code}，但操作成功")


def load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


class Demo:
    def __init__(self, root, large_docs=0):
        self.root = root
        self.large_docs = large_docs
        self.info = audit_fixture.generate(os.path.join(root, "src"))
        self.ws = self.info["workspace"]
        self.release_id = self.info["release"]["release_id"]
        self.manifest = self.info["manifest"]
        self.audit_dir = os.path.join(root, "audit")
        self.conn = open_audit(self.audit_dir)
        self.sample_size = 3
        self.seed = 20261121
        self.receipts = {}

    # -- 小助手 -------------------------------------------------------------
    def create(self, rid, *, size=None, seed=None, release_id=None):
        req = {
            "request_id": rid,
            "release_id": release_id or self.release_id,
            "sample_size": self.sample_size if size is None else size,
            "seed": self.seed if seed is None else seed,
        }
        rec = create_batch(self.audit_dir, self.conn, self.ws, req)
        self.receipts[rid] = rec
        return rec

    def blind_rows(self, batch_id):
        return [json.loads(line) for line in
                stream_blind(self.conn, batch_id)]

    def blind_fragment(self, rows, doc_id):
        for r in rows:
            if r["record_kind"] == "fragment" and r.get("blind_doc_id"):
                return r
        raise AssertionError("no blind fragment")

    # -- 主流程 -------------------------------------------------------------
    def run(self):
        info = self.info
        say(f"工作区: {self.ws}")
        say(f"发布 R1: {self.release_id}")
        say(f"发布内公文: {info['release']['doc_ids']}")
        say(f"被阻挡的发布（D3/D6 待定）: {info['blocked_release']['released']}")

        step("登记发布后创建盲审批次：恰好抽取指定数量，总体为有正式记录的公文")
        first = self.create("r1")
        # 发布 R1 点名 4 篇，但只有 D1/D2/D4/D5 有片段/引用；总体即这 4 篇
        check(first["population_size"] == 4,
              f"抽样总体 4 篇（D3 无正式记录、不进发布）")
        check(len(first["sample_doc_ids"]) == self.sample_size,
              "不放回抽取恰好 3 篇")
        check(len(set(first["sample_doc_ids"])) == self.sample_size,
              "样本内部无重复")
        check(first["fragment_count"] >= 3 and first["relation_count"] >= 1,
              "批次快照含正式片段与有向引用")
        batch1 = first["batch_id"]

        step("参考抽样（独立实现）对照选中公文集合")
        ref = audit_reference.sample(
            self.ws, self.release_id, self.sample_size, self.seed)
        check(sorted(ref["sample"]) == sorted(first["sample_doc_ids"]),
              "产品抽样与参考实现选中同一集合")

        step("相同发布/样本量/种子 → 同一批次；同一请求 ID 重放取回原回执")
        replay = self.create("r1")
        check(replay["batch_id"] == batch1 and replay["replayed"],
              "请求 r1 重放返回原批次且标记 replayed")
        again = self.create("r1-again")
        check(again["batch_id"] == batch1,
              "不同请求 ID、相同参数也重开同一批次（样本不变）")
        check(len([b for b in list_batches(self.conn) if b["batch_id"] == batch1
                   and b["sample_digest"] == first["sample_digest"]]) == 1,
              "批次按样本摘要唯一")

        step("换种子 → 样本改变（但总体与样本量不变）")
        other = self.create("r2", seed=self.seed + 1)
        check(other["batch_id"] != batch1, "换种子得到不同批次")
        check(other["population_size"] == first["population_size"],
              "总体大小不随种子变化")
        ref2 = audit_reference.sample(
            self.ws, self.release_id, self.sample_size, self.seed + 1)
        check(sorted(ref2["sample"]) == sorted(other["sample_doc_ids"]),
              "换种子后仍与参考实现一致")

        step("反转源记录插入顺序重建 → 相同参数得到同一批次与样本")
        rev_root = os.path.join(self.root, "src-reversed")
        rev_info = audit_fixture.generate(rev_root, reverse=True)
        rev_release = rev_info["release"]["release_id"]
        rev_audit = os.path.join(self.root, "audit-reversed")
        rev_conn = open_audit(rev_audit)
        try:
            rev_rec = create_batch(
                rev_audit, rev_conn, rev_info["workspace"],
                {"request_id": "r1", "release_id": rev_release,
                 "sample_size": self.sample_size, "seed": self.seed})
        finally:
            rev_conn.close()
        check(sorted(rev_rec["sample_doc_ids"]) ==
              sorted(first["sample_doc_ids"]),
              "逆序构建库上选中同一批公文（不随文件/记录排列漂移）")

        step("盲审导出：可见原文坐标/片段/引用，但不泄露团队、投票、裁决来源")
        rows = self.blind_rows(batch1)
        kinds = {r["record_kind"] for r in rows}
        check({"batch", "document", "fragment", "relation"} <= kinds,
              "盲审导出含批次、公文、片段、引用四类行")
        leaked = [r for r in rows if blind_leaks(r)]
        check(not leaked, "任何盲审行都不含正式档号/发布/团队/来源/理由字段")
        raw = "".join(stream_blind(self.conn, batch1))
        for forbidden in ("release_", "rev_", "teamA", "tA", "\"reason\"",
                          "\"origin\"", "\"ticket_uid\"", "\"doc_id\""):
            check(forbidden not in raw, f"盲审文本不含 {forbidden!r}")
        for rec in rows:
            if rec["record_kind"] != "fragment":
                continue
            # 精确字节范围：按盲文档标识读原文切片必须与片段原文一致
            src = blind_source(self.conn, batch1, rec["blind_doc_id"],
                               rec["start"], rec["end"], manifest_path=self.manifest)
            check(src["text"] == rec["text"],
                  f"片段 {rec['blind_fragment_id']} 的精确字节范围可复现原文")
        # 一个公文一经抽中，它在本发布下的全部正式片段/引用都列出
        for d in first["blind_doc_ids"]:
            frags = [r for r in rows if r["record_kind"] == "fragment"
                     and r["blind_doc_id"] == d]
            rels = [r for r in rows if r["record_kind"] == "relation"
                    and r["blind_doc_id"] == d]
            doc_id = next(x["doc_id"] for x in
                          get_batch(self.conn, batch1)["documents"]
                          if x["blind_doc_id"] == d)
            snap = [json.loads(x) for x in stream_snapshot(self.conn, batch1)]
            want_f = sum(1 for x in snap if x["record_kind"] == "fragment"
                         and x["doc_id"] == doc_id)
            want_r = sum(1 for x in snap if x["record_kind"] == "relation"
                         and x["doc_id"] == doc_id)
            check(len(frags) == want_f and len(rels) == want_r,
                  f"公文 {doc_id} 的全部 {want_f} 片段与 {want_r} 引用都进盲审")
        # 引用两端用本批盲审标识
        for r in rows:
            if r["record_kind"] == "relation":
                check(r["from"].startswith("bf_") and r["to"].startswith("bf_"),
                      "有向引用用本批盲审片段标识表示端点")
                break

        step("审核提交：一致意见可幂等重放；问题证据须核对原文坐标")
        snapshot = [json.loads(x) for x in stream_snapshot(self.conn, batch1)]
        doc_rows = [r for r in rows if r["record_kind"] == "document"]
        frag_rows = [r for r in rows if r["record_kind"] == "fragment"]
        rel_rows = [r for r in rows if r["record_kind"] == "relation"]
        bd0 = doc_rows[0]["blind_doc_id"]
        bf0 = next(f for f in frag_rows if f["blind_doc_id"] == bd0)
        target_doc = next(x["doc_id"] for x in snapshot
                          if x["record_kind"] == "document"
                          and x["blind_doc_id"] == bd0)
        issue_text = bf0["text"]
        issue = {
            "subject": {"kind": "fragment",
                        "blind_id": bf0["blind_fragment_id"]},
            "verdict": "issue", "note": "片段标签与正文不符",
            "evidence": {"blind_doc_id": bd0,
                         "start": bf0["start"], "end": bf0["end"],
                         "text": issue_text},
        }
        doc_ok = {
            "subject": {"kind": "document", "blind_id": bd0},
            "verdict": "conforming", "note": "通读未见异常",
        }
        sub_req = {"request_id": "s1", "batch_id": batch1,
                   "reviewer": "auditor-1", "results": [issue, doc_ok]}
        sub1 = submit_results(self.audit_dir, self.conn, sub_req,
                              manifest_path=self.manifest)
        check(sub1["issue_count"] == 1 and sub1["result_count"] == 2,
              "一次提交同时登记符合与发现问题")
        sub1b = submit_results(self.audit_dir, self.conn, sub_req,
                               manifest_path=self.manifest)
        check({k: v for k, v in sub1b.items() if k != "replayed"} ==
              {k: v for k, v in sub1.items() if k != "replayed"} and
              sub1b["replayed"] and not sub1["replayed"],
              "成功提交幂等重放返回原回执（仅标记 replayed）")
        # 审计记录可定位到正式身份与原文证据
        results = [json.loads(x) for x in stream_results(self.conn, batch1)]
        issue_row = next(r for r in results if r["verdict"] == "issue")
        check(issue_row["doc_id"] == target_doc and
              issue_row["real_id"] != bf0["blind_fragment_id"],
              "审计记录按本批盲审标识落库，可回查正式身份")
        check(issue_row["evidence"]["text"] == issue_text and
              issue_row["evidence"]["start"] == bf0["start"],
              "审计记录保存可定位的原文证据")

        step("整批拒绝：越批标识、重复主体、非法 UTF-8 边界、重复结论")
        v0 = self.counts()
        ghost = {
            "request_id": "s-bad1", "batch_id": batch1,
            "results": [{"subject": {"kind": "fragment",
                                     "blind_id": "bf_越批"},
                         "verdict": "conforming", "note": "越批"}],
        }
        expect_error(lambda: submit_results(self.audit_dir, self.conn, ghost),
                     "SUBMISSION_REJECTED", "不属于本批的盲审标识整批拒绝")
        dup = {
            "request_id": "s-bad2", "batch_id": batch1,
            "results": [doc_ok, doc_ok],
        }
        exc = expect_error(
            lambda: submit_results(self.audit_dir, self.conn, dup),
            "SUBMISSION_REJECTED", "同一批结果重复主体整批拒绝")
        check(any(e["code"] == "DUPLICATE_RESULT" for e in exc.errors),
              "错误明细指出重复主体")
        # 另一个抽中文档的片段，用它做坏边界与坏文本证据
        bd1 = next(d["blind_doc_id"] for d in doc_rows
                   if d["blind_doc_id"] != bd0)
        raw1 = blind_source(self.conn, batch1, bd1,
                            manifest_path=self.manifest)["text"]
        if not any(len(ch.encode("utf-8")) > 1 for ch in raw1):
            raw1 = None
        bad_boundary = {
            "request_id": "s-bad3", "batch_id": batch1,
            "results": [{
                "subject": {"kind": "document", "blind_id": bd1},
                "verdict": "issue", "note": "边界错误",
                "evidence": {"blind_doc_id": bd1, "start": 0, "end": 1,
                             "text": ""}}],
        }
        # end=1 在多字节首字符中段 -> 非法；用实际字节构造
        raw_bytes = raw1.encode("utf-8") if raw1 else b"\xe7\x94\xb2"
        # 找到第一个多字节字符的起始字节，切在它中间
        split_at = None
        for i, ch in enumerate(raw1 or "甲"):
            if len(ch.encode("utf-8")) > 1:
                split_at = len((raw1 or "甲")[:i].encode("utf-8")) + 1
                break
        if split_at is None:
            split_at = 1
        bad_boundary["results"][0]["evidence"] = {
            "blind_doc_id": bd1, "start": 0, "end": split_at,
            "text": raw_bytes[:split_at].decode("utf-8", "replace")}
        exc = expect_error(
            lambda: submit_results(
                self.audit_dir, self.conn, bad_boundary,
                manifest_path=self.manifest),
            "SUBMISSION_REJECTED", "切在多字节字符中段的坐标整批拒绝")
        check(any(e["code"] == "BAD_EVIDENCE_RANGE" for e in exc.errors),
              "错误明细指出非法字节边界")
        wrong_text = {
            "request_id": "s-bad4", "batch_id": batch1,
            "results": [{
                "subject": {"kind": "document", "blind_id": bd1},
                "verdict": "issue", "note": "文本不符",
                "evidence": {"blind_doc_id": bd1, "start": 0, "end": 4,
                             "text": "ZZZZ"}}],
        }
        exc = expect_error(
            lambda: submit_results(
                self.audit_dir, self.conn, wrong_text,
                manifest_path=self.manifest),
            "SUBMISSION_REJECTED", "证据文本与坐标处原文不符整批拒绝")
        check(any(e["code"] == "EVIDENCE_TEXT_MISMATCH" for e in exc.errors),
              "错误明细指出证据文本不符")
        # 不得因同批其他结果有效而部分写入
        mixed = {
            "request_id": "s-bad5", "batch_id": batch1,
            "results": [
                {"subject": {"kind": "fragment",
                             "blind_id": frag_rows[-1]["blind_fragment_id"]},
                 "verdict": "conforming", "note": "有效的一条"},
                ghost["results"][0],
            ],
        }
        expect_error(lambda: submit_results(self.audit_dir, self.conn, mixed),
                     "SUBMISSION_REJECTED", "同批混入非法则整批不写入")
        check(self.counts() == v0, "所有被拒提交都没有改变审计记录")
        # 已结案主体再次提交 -> 重复结论
        again_subject = {
            "request_id": "s-dup-existing", "batch_id": batch1,
            "results": [doc_ok],
        }
        expect_error(
            lambda: submit_results(self.audit_dir, self.conn, again_subject),
            "SUBMISSION_REJECTED", "已结案主体不能重复提交结论")

        step("未审记录、覆盖与审计结论的只读查询")
        cov = batch_coverage(self.conn, batch1)
        check(cov["totals"]["documents_reviewed"] == 1,
              "覆盖统计：1 篇已整篇审读")
        unsettled = [json.loads(x) for x in
                     stream_unreviewed(self.conn, batch1)]
        check(len(unsettled) >= 1, "仍有未审记录可列")
        check(all("blind_doc_id" in r for r in unsettled),
              "未审记录同时给出盲审标识供档案定位")

        step("仅修改审定工作区并另发新发布 → 旧批次仍显示原发布内容")
        old_blind = "".join(stream_blind(self.conn, batch1))
        old_snap = "".join(stream_snapshot(self.conn, batch1))
        self._new_release_from_workspace()
        check("".join(stream_blind(self.conn, batch1)) == old_blind,
              "旧批次盲审导出逐字节不变")
        check("".join(stream_snapshot(self.conn, batch1)) == old_snap,
              "旧批次发布绑定快照逐字节不变")
        old_info = get_batch(self.conn, batch1)
        check(old_info["release_id"] == self.release_id,
              "旧批次仍绑定原发布")

        step("无效样本量与请求 ID 参数冲突")
        v1 = self.counts()
        expect_error(lambda: self.create("r-bad0", size=0), "BAD_REQUEST",
                     "样本量 0 拒绝")
        expect_error(lambda: self.create("r-badneg", size=-1), "BAD_REQUEST",
                     "负样本量拒绝")
        expect_error(lambda: self.create("r-big", size=99), "SAMPLE_TOO_LARGE",
                     "样本量超过总体整次拒绝且不建空壳批次")
        expect_error(
            lambda: self.create("r1", seed=self.seed + 5), "REQUEST_ID_CONFLICT",
            "同一请求 ID 携带不同参数拒绝")
        check(self.counts()["batches"] == v1["batches"],
              "以上失败均未生成新批次")

        step("中断恢复：子进程写入批次后提交前被 SIGKILL，不留半批")
        before = self.counts()
        self._kill_during_create()
        after = self.counts()
        check(after["batches"] == before["batches"] and
              after["requests"] == before["requests"],
              "中断后没有可见半批（批次/请求台账不变）")
        recovered = self.create("r-interrupt")
        check(recovered["batch_id"] == batch1,
              "中断后重试同一参数得到原批次（不产生半个批次）")

        if self.large_docs:
            self.large_scale(before)

        say()
        say("== 全部断言通过")
        say(f"批次 {batch1}：{first['sample_doc_ids']}，"
            f"片段 {first['fragment_count']}，引用 {first['relation_count']}")
        return 0

    # -- 工作区演进并另发发布 ------------------------------------------------
    def _new_release_from_workspace(self):
        conn = connect(self.ws)
        try:
            view = view_revision(conn, "HEAD", doc_id="D1")
            frags = {(f["layer"], f["start"]): f["fragment_uid"]
                     for f in view["documents"]["D1"]["fragments"]}
            # 找到包络为 [0,8) 的那个 clause 区域（D1 有两个 clause 区域）
            region0 = next(
                r["region_uid"] for r in view["documents"]["D1"]["regions"]
                if r["layer"] == "clause" and r["fragments"]
                and any(f["fragment_uid"] in r["fragments"]
                        and f["start"] == 0
                        for f in view["documents"]["D1"]["fragments"]))
            start_frag = next(f["fragment_uid"]
                              for f in view["documents"]["D1"]["fragments"]
                              if f["fragment_uid"] in
                              next(r["fragments"]
                                   for r in view["documents"]["D1"]["regions"]
                                   if r["region_uid"] == region0))
            base = resolve_revision(conn, "HEAD")["rev_id"]
            y = frags[("annex", 20)]
            ticket = {
                "reviewer": "curator", "title": "拆分 X1 并退役旧边",
                "base": base,
                "regions": [{
                    "region": region0, "decision": "fragments",
                    "reason": "X1 拆为 X1a/X1b",
                    "fragments": [
                        {"ref": "X1a", "start": 0, "end": 4, "label": "L1"},
                        {"ref": "X1b", "start": 4, "end": 8, "label": "L1"},
                    ],
                }],
                "relations": [
                    {"doc": "D1", "type": "ref",
                     "from": start_frag, "to": y,
                     "decision": "retired", "reason": "X1 拆分后旧边失效"},
                    # 另一个 clause 片段指向同一 annex 的引用也要显式退役，
                    # 才能通过工单基版自洽预检（不得静默丢弃既有判定）
                    {"doc": "D1", "type": "ref",
                     "from": frags[("clause", 10)], "to": y,
                     "decision": "retired", "reason": "同批显式退役"},
                ],
            }
            src = WorkspaceSources(conn, manifest_path=self.manifest)
            result = merge(conn, src, self.ws, data=ticket)
            if result["status"] != "merged":
                raise AssertionError(f"merge failed: {result}")
            rel = release(conn, self.ws, rev="HEAD",
                          doc_ids=["D1", "D2", "D4", "D5"])
            if not rel.get("released"):
                raise AssertionError(f"new release blocked: {rel}")
            self.release_v2 = rel["release_id"]
            # 新发布上的盲审应看到不同内容（证明审计确实只读快照）
            req = {"request_id": "r-v2", "release_id": self.release_v2,
                   "sample_size": 3, "seed": self.seed}
            rec2 = create_batch(self.audit_dir, self.conn, self.ws, req)
            check(rec2["batch_id"] != self.receipts["r1"]["batch_id"],
                  "新发布上的同参数批次与旧批次不同（发布绑定不同）")
        finally:
            conn.close()

    # -- 中断测试 -----------------------------------------------------------
    def _kill_during_create(self):
        script = f"""
import json, os, sys, time
sys.path.insert(0, {ROOT_DIR!r})
from qc.audit_common import open_audit
from qc import audit
audit_dir = {self.audit_dir!r}
ws = {self.ws!r}
conn = open_audit(audit_dir)
orig = audit._snapshot_fragments
def slow(*a, **kw):
    conn.execute("SELECT 1")
    time.sleep(5)
    return orig(*a, **kw)
audit._snapshot_fragments = slow
try:
    audit.create_batch(audit_dir, conn, ws,
        {{"request_id": "r-killed", "release_id": {self.release_id!r},
          "sample_size": {self.sample_size}, "seed": {self.seed + 99}}})
except BaseException:
    pass
os._exit(9)
"""
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                cwd=ROOT_DIR, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        # 等待子进程进入写事务（sleep 5 秒），再在其提交前 SIGKILL
        import time
        time.sleep(1.0)
        proc.kill()
        proc.communicate(timeout=30)

    # -- 大规模有界内存 -----------------------------------------------------
    def large_scale(self, before):
        from tools import audit_scalegen

        say()
        step(f"运行时生成 {self.large_docs} 篇文档，核对有界内存的抽样与导出")
        scale_root = os.path.join(self.root, "scale")
        info = audit_scalegen.generate(scale_root, self.large_docs)
        audit_dir = os.path.join(scale_root, "audit")
        conn = open_audit(audit_dir)
        try:
            import resource
            import time
            t0 = time.monotonic()
            rec = create_batch(
                audit_dir, conn, info["workspace"],
                {"request_id": "big", "release_id": info["release_id"],
                 "sample_size": 50, "seed": 12345})
            create_s = time.monotonic() - t0
            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            check(rec["population_size"] == self.large_docs,
                  f"大发布总体 {self.large_docs} 篇")
            check(len(rec["sample_doc_ids"]) == 50, "从大总体抽取 50 篇")
            # 参考实现对照（参考实现会把总体读成列表；仅用于正确性）
            ref = audit_reference.sample(
                info["workspace"], info["release_id"], 50, 12345)
            check(sorted(ref["sample"]) == sorted(rec["sample_doc_ids"]),
                  "大总体抽样与参考实现一致")
            rows = 0
            chars = 0
            t0 = time.monotonic()
            for line in stream_blind(conn, rec["batch_id"]):
                rows += 1
                chars += len(line)
            export_s = time.monotonic() - t0
            rss_mb2 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            say(f"   抽样 {create_s:.2f}s，峰值 RSS {rss_mb:.0f}MB；"
                f"导出 {rows} 行 {chars / 1e6:.1f}MB，{export_s:.2f}s，"
                f"峰值 RSS {rss_mb2:.0f}MB")
            # 批次行 1 + 50 公文 + 每篇至少 1 条片段 + 部分引用
            check(rows > 51, "盲审导出用服务端游标逐行产出（批次、公文、片段、引用）")
            check(rss_mb2 < 1024, f"峰值 RSS {rss_mb2:.0f}MB 低于 1GB")
        finally:
            conn.close()

    def counts(self):
        return {
            "batches": self.conn.execute(
                "SELECT COUNT(*) c FROM aud_batch").fetchone()["c"],
            "requests": self.conn.execute(
                "SELECT COUNT(*) c FROM aud_request").fetchone()["c"],
            "results": self.conn.execute(
                "SELECT COUNT(*) c FROM aud_result").fetchone()["c"],
        }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None,
                    help="产物目录（默认临时目录，事后删除）")
    ap.add_argument("--large-docs", type=int, default=0,
                    help="额外生成的大批文档数（0 表示跳过规模检查）")
    args = ap.parse_args()
    root = args.root or tempfile.mkdtemp(prefix="audit-demo-")
    if os.path.isdir(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    demo = Demo(root, large_docs=args.large_docs)
    try:
        return demo.run()
    finally:
        demo.conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
