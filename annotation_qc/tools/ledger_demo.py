# -*- coding: utf-8 -*-
"""可复跑的分仓台账验收示例。

场景（对应任务书逐条）：

1. 公文甲、乙单独分入不同仓并确认交付（落锁 + 留存交付摘要）；
2. 选进未交付的丙：甲丙共享非豁免条款、丙乙有两条人工同源证据
   → 三篇全部隔离；撤一条人工证据仍隔离；撤第二条后甲、丙回甲仓、
   乙回乙仓，旧交付凭据不变；
3. 甲改选不含旧条款的发布并撤出；选进只复用旧条款的丁，
   丁仍随甲的历史仓位，证据指到实际交付的旧发布；
4. 对照检查：只共享豁免落款不必同组；底稿相同但没有片段仍同组；
   旧版确认、错误发布、提交中断、相同请求重放；
5. 每一步混合增删后都用小图全量重算核对成员与仓位。

用法::

    python -I tools/ledger_demo.py --root /tmp/ldemo
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

from qc.ledger import Ledger, init_ledger  # noqa: E402
from qc.ledger_common import LedgerError  # noqa: E402
from tools.ledger_fixture import generate  # noqa: E402

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
    except LedgerError as exc:
        assert exc.code == code, f"期望错误 {code}，实际 {exc.code}: {exc}"
        say(f"   ✓ {msg}（{code}）")
        return exc
    raise AssertionError(f"期望错误 {code}，但操作成功")


class Demo:
    def __init__(self, root):
        self.root = root
        self.info = generate(os.path.join(root, "upstream"))
        self.ledger_dir = os.path.join(root, "ledger")
        init_ledger(
            self.ledger_dir, self.info["workspace"],
            layer="clause", min_segment_bytes=16,
            exempt_digests=self.info["exempt"])
        self.lg = Ledger(self.ledger_dir)
        self.docs = self.info["docs"]
        self.rel = self.info["releases"]
        self.receipts = {}

    # -- 小助手 -------------------------------------------------------------
    @property
    def version(self):
        return self.lg.version

    def apply(self, rid, **ops):
        req = {"request_id": rid, "base_version": self.version, **ops}
        receipt = self.lg.apply_change(req)
        self.receipts[rid] = receipt
        self.verify()
        return receipt

    def confirm(self, rid, doc_ids):
        req = {"request_id": rid, "base_version": self.version,
               "list_version": self.version, "doc_ids": doc_ids}
        receipt = self.lg.confirm(req)
        self.receipts[rid] = receipt
        self.verify()
        return receipt

    def verify(self):
        result = self.lg.verify()
        assert result["consistent"], result["differences"]
        say(f"   ✓ 全量重算对照一致（v{self.version}，"
            f"成员 {result['full_rebuild_diagnostics']['members_recomputed']}）")

    def listed(self, doc_id):
        for pos in ("train", "eval", "quarantine"):
            for row in self.lg.lists()[pos]:
                if row["doc_id"] == doc_id:
                    return pos
        return None

    def inuse_segments(self, doc_id):
        return {r["digest"] for r in self.lg.conn.execute(
            "SELECT digest FROM lg_digest WHERE doc_id=?"
            " AND source='inuse' AND kind='segment'", (doc_id,))}

    def component_of(self, doc_id):
        row = self.lg.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_id,)).fetchone()
        return None if row is None else row["component_id"]

    # -- 主流程 -------------------------------------------------------------
    def run(self):
        d, r = self.docs, self.rel
        jia, yi, bing = d["甲"], d["乙"], d["丙"]
        ding, wu, ji = d["丁"], d["戊"], d["己"]
        geng, xin, ren = d["庚"], d["辛"], d["壬"]
        clause_old = self.info["digests"]["clause_old"]

        say(f"上游工作区: {self.info['workspace']}")
        say(f"发布 R1={r['R1']}")
        say(f"发布 R2={r['R2']}")
        say(f"档号: 甲={jia} 乙={yi} 丙={bing} 丁={ding}")

        step("甲、乙分别选入 R1：各自独立分量，按档号哈希分入不同仓")
        rec = self.apply("b1", select=[
            {"doc_id": jia, "release_id": r["R1"]},
            {"doc_id": yi, "release_id": r["R1"]},
        ])
        check(self.listed(jia) == "train", f"甲({jia}) 进训练仓")
        check(self.listed(yi) == "eval", f"乙({yi}) 进评测仓")
        check(self.component_of(jia) != self.component_of(yi),
              "甲乙不同分量（甲的 annex 层片段与乙的 clause 片段文本相同，"
              "但台账只看建账选定的 clause 层，不同组）")
        check(clause_old in self.inuse_segments(jia),
              "甲@R1 的在用摘要含旧条款（HEAD 已废止该条款，"
              "证明台账读的是发布快照而非当前审定稿）")
        diag = rec["diagnostics"]
        check(diag["mode"] == "incremental" and
              sorted(diag["upstream_docs_read"]) == sorted([jia, yi]),
              f"诊断：本次只读取 2 篇公文（{diag['upstream_docs_read']}）")

        step("确认甲、乙所在清单版本已对外交付：落锁 + 留存交付摘要")
        rec = self.confirm("c1", [jia, yi])
        v_c1 = rec["version"]
        creds = {x["doc_id"]: x for x in rec["deliveries"]}
        check(creds[jia]["position"] == "train" and
              creds[yi]["position"] == "eval", "甲乙各锁本仓")
        check(any(s["digest"] == clause_old
                  for s in creds[jia]["segment_digests"]),
              "甲的交付凭据含旧条款摘要（历史证据）")

        step("选进丙：甲丙共享旧条款，丙乙登记两条人工同源证据 → 三篇隔离")
        rec = self.apply("b2",
                         select=[{"doc_id": bing, "release_id": r["R1"]}],
                         evidence_register=[
                             {"evidence_id": "E1", "doc_a": bing, "doc_b": yi,
                              "provenance": "1987年移交清册第14页"},
                             {"evidence_id": "E2", "doc_a": bing, "doc_b": yi,
                              "provenance": "数字化外包合同附件3"},
                         ])
        for doc in (jia, yi, bing):
            check(self.listed(doc) == "quarantine", f"{doc} 在隔离清单")
        qr = self.lg.quarantine_reason(bing)
        check(qr["quarantined"] and len(qr["locks"]) == 2,
              "隔离原因可查到两个不同仓位的历史确认")
        path_jia = qr["paths_from_member"]["train"]["path"]
        path_yi = qr["paths_from_member"]["eval"]["path"]
        check(path_jia and path_jia[0]["via"]["digest"] == clause_old,
              "丙→甲 的证据路径经过旧条款摘要")
        check(path_yi and path_yi[0]["via"]["type"] == "manual_evidence",
              "丙→乙 的证据路径经过人工同源证据")
        expect_error(lambda: self.lg.confirm({
            "request_id": "cx1", "base_version": self.version,
            "list_version": self.version, "doc_ids": [bing]}),
            "CONFIRM_REJECTED", "隔离中的丙不能确认交付")

        step("撤掉一条人工证据（E1）：另一条仍在，仍隔离")
        self.apply("b3", evidence_revoke=["E1"])
        check(self.listed(bing) == "quarantine", "撤一条后仍隔离")

        step("撤掉第二条人工证据（E2）：甲丙回甲仓、乙回乙仓，旧凭据不变")
        self.apply("b4", evidence_revoke=["E2"])
        check(self.listed(jia) == "train" and self.listed(bing) == "train",
              "甲、丙回训练仓（服从甲的锁定仓位）")
        check(self.listed(yi) == "eval", "乙回评测仓")
        before = self.lg.credentials(jia)
        check(len(before) == 1 and before[0]["delivery_id"] ==
              creds[jia]["delivery_id"], "甲的旧交付凭据未被改写")

        step("分次确认：单独确认丙已交付（锁训练仓）")
        self.confirm("c2", [bing])

        step("甲改选 R2（不含旧条款）并撤出：历史摘要保留，丙仍随甲仓")
        self.apply("b5", select=[{"doc_id": jia, "release_id": r["R2"]}])
        check(clause_old not in self.inuse_segments(jia),
              "甲改选后在用摘要不含旧条款")
        check(any(x["digest"] == clause_old
                  for x in self.lg.credentials(jia)[0]["digests"]),
              "甲已交付的旧条款摘要不被覆盖")
        self.apply("b6", withdraw=[jia])
        check(self.listed(jia) is None, "甲撤出后不再列为待取用公文")
        check(self.listed(bing) == "train", "丙仍随甲的历史仓位")

        step("丙改选 R2 并再次确认交付：同仓位留下另一实际交付版本的摘要")
        self.apply("b7", select=[{"doc_id": bing, "release_id": r["R2"]}])
        self.confirm("c3", [bing])
        creds_bing = self.lg.credentials(bing)
        check(len(creds_bing) == 2 and
              {c["position"] for c in creds_bing} == {"train"},
              "丙两次交付凭据仓位一致")
        check({tuple(sorted(x["digest"] for x in c["digests"]))
               for c in creds_bing}.__len__() == 2,
              "丙第二次交付留下新的摘要集合")

        step("选进丁（只复用旧条款）：丁随甲的历史仓位，证据指向旧发布")
        rec = self.apply("b8", select=[{"doc_id": ding, "release_id": r["R1"]}])
        check(self.listed(ding) == "train", "丁随甲的历史仓位（训练仓）")
        path = self.lg.path_between(ding, jia)
        check(path is not None, "丁与甲同组（经历史摘要连通）")
        side_jia = next(s for st in path for s in st["via"].get("sides", [])
                        if s["doc_id"] == jia)
        check(side_jia["source"] == "delivered" and
              side_jia["release_id"] == r["R1"] and
              side_jia["delivery"]["delivery_id"] == creds[jia]["delivery_id"],
              "证据定位到实际交付的旧发布 R1 与交付凭据")

        step("对照：戊己只共享豁免落款与不足最短字数的片段 → 不必同组")
        self.apply("b9", select=[
            {"doc_id": wu, "release_id": r["R1"]},
            {"doc_id": ji, "release_id": r["R1"]},
        ])
        check(self.component_of(wu) != self.component_of(ji),
              "豁免落款与 15 字节短句都不构成同组证据")

        step("对照：庚辛底稿相同但没有片段 → 仍同组并列于清单")
        self.apply("b10", select=[
            {"doc_id": geng, "release_id": r["R1"]},
            {"doc_id": xin, "release_id": r["R1"]},
        ])
        check(self.component_of(geng) == self.component_of(xin),
              "整篇底稿摘要相同即同组（无片段也同组）")
        check(self.listed(geng) is not None and
              self.listed(xin) is not None, "无片段公文也在清单中")

        step("对照：非法与边界情形")
        v0 = self.version
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e1", "base_version": self.version,
            "select": [{"doc_id": jia, "release_id": "rel_不存在"}]}),
            "BATCH_REJECTED", "未知发布整批拒绝")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e2", "base_version": self.version,
            "select": [{"doc_id": ren, "release_id": r["R2"]}]}),
            "BATCH_REJECTED", "档号不属于该发布整批拒绝")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e3", "base_version": self.version,
            "select": [{"doc_id": ren, "release_id": r["R1"]},
                       {"doc_id": jia, "release_id": "rel_不存在"}]}),
            "BATCH_REJECTED", "批中一条非法则整批不入账")
        check(self.component_of(ren) is None and self.version == v0,
              "壬未入账，台账版本未前进")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e4", "base_version": self.version,
            "evidence_register": [{"evidence_id": "E9", "doc_a": ren,
                                   "doc_b": "档-不存在",
                                   "provenance": "x"}]}),
            "BATCH_REJECTED", "证据端点必须是已知档号")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e5", "base_version": self.version,
            "evidence_revoke": ["E-不存在"]}),
            "BATCH_REJECTED", "撤销未知证据整批拒绝")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e6", "base_version": self.version,
            "evidence_register": [{"evidence_id": "E2", "doc_a": bing,
                                   "doc_b": yi, "provenance": "重复"}]}),
            "BATCH_REJECTED", "证据 ID 不可复用（含已撤销）")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "e7", "base_version": 0,
            "select": [{"doc_id": ren, "release_id": r["R1"]}]}),
            "STALE_BASE_VERSION", "旧版变更拒绝")
        expect_error(lambda: self.lg.confirm({
            "request_id": "e8", "base_version": self.version,
            "list_version": v_c1, "doc_ids": [ren]}),
            "STALE_LIST_VERSION", "旧版清单不能二次确认成新交付")

        step("对照：相同请求重放取回原回执；不同内容不能共用 ID")
        replay = self.lg.apply_change({
            "request_id": "b1", "base_version": 0,
            "select": [{"doc_id": jia, "release_id": r["R1"]},
                       {"doc_id": yi, "release_id": r["R1"]}]})
        check(replay == self.receipts["b1"] and self.version == v0,
              "b1 重放返回原回执，版本未前进")
        expect_error(lambda: self.lg.apply_change({
            "request_id": "b1", "base_version": 0, "withdraw": [yi]}),
            "REQUEST_ID_CONFLICT", "不同内容共用请求 ID 拒绝")

        step("对照：写入中断不留半版")
        self.crash_test(ren, r["R1"])

        say()
        say("== 全部断言通过；最终清单：")
        lists = self.lg.lists()
        for pos in ("train", "eval", "quarantine"):
            say(f"  {pos}: {[x['doc_id'] for x in lists[pos]]}")
        say(f"台账版本 v{self.version}")
        return 0

    def crash_test(self, doc_id, release_id):
        """子进程在写入中途强制退出：重开不能留下部分摘要/错误锁位/坏回执。"""
        before = (self.version, self.lg.status())
        code = f"""
import os, sqlite3
conn = sqlite3.connect({os.path.join(self.ledger_dir, 'ledger.db')!r})
conn.isolation_level = None
conn.execute('BEGIN IMMEDIATE')
conn.execute("INSERT INTO lg_selection(doc_id, release_id, rev_id, rev_seq,"
             " status, whole_digest, updated_version)"
             " VALUES ('档-幽灵', 'rel_x', 'rev_x', 0, 'selected', 'x', 999)")
conn.execute("UPDATE lg_meta SET value='999' WHERE key='ledger_version'")
os._exit(1)  # 模拟进程在写入中退出：未提交
"""
        subprocess.run([sys.executable, "-I", "-c", code], check=False,
                       capture_output=True)
        with Ledger(self.ledger_dir) as lg:
            check(lg.version == before[0], "中断后台账版本未变")
            row = lg.conn.execute(
                "SELECT 1 FROM lg_selection WHERE doc_id='档-幽灵'").fetchone()
            check(row is None, "中断未留下部分写入")
            check(lg.status()["docs_selected"] ==
                  before[1]["docs_selected"], "清单统计与中断前一致")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None,
                    help="产物目录（默认临时目录，事后删除）")
    args = ap.parse_args()
    root = args.root or tempfile.mkdtemp(prefix="ledger-demo-")
    if os.path.isdir(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    demo = Demo(root)
    try:
        return demo.run()
    finally:
        demo.lg.close()


if __name__ == "__main__":
    raise SystemExit(main())
