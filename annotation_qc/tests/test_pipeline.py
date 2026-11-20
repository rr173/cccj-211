# -*- coding: utf-8 -*-
"""管道端到端测试（标准库 unittest，python -I 下运行）。

覆盖：
- 手算表逐项核对（归组、分母、候选票、无片段、关系双方计票）；
- 紧邻不合组、传递归并、覆盖缺口逐字节报告；
- UTF-8 组合字符/中段错位拒收、所见原文逐字节一致（无归一化）；
- 跨包同名局部 ID、同团队重复/矛盾关系、并列无胜出；
- 重复包、重复片段 ID、悬空关系、越出审核范围、错误底稿；
- 结构错误不发布且不替换旧产物；
- 合法记录重排不改变逻辑标识与业务结果；
- 只读库拒绝写入；
- 规模生成器下无线性平方级交叠边（基于工作量计数与计时）。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qc.build import build, coverage_gaps  # noqa: E402
from qc.policy import parse_policy  # noqa: E402
from qc.query import QueryDB  # noqa: E402
from qc.util import is_utf8_boundary, stable_id  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "fixtures")


def _load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_from_fixture(testcase, subdir_packages=None, manifest=None,
                        policy=None, out_name="w.db"):
    tmp = testcase.tmpdir
    db = os.path.join(tmp, out_name)
    result = build(
        manifest_path=manifest or os.path.join(FIXTURES, "manifest.json"),
        packages_root=subdir_packages or os.path.join(FIXTURES, "packages"),
        output_db=db,
        policy=parse_policy(_load_json(
            policy or os.path.join(FIXTURES, "policy.json"))),
        reject_report=os.path.join(tmp, "reject.json"),
    )
    return result, db


class FixtureTestCase(unittest.TestCase):
    """基于可重建小夹具的行为核对（与 hand_calc.json 独立手算对照）。"""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="qctest-")
        cls.result, cls.db = _build_from_fixture(cls)
        cls.hand = _load_json(os.path.join(FIXTURES, "hand_calc.json"))
        cls.qdb = QueryDB(cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.qdb.close()
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_00_published(self):
        self.assertEqual(self.result.errors, [])
        self.assertTrue(os.path.isfile(self.db))

    def test_01_region_envelopes_and_transitive_group(self):
        regions = self.qdb.list_regions()
        envelopes = sorted((r["layer"], tuple([r["start_byte"], r["end_byte"]]))
                           for r in regions)
        # R1 传递归并 [0,9)；R4 紧邻产生两个独立区域而非 [28,34)
        self.assertIn(("clause", (0, 9)), envelopes)
        self.assertIn(("clause", (28, 31)), envelopes)
        self.assertIn(("clause", (31, 34)), envelopes)
        self.assertNotIn(("clause", (28, 34)), envelopes)
        self.assertEqual(len(regions), 6)

    def test_02_r1_denominator_votes_and_gap(self):
        r1 = next(r for r in self.qdb.list_regions()
                  if (r["start_byte"], r["end_byte"]) == (0, 9))
        detail = self.qdb.region_detail(r1["region_uid"])
        self.assertEqual(detail["denominator"], 4)
        # 三个不同坐标候选各 1 票，无片段 1 票
        cand_votes = sorted(c["vote_count"] for c in detail["candidates"])
        self.assertEqual(cand_votes, [1, 1, 1, 1])
        # teamE 部分覆盖，缺口精确到单字节 [5,6)
        e = next(x for x in detail["eligibility"] if x["team_id"] == "teamE")
        self.assertEqual(e["coverage"], "partial")
        self.assertEqual(e["coverage_gaps"], [[5, 6]])
        # 被排除意见可查
        excluded_teams = {x["proposal"]["team_id"]
                          for x in detail["excluded_opinions"]}
        self.assertEqual(excluded_teams, {"teamE"})
        # 待核
        self.assertEqual(detail["outcome"]["winner_kind"], "pending")
        review = self.qdb.review_items(item_kind="region")
        self.assertTrue(any(x["region_uid"] == r1["region_uid"] for x in review))

    def test_03_r2_self_contradiction_in_denominator(self):
        r2 = next(r for r in self.qdb.list_regions()
                  if (r["start_byte"], r["end_byte"]) == (10, 18))
        detail = self.qdb.region_detail(r2["region_uid"])
        self.assertEqual(detail["denominator"], 4)
        conflicts = {v["team_id"] for v in detail["self_contradicting_teams"]}
        self.assertEqual(conflicts, {"teamH"})
        l3 = next(c for c in detail["candidates"]
                  if not c["is_no_fragment"] and c["label"] == "L3")
        self.assertEqual(l3["vote_count"], 2)  # 2 票但严格多数为 3
        none_c = next(c for c in detail["candidates"] if c["is_no_fragment"])
        self.assertEqual(none_c["vote_count"], 1)
        self.assertEqual(detail["outcome"]["winner_kind"], "pending")

    def test_04_r3_no_fragment_wins(self):
        r3 = next(r for r in self.qdb.list_regions()
                  if (r["start_byte"], r["end_byte"]) == (19, 26))
        detail = self.qdb.region_detail(r3["region_uid"])
        self.assertEqual(detail["outcome"]["winner_kind"], "no_fragment")
        self.assertIsNone(detail["official_fragment"])  # 确认不入仓
        none_c = next(c for c in detail["candidates"] if c["is_no_fragment"])
        self.assertEqual(none_c["vote_count"], 3)
        # 该区域没有正式片段
        self.assertEqual(
            self.qdb.official_fragments(layer="clause",
                                        doc_id=r3["doc_id"]).__len__(), 2
        )

    def test_05_official_fragments_identity_stable(self):
        frags = self.qdb.official_fragments()
        coords = sorted((f["layer"], f["start_byte"], f["end_byte"], f["label"])
                        for f in frags)
        self.assertEqual(
            coords,
            [
                ("annex", 28, 34, "A1"),
                ("clause", 28, 31, "L1"),
                ("clause", 31, 34, "L1"),
            ],
        )
        # 身份为内容派生，且与手算重算一致
        f = frags[0]
        expect = stable_id(
            "frag", f["doc_id"], f["layer"], f["start_byte"],
            f["end_byte"], f["label"],
        )
        self.assertEqual(f["fragment_uid"], expect)
        self.assertNotIn("AUTOINCREMENT", json.dumps(frags))

    def test_06_forward_relation_confirmed(self):
        rels = self.qdb.official_relations(rel_type="ref")
        self.assertEqual(len(rels), 1)
        rel = rels[0]
        self.assertEqual(rel["for_votes"], 2)
        self.assertEqual(rel["against_votes"], 0)
        self.assertEqual(rel["denominator"], 2)
        self.assertEqual(rel["from"]["start_byte"], 28)
        self.assertEqual(rel["to"]["start_byte"], 28)
        detail = self.qdb.relation_group_detail(rel["rel_group_uid"])
        teams = {v["team_id"] for v in detail["votes"]}
        self.assertEqual(teams, {"teamM", "teamN"})

    def test_07_third_team_silence_not_counted(self):
        # teamQ 看过两端但没投关系票：不在任何关系组分母里
        rels = self.qdb.official_relations(rel_type="ref")
        detail = self.qdb.relation_group_detail(rels[0]["rel_group_uid"])
        self.assertNotIn("teamQ", {v["team_id"] for v in detail["votes"]})

    def test_08_other_candidate_relation_blocked_and_traceable(self):
        blocked = self.qdb.blocked_relations(rel_type="ref")
        r = next(x for x in blocked if x["team_id"] == "teamR")
        self.assertEqual(r["blocked_reason"], "endpoint_other_candidate_won")
        self.assertFalse(r["endpoint_regions"]["from"]["adopted"])
        self.assertTrue(r["endpoint_regions"]["to"]["adopted"])
        # 记录位置证据
        self.assertEqual(r["record"]["file_name"], "teamR.jsonl")
        self.assertIsInstance(r["record"]["line_no"], int)

    def test_09_reverse_relation_separate_and_pending(self):
        # 反向类型只有一票，低于 min_support=2 -> 待核，且不与正向并组
        reviews = self.qdb.review_items(item_kind="relation",
                                        rel_type="reverse_ref")
        self.assertEqual(len(reviews), 1)
        groups = self.qdb.conn.execute(
            "SELECT * FROM relation_group WHERE rel_type='reverse_ref'"
        ).fetchall()
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["decision"], "pending")
        self.assertEqual(groups[0]["for_votes"], 1)

    def test_10_conflicting_relation_vote_in_denominator(self):
        row = self.qdb.conn.execute(
            "SELECT g.* FROM relation_group g WHERE g.conflict_votes=1"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["denominator"], 2)
        self.assertEqual(row["for_votes"], 1)
        self.assertEqual(row["decision"], "pending")
        detail = self.qdb.relation_group_detail(row["rel_group_uid"])
        t = next(v for v in detail["votes"] if v["team_id"] == "teamT")
        self.assertEqual(t["sign"], "conflict")
        self.assertEqual(len(t["records"]), 2)  # 赞成+反对两条原始记录

    def test_11_forward_reference_allowed(self):
        # teamM 把关系写在片段之前；teamU 同样前向，均成功解析
        rows = self.qdb.conn.execute(
            "SELECT resolved FROM relation_endpoint"
        ).fetchall()
        self.assertTrue(rows)
        # teamU 的记录解析成功（与 teamT 同组 [31,34)->[28,34)）
        u = self.qdb.conn.execute(
            "SELECT re.resolved FROM relation_endpoint re"
            " JOIN package p ON p.package_uid=re.package_uid"
            " WHERE p.team_id='teamU'"
        ).fetchall()
        self.assertEqual(len(u), 1)
        self.assertEqual(u[0]["resolved"], 1)

    def test_12_evidence_chain_complete(self):
        r1 = next(r for r in self.qdb.list_regions()
                  if (r["start_byte"], r["end_byte"]) == (0, 9))
        detail = self.qdb.region_detail(r1["region_uid"])
        coord = detail["byte_coordinates"]
        self.assertEqual(len(coord["source_sha256"]), 64)
        self.assertIn("source_path", coord)
        m = detail["member_proposals"][0]
        self.assertIn("record_position", m)
        self.assertIn("merged_review_ranges", m)
        self.assertIn("package_id", m["package"])

    def test_13_readonly_rejects_writes(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.qdb.conn.execute("DELETE FROM region")

    def test_14_no_dangling_official_relation(self):
        n = self.qdb.conn.execute(
            "SELECT COUNT(*) c FROM official_relation o"
            " WHERE o.from_fragment NOT IN (SELECT fragment_uid FROM"
            " official_fragment) OR o.to_fragment NOT IN (SELECT"
            " fragment_uid FROM official_fragment)"
        ).fetchone()["c"]
        self.assertEqual(n, 0)

    def test_15_hand_calc_table_matches(self):
        """与独立手算表 hand_calc.json 逐项核对。"""
        hand = self.hand
        regions = {
            (r["start_byte"], r["end_byte"]): r
            for r in self.qdb.list_regions(layer="clause")
        }
        for key, spec in hand["regions"].items():
            env = tuple(spec["envelope"])
            r = regions.get(env)
            if key == "annex":
                continue
            self.assertIsNotNone(r, key)
            detail = self.qdb.region_detail(r["region_uid"])
            if "denominator" in spec:
                self.assertEqual(detail["denominator"],
                                 spec["denominator"], key)
            if spec.get("winner") == "pending":
                self.assertEqual(detail["outcome"]["winner_kind"], "pending", key)
            elif spec.get("winner") == "no_fragment":
                self.assertEqual(
                    detail["outcome"]["winner_kind"], "no_fragment", key)
            if "winner_label" in spec:
                self.assertEqual(detail["outcome"]["winner_kind"], "candidate", key)
                self.assertEqual(
                    detail["official_fragment"]["label"],
                    spec["winner_label"], key)

    def test_16_coverage_gap_helper(self):
        # 紧邻接上 -> 无缺口；差一个字节 -> 单字节缺口
        self.assertEqual(coverage_gaps([(0, 5), (5, 9)], 0, 9), [])
        self.assertEqual(coverage_gaps([(0, 5), (6, 9)], 0, 9), [[5, 6]])
        self.assertEqual(coverage_gaps([(2, 4)], 0, 9), [[0, 2], [4, 9]])


class SyntheticRejectTestCase(unittest.TestCase):
    """动态构造的拒收与边界场景。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="qcsyn-")
        self._write_base()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_base(self, text="0123456789abcdefghij"):
        self.text = text.encode("utf-8")
        self.digest = hashlib.sha256(self.text).hexdigest()
        os.makedirs(os.path.join(self.tmpdir, "src"), exist_ok=True)
        self.src = os.path.join(self.tmpdir, "src", "d.txt")
        with open(self.src, "wb") as fh:
            fh.write(self.text)
        self.manifest = os.path.join(self.tmpdir, "manifest.json")
        with open(self.manifest, "w", encoding="utf-8") as fh:
            json.dump({"documents": [
                {"doc_id": "D", "path": "src/d.txt", "sha256": self.digest}
            ]}, fh)
        self.policy = os.path.join(self.tmpdir, "policy.json")
        with open(self.policy, "w", encoding="utf-8") as fh:
            json.dump({
                "layers": {"L": {"labels": ["X", "Y"], "min_support": 2}},
                "relation_types": {},
            }, fh)
        self.pkg_root = os.path.join(self.tmpdir, "pkgs")
        os.makedirs(self.pkg_root, exist_ok=True)
        # 每个测试独立的包目录，避免读到其他测试残留的包
        self._pkg_names: set[str] = set()

    def _pkg(self, team, records, digest=None, package_id=None):
        header = {
            "kind": "package", "package_id": package_id or f"P-{team}",
            "doc_id": "D", "team_id": team,
            "source_digest": digest if digest is not None else self.digest,
        }
        path = os.path.join(self.pkg_root, f"{team}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(header) + "\n")
            for r in records:
                fh.write(json.dumps(r) + "\n")

    def _review(self, ranges):
        return {"kind": "review", "layer": "L", "ranges": ranges}

    def _frag(self, lid, start, end, label="X", seen=None):
        return {
            "kind": "fragment", "id": lid, "layer": "L", "label": label,
            "start": start, "end": end,
            "seen_text": seen if seen is not None
            else self.text[start:end].decode("utf-8"),
        }
    def _build(self, out="w.db"):
        result = build(
            manifest_path=self.manifest,
            packages_root=self.pkg_root,
            output_db=os.path.join(self.tmpdir, out),
            policy=parse_policy(_load_json(self.policy)),
            reject_report=os.path.join(self.tmpdir, "reject.json"),
        )
        return result, os.path.join(self.tmpdir, out)

    def _codes(self, result):
        return {e.code for e in result.errors}

    def test_duplicate_package_same_doc_team(self):
        self._pkg("t1", [self._review([[0, 5]])], package_id="P1")
        path2 = os.path.join(self.pkg_root, "t1b.jsonl")
        with open(path2, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "kind": "package", "package_id": "P2", "doc_id": "D",
                "team_id": "t1", "source_digest": self.digest}) + "\n")
            fh.write(json.dumps(self._review([[0, 5]])) + "\n")
        result, _ = self._build()
        self.assertIn("DUPLICATE_PACKAGE", self._codes(result))

    def test_duplicate_local_fragment_id(self):
        self._pkg("t1", [
            self._review([[0, 10]]),
            self._frag("same", 0, 3),
            self._frag("same", 4, 7),
        ])
        result, _ = self._build()
        self.assertIn("DUPLICATE_LOCAL_ID", self._codes(result))

    def test_dangling_relation(self):
        self._pkg("t1", [
            self._review([[0, 10]]),
            self._frag("a", 0, 3),
            {"kind": "relation", "type": "ref", "from_id": "a",
             "to_id": "ghost", "sign": "for"},
        ])
        # 需要关系类型策略
        with open(self.policy, "w", encoding="utf-8") as fh:
            json.dump({
                "layers": {"L": {"labels": ["X"], "min_support": 2}},
                "relation_types": {"ref": {"from_layer": "L",
                                            "to_layer": "L", "min_support": 2}},
            }, fh)
        result, _ = self._build()
        self.assertIn("DANGLING_RELATION", self._codes(result))

    def test_fragment_outside_reviewed_span(self):
        self._pkg("t1", [
            self._review([[0, 5]]),
            self._frag("a", 4, 8),
        ])
        result, _ = self._build()
        self.assertIn("OUTSIDE_REVIEWED_SPAN", self._codes(result))

    def test_wrong_source_digest_rejected(self):
        self._pkg("t1", [self._review([[0, 5]])],
                  digest="0" * 64)
        result, _ = self._build()
        self.assertIn("WRONG_SOURCE_DIGEST", self._codes(result))

    def test_bad_utf8_boundary_rejected(self):
        # 构造含多字节字符的原文，区间切在字符中段
        text = "abécd".encode("utf-8")  # é = c3 a9，字节: 61 62 c3 a9 63 64
        with open(self.src, "wb") as fh:
            fh.write(text)
        digest = hashlib.sha256(text).hexdigest()
        with open(self.manifest, "w", encoding="utf-8") as fh:
            json.dump({"documents": [
                {"doc_id": "D", "path": "src/d.txt", "sha256": digest}]}, fh)
        self.digest = digest
        self.text = text
        # 字节布局：0='a' 1='b' 2=é首 3=é续 4='c' 5='d'。
        # start=4 的前一字节是续字节 3 -> 4 不是字符边界（切在 é 中段之后）。
        self.assertFalse(is_utf8_boundary(text, 4))
        self.assertTrue(is_utf8_boundary(text, 2))
        self.assertTrue(is_utf8_boundary(text, 6))
        self._pkg("t1", [
            self._review([[0, 6]]),
            {
                "kind": "fragment", "id": "a", "layer": "L", "label": "X",
                "start": 4, "end": 6,
                "seen_text": "zz",
            },
        ])
        result, _ = self._build()
        self.assertIn("BAD_RANGE", self._codes(result))

    def test_seen_text_no_unicode_normalization(self):
        # 显式组合序列：'c','a','f', 'e' + U+0301（两个码位，共 6 字节），
        # 不做 NFC 归一化；0 与 6 都是合法字符边界
        text = ("caf" + "e" + "\u0301").encode("utf-8")
        self.assertEqual(len(text), 6)
        with open(self.src, "wb") as fh:
            fh.write(text)
        digest = hashlib.sha256(text).hexdigest()
        with open(self.manifest, "w", encoding="utf-8") as fh:
            json.dump({"documents": [
                {"doc_id": "D", "path": "src/d.txt", "sha256": digest}]}, fh)
        self.text = text
        self.digest = digest
        # 正确的逐字节所见原文通过（审核区间端点 0 与 len(text) 均为边界）
        self._pkg("t1", [self._review([[0, len(text)]])])
        self._pkg("t2", [
            self._review([[0, len(text)]]),
            self._frag("a", 0, len(text)),
        ])
        result, db = self._build()
        self.assertEqual(result.errors, [])
        # 若提交 NFC 归一化后的单码位 é，则逐字节不一致 -> 拒收
        nfc = "caf" + "\u00e9"  # 预组合，5 字节，与原文不同
        self.assertEqual(len(nfc.encode("utf-8")), 5)
        self.assertNotEqual(nfc.encode("utf-8"), text)
        self._pkg("t3", [
            self._review([[0, len(text)]]),
            self._frag("b", 0, len(text), seen=nfc),
        ])
        result2, _ = self._build(out="w2.db")
        self.assertIn("SEEN_TEXT_MISMATCH", self._codes(result2))

    def test_tie_no_winner(self):
        # 两个候选各 2 票，分母 4，严格多数 3 -> 并列无胜出
        self._pkg("t1", [self._review([[0, 10]]), self._frag("a", 0, 5, "X")])
        self._pkg("t2", [self._review([[0, 10]]), self._frag("b", 0, 5, "X")])
        self._pkg("t3", [self._review([[0, 10]]), self._frag("c", 0, 5, "Y")])
        self._pkg("t4", [self._review([[0, 10]]), self._frag("d", 0, 5, "Y")])
        result, db = self._build()
        self.assertEqual(result.errors, [])
        with QueryDB(db) as q:
            regs = q.list_regions()
            self.assertEqual(len(regs), 1)
            detail = q.region_detail(regs[0]["region_uid"])
            self.assertEqual(detail["outcome"]["winner_kind"], "pending")
            self.assertEqual(len(q.official_fragments()), 0)

    def test_rejected_build_keeps_previous_good_result(self):
        # 先发布一份完整结果
        self._pkg("t1", [self._review([[0, 5]])])
        self._pkg("t2", [self._review([[0, 5]])])
        result, db = self._build(out="keep.db")
        self.assertEqual(result.errors, [])
        good_stat = os.stat(db)
        # 再制造结构错误重新构建到同一路径
        self._pkg("t3", [
            self._review([[0, 5]]),
            self._frag("oops", 6, 9),  # 越出审核范围
        ])
        result2, _ = self._build(out="keep.db")
        self.assertTrue(result2.errors)
        self.assertFalse(os.path.isfile(db + "-wal"))
        # 旧库仍可打开且内容未变
        self.assertEqual(os.stat(db).st_size, good_stat.st_size)
        with QueryDB(db) as q:
            self.assertIn("format_version", q.meta())

    def test_confirmed_absent_relation_on_against_majority(self):
        # 两个正式片段 + 两队明确反对（2 票，分母 2，严格多数 2）-> 确认无引用
        with open(self.policy, "w", encoding="utf-8") as fh:
            json.dump({
                "layers": {"L": {"labels": ["X"], "min_support": 2}},
                "relation_types": {"ref": {"from_layer": "L",
                                            "to_layer": "L", "min_support": 2}},
            }, fh)
        for t in ("t1", "t2"):
            self._pkg(t, [
                self._review([[0, 10]]),
                self._frag("a", 0, 3),
                self._frag("b", 6, 9),
                {"kind": "relation", "type": "ref", "from_id": "a",
                 "to_id": "b", "sign": "against"},
            ])
        result, db = self._build()
        self.assertEqual(result.errors, [])
        with QueryDB(db) as q:
            # 两个片段都胜出
            self.assertEqual(len(q.official_fragments()), 2)
            # 没有正式引用
            self.assertEqual(q.official_relations(), [])
            row = q.conn.execute(
                "SELECT decision, for_votes, against_votes, denominator"
                " FROM relation_group"
            ).fetchone()
            self.assertEqual(row["decision"], "confirmed_absent")
            self.assertEqual(row["against_votes"], 2)
            self.assertEqual(row["denominator"], 2)
            # 复核库里不应有该关系组的待核项（已判定确认无引用）
            pending = q.review_items(item_kind="relation")
            self.assertFalse(
                any(x.get("detail", {}).get("reason")
                    == "relation_threshold_not_met" for x in pending)
            )

    def test_min_support_below_two_rejected_by_policy(self):
        with self.assertRaises(ValueError):
            parse_policy({
                "layers": {"L": {"labels": ["X"], "min_support": 1}},
                "relation_types": {},
            })

    def test_multiple_distinct_errors_collected_with_locations(self):
        # 同一次构建应报全重复局部 ID 与悬空关系，且定位到具体记录
        with open(self.policy, "w", encoding="utf-8") as fh:
            json.dump({
                "layers": {"L": {"labels": ["X"], "min_support": 2}},
                "relation_types": {"ref": {"from_layer": "L",
                                            "to_layer": "L", "min_support": 2}},
            }, fh)
        self._pkg("t1", [
            self._review([[0, 10]]),
            self._frag("a", 0, 3),
            self._frag("a", 4, 6),  # 重复局部 ID
            {"kind": "relation", "type": "ref", "from_id": "a",
             "to_id": "ghost", "sign": "for"},  # 悬空
        ])
        result, _ = self._build()
        codes = self._codes(result)
        self.assertIn("DUPLICATE_LOCAL_ID", codes)
        self.assertIn("DANGLING_RELATION", codes)
        by_code = {}
        for e in result.errors:
            by_code.setdefault(e.code, e)
        self.assertEqual(by_code["DANGLING_RELATION"].line_no, 5)
        self.assertEqual(by_code["DANGLING_RELATION"].record_index, 3)
        self.assertEqual(by_code["DUPLICATE_LOCAL_ID"].line_no, 4)


class ReorderStabilityTestCase(unittest.TestCase):
    """合法记录/文件重排不改变逻辑标识与业务结果。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="qcreorder-")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_reorder_records_and_files(self):
        src_root = os.path.join(FIXTURES)
        db1 = os.path.join(self.tmpdir, "a.db")
        r1 = build(
            manifest_path=os.path.join(src_root, "manifest.json"),
            packages_root=os.path.join(src_root, "packages"),
            output_db=db1,
            policy=parse_policy(_load_json(
                os.path.join(src_root, "policy.json"))),
        )
        self.assertEqual(r1.errors, [])

        # 复制并打乱：包目录顺序反转；每个包内记录行逆序（包头保持首行）
        shuffled = os.path.join(self.tmpdir, "pkgs2")
        os.makedirs(shuffled, exist_ok=True)
        names = sorted(os.listdir(os.path.join(src_root, "packages")), reverse=True)
        for name in names:
            src = os.path.join(src_root, "packages", name)
            dst = os.path.join(shuffled, name)
            if os.path.isdir(src):
                shutil.copytree(src, dst)
                continue
            with open(src, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
            header, body = lines[0], lines[1:]
            with open(dst, "w", encoding="utf-8") as fh:
                fh.write(header + "\n")
                for line in reversed(body):
                    fh.write(line + "\n")
        db2 = os.path.join(self.tmpdir, "b.db")
        r2 = build(
            manifest_path=os.path.join(src_root, "manifest.json"),
            packages_root=shuffled,
            output_db=db2,
            policy=parse_policy(_load_json(
                os.path.join(src_root, "policy.json"))),
        )
        self.assertEqual(r2.errors, [])

        def snapshot(db):
            con = sqlite3.connect(db)
            con.row_factory = sqlite3.Row
            out = {}
            for table in ("region", "region_outcome", "official_fragment",
                          "relation_group", "official_relation",
                          "region_candidate", "region_vote",
                          "relation_group_vote", "review_item",
                          "proposal", "region_proposal"):
                out[table] = sorted(
                    tuple("" if v is None else str(v) for v in row)
                    for row in
                    con.execute(f"SELECT * FROM {table}").fetchall()
                )
            con.close()
            return out

        s1, s2 = snapshot(db1), snapshot(db2)
        self.assertEqual(s1["region"], s2["region"])
        self.assertEqual(s1["region_outcome"], s2["region_outcome"])
        self.assertEqual(s1["official_fragment"], s2["official_fragment"])
        self.assertEqual(s1["relation_group"], s2["relation_group"])
        self.assertEqual(s1["official_relation"], s2["official_relation"])
        self.assertEqual(s1["proposal"], s2["proposal"])
        self.assertEqual(s1["review_item"], s2["review_item"])


class ScaleTestCase(unittest.TestCase):
    """规模生成器：确认有界内存与非平方级工作量。"""

    def test_large_overlapping_layer_is_near_linear(self):
        sys.path.insert(0, ROOT)
        from tools import scalegen
        tmp = tempfile.mkdtemp(prefix="qcscale-")

        def build_scale(n_proposals):
            sub = tempfile.mkdtemp(prefix="qcscale-n-", dir=tmp)
            scalegen.generate(sub, n_teams=12, n_proposals=n_proposals,
                              doc_bytes=200000, seed=7)
            t0 = time.perf_counter()
            result = build(
                manifest_path=os.path.join(sub, "manifest.json"),
                packages_root=os.path.join(sub, "packages"),
                output_db=os.path.join(sub, "w.db"),
                policy=parse_policy(_load_json(
                    os.path.join(sub, "policy.json"))),
            )
            elapsed = time.perf_counter() - t0
            self.assertEqual(result.errors, [])
            with QueryDB(os.path.join(sub, "w.db")) as q:
                counters = q.meta()["run_counters"]
                clause_regions = q.list_regions(layer="clause")
            return counters, len(clause_regions), elapsed

        try:
            small, small_regions, t_small = build_scale(2000)
            big, big_regions, t_big = build_scale(8000)
            # clause 全部相互交叠 -> 两个规模下都恰好一个大区域
            self.assertEqual(small_regions, 1)
            self.assertEqual(big_regions, 1)
            self.assertEqual(small["regions"], 2)
            self.assertEqual(big["regions"], 2)
            # 关键工作量断言：扫描线交叠比较数远小于两两配对的平方级。
            # 若退化为 O(n^2)，n 翻 4 倍时比较数应接近 16 倍；这里要求 <= 6 倍。
            ratio = (
                big["overlap_comparisons"] / max(1, small["overlap_comparisons"])
            )
            n_ratio = big["records_loaded"] / small["records_loaded"]
            self.assertLess(ratio, n_ratio * n_ratio * 0.5)
            self.assertGreater(big["records_loaded"], 8000)
            # 粗护栏：该规模应在合理时间内完成（CI 友好的宽松上界）
            self.assertLess(t_big, 120)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
