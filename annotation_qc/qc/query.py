# -*- coding: utf-8 -*-
"""只读查询与证据展开。

- 以 ``mode=ro`` 打开正式库，任何写操作都会失败；
- 所有查询都带 doc_id 条件并走索引，常规查询不扫描别的文档；
- 覆盖判断在查询时从落盘的 review_range 重新合并复算，
  且显式返回覆盖缺口（不只是布尔值）；
- 证据链：原文摘要 + 字节坐标、包与记录位置（文件/行/序号）、
  审核区间；关系可追到两个局部端点、各自区域与是否被采信。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Iterator, Optional

from .build import coverage_gaps


class QueryDB:
    def __init__(self, db_path: str):
        self.db_path = os.path.abspath(db_path)
        if not os.path.isfile(self.db_path):
            raise FileNotFoundError(self.db_path)
        uri = "file:" + self.db_path + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # 总览
    # ------------------------------------------------------------------

    def meta(self) -> dict:
        rows = self.conn.execute("SELECT key, value FROM build_meta").fetchall()
        meta = {r["key"]: r["value"] for r in rows}
        counters = {
            r["name"]: r["value"]
            for r in self.conn.execute(
                "SELECT name, value FROM run_counter ORDER BY name"
            )
        }
        meta["run_counters"] = counters
        return meta

    def documents(self) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT doc_id, rel_path, sha256, byte_length, verified_sha,"
                " verified FROM source_doc ORDER BY doc_id"
            )
        ]

    # ------------------------------------------------------------------
    # 正式记录
    # ------------------------------------------------------------------

    def official_fragments(
        self, doc_id: Optional[str] = None, layer: Optional[str] = None
    ) -> list[dict]:
        sql = (
            "SELECT fragment_uid, doc_id, layer, label, start_byte, end_byte,"
            " region_uid, text_sha256, seen_text FROM official_fragment WHERE 1=1"
        )
        params: list = []
        if doc_id is not None:
            sql += " AND doc_id=?"
            params.append(doc_id)
        if layer is not None:
            sql += " AND layer=?"
            params.append(layer)
        sql += " ORDER BY doc_id, layer, start_byte, end_byte"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def official_relations(
        self, doc_id: Optional[str] = None, rel_type: Optional[str] = None
    ) -> list[dict]:
        sql = (
            "SELECT o.rel_group_uid, o.doc_id, o.rel_type, o.from_fragment,"
            " o.to_fragment, g.for_votes, g.against_votes, g.conflict_votes,"
            " g.denominator, g.min_support, g.strict_majority"
            " FROM official_relation o JOIN relation_group g"
            " ON g.rel_group_uid = o.rel_group_uid WHERE 1=1"
        )
        params: list = []
        if doc_id is not None:
            sql += " AND o.doc_id=?"
            params.append(doc_id)
        if rel_type is not None:
            sql += " AND o.rel_type=?"
            params.append(rel_type)
        sql += " ORDER BY o.doc_id, o.rel_type, o.from_fragment, o.to_fragment"
        out = []
        for r in self.conn.execute(sql, params):
            item = dict(r)
            item["from"] = self._fragment_ref(r["from_fragment"])
            item["to"] = self._fragment_ref(r["to_fragment"])
            out.append(item)
        return out

    def _fragment_ref(self, fragment_uid: str) -> dict:
        r = self.conn.execute(
            "SELECT fragment_uid, doc_id, layer, label, start_byte, end_byte"
            " FROM official_fragment WHERE fragment_uid=?",
            (fragment_uid,),
        ).fetchone()
        return dict(r) if r else {"fragment_uid": fragment_uid}

    # ------------------------------------------------------------------
    # 复核工作库
    # ------------------------------------------------------------------

    def review_items(
        self,
        doc_id: Optional[str] = None,
        layer: Optional[str] = None,
        rel_type: Optional[str] = None,
        item_kind: Optional[str] = None,
    ) -> list[dict]:
        sql = "SELECT * FROM review_item WHERE 1=1"
        params: list = []
        if doc_id is not None:
            sql += " AND doc_id=?"
            params.append(doc_id)
        if layer is not None:
            sql += " AND layer=?"
            params.append(layer)
        if rel_type is not None:
            sql += " AND rel_type=?"
            params.append(rel_type)
        if item_kind is not None:
            sql += " AND item_kind=?"
            params.append(item_kind)
        sql += " ORDER BY doc_id, item_kind, layer, rel_type"
        return [
            {**dict(r), "detail": json.loads(r["detail_json"])}
            for r in self.conn.execute(sql, params)
        ]

    # ------------------------------------------------------------------
    # 区域逐项展开：分母、候选支持、被排除意见、覆盖缺口与证据
    # ------------------------------------------------------------------

    def region_detail(self, region_uid: str) -> dict:
        reg = self.conn.execute(
            "SELECT * FROM region WHERE region_uid=?", (region_uid,)
        ).fetchone()
        if reg is None:
            raise KeyError(region_uid)
        doc = self.conn.execute(
            "SELECT doc_id, rel_path, sha256, byte_length FROM source_doc"
            " WHERE doc_id=?",
            (reg["doc_id"],),
        ).fetchone()
        outcome = self.conn.execute(
            "SELECT * FROM region_outcome WHERE region_uid=?", (region_uid,)
        ).fetchone()

        eligibility = []
        for e in self.conn.execute(
            "SELECT * FROM region_eligibility WHERE region_uid=?"
            " ORDER BY coverage, package_uid",
            (region_uid,),
        ):
            entry = dict(e)
            # 查询时从落盘区间复算覆盖，并给出缺口
            merged = self._merged_reviews(e["package_uid"], reg["layer"])
            entry["merged_review_ranges"] = merged
            entry["coverage_gaps"] = coverage_gaps(
                merged, reg["start_byte"], reg["end_byte"]
            )
            entry["package"] = self._package_evidence(e["package_uid"])
            eligibility.append(entry)

        candidates = []
        for c in self.conn.execute(
            "SELECT * FROM region_candidate WHERE region_uid=?"
            " ORDER BY is_no_fragment, start_byte, end_byte, label",
            (region_uid,),
        ):
            cand = dict(c)
            if cand["is_no_fragment"]:
                cand["label_display"] = "NO_FRAGMENT"
            votes = []
            for v in self.conn.execute(
                "SELECT * FROM region_vote WHERE region_uid=?"
                " AND candidate_uid IS ? ORDER BY package_uid",
                (region_uid, cand["candidate_uid"]),
            ):
                votes.append(dict(v))
            # 自相矛盾票的 candidate_uid 为 NULL，单独挂在区域上
            cand["votes"] = votes
            cand["vote_count"] = len(votes)
            candidates.append(cand)
        conflict_votes = [
            dict(v)
            for v in self.conn.execute(
                "SELECT * FROM region_vote WHERE region_uid=?"
                " AND vote_kind='self_contradiction' ORDER BY package_uid",
                (region_uid,),
            )
        ]

        members = []
        for m in self.conn.execute(
            "SELECT p.*, r.file_name, r.line_no, r.record_index"
            " FROM region_proposal rp"
            " JOIN proposal p ON p.proposal_uid = rp.proposal_uid"
            " JOIN record r ON r.record_uid = p.record_uid"
            " WHERE rp.region_uid=? ORDER BY p.package_uid, p.start_byte",
            (region_uid,),
        ):
            members.append(self._proposal_evidence(m))

        # 被排除的意见：部分覆盖团队的提议（不投票，但可查原因与缺口）
        excluded = []
        partial_pkgs = {
            e["package_uid"]
            for e in eligibility
            if e["coverage"] == "partial"
        }
        for m in members:
            if m["package_uid"] in partial_pkgs:
                gap = next(
                    (
                        e["coverage_gaps"]
                        for e in eligibility
                        if e["package_uid"] == m["package_uid"]
                    ),
                    [],
                )
                excluded.append(
                    {
                        "proposal": m,
                        "reason": "team_only_partially_covered_region",
                        "coverage_gaps": gap,
                    }
                )

        official = self.conn.execute(
            "SELECT fragment_uid, label, start_byte, end_byte, text_sha256"
            " FROM official_fragment WHERE region_uid=?",
            (region_uid,),
        ).fetchone()

        return {
            "region_uid": region_uid,
            "doc": dict(doc),
            "layer": reg["layer"],
            "envelope": [reg["start_byte"], reg["end_byte"]],
            "byte_coordinates": {
                "start": reg["start_byte"],
                "end": reg["end_byte"],
                "source_sha256": doc["sha256"],
                "source_path": doc["rel_path"],
            },
            "outcome": dict(outcome) if outcome else None,
            "denominator": outcome["denominator"] if outcome else None,
            "candidates": candidates,
            "self_contradicting_teams": conflict_votes,
            "eligibility": eligibility,
            "member_proposals": members,
            "excluded_opinions": excluded,
            "official_fragment": dict(official) if official else None,
        }

    def list_regions(
        self, doc_id: Optional[str] = None, layer: Optional[str] = None
    ) -> list[dict]:
        sql = (
            "SELECT r.region_uid, r.doc_id, r.layer, r.start_byte, r.end_byte,"
            " o.winner_kind, o.denominator"
            " FROM region r LEFT JOIN region_outcome o"
            " ON o.region_uid = r.region_uid WHERE 1=1"
        )
        params: list = []
        if doc_id is not None:
            sql += " AND r.doc_id=?"
            params.append(doc_id)
        if layer is not None:
            sql += " AND r.layer=?"
            params.append(layer)
        sql += " ORDER BY r.doc_id, r.layer, r.start_byte"
        return [dict(r) for r in self.conn.execute(sql, params)]

    # ------------------------------------------------------------------
    # 关系逐项展开：归票、局部端点、阻断原因、对应区域
    # ------------------------------------------------------------------

    def relation_group_detail(self, rel_group_uid: str) -> dict:
        g = self.conn.execute(
            "SELECT * FROM relation_group WHERE rel_group_uid=?",
            (rel_group_uid,),
        ).fetchone()
        if g is None:
            raise KeyError(rel_group_uid)
        votes = []
        for v in self.conn.execute(
            "SELECT * FROM relation_group_vote WHERE rel_group_uid=?"
            " ORDER BY package_uid",
            (rel_group_uid,),
        ):
            item = dict(v)
            item["record_uids_list"] = json.loads(v["record_uids"])
            item["records"] = [
                self._record_evidence(uid) for uid in item["record_uids_list"]
            ]
            item["package"] = self._package_evidence(v["package_uid"])
            votes.append(item)
        return {
            "group": dict(g),
            "from_fragment": self._fragment_ref(g["from_fragment"]),
            "to_fragment": self._fragment_ref(g["to_fragment"]),
            "votes": votes,
            "confirmed": g["decision"] == "confirmed",
            "decision": g["decision"],
        }

    def blocked_relations(
        self, doc_id: Optional[str] = None, rel_type: Optional[str] = None
    ) -> list[dict]:
        sql = (
            "SELECT rv.*, r.rel_type AS rt, r.from_local, r.to_local, r.sign,"
            " r.package_uid, r.team_id, re.resolved, re.blocked_reason,"
            " re.from_proposal, re.to_proposal"
            " FROM review_item rv"
            " JOIN raw_relation r ON r.record_uid = rv.record_uid"
            " JOIN relation_endpoint re ON re.record_uid = rv.record_uid"
            " WHERE rv.item_kind='relation' AND rv.rel_group_uid IS NULL"
        )
        params: list = []
        if doc_id is not None:
            sql += " AND rv.doc_id=?"
            params.append(doc_id)
        if rel_type is not None:
            sql += " AND rv.rel_type=?"
            params.append(rel_type)
        out = []
        for row in self.conn.execute(sql, params):
            item = dict(row)
            item["record"] = self._record_evidence(row["record_uid"])
            item["endpoint_regions"] = {
                "from": self._proposal_region_trace(row["from_proposal"]),
                "to": self._proposal_region_trace(row["to_proposal"]),
            }
            out.append(item)
        return out

    def relation_trace_for_record(self, record_uid: str) -> dict:
        """一条原始关系的完整追踪：局部端点、区域、是否采信、归票组。"""
        r = self.conn.execute(
            "SELECT * FROM raw_relation WHERE record_uid=?", (record_uid,)
        ).fetchone()
        if r is None:
            raise KeyError(record_uid)
        ep = self.conn.execute(
            "SELECT * FROM relation_endpoint WHERE record_uid=?", (record_uid,)
        ).fetchone()
        return {
            "raw_relation": dict(r),
            "record": self._record_evidence(record_uid),
            "from_endpoint": self._proposal_region_trace(ep["from_proposal"]),
            "to_endpoint": self._proposal_region_trace(ep["to_proposal"]),
            "resolved": bool(ep["resolved"]),
            "blocked_reason": ep["blocked_reason"],
        }

    # ------------------------------------------------------------------
    # 证据辅助
    # ------------------------------------------------------------------

    def _merged_reviews(self, package_uid: str, layer: str) -> list[list[int]]:
        cur = self.conn.execute(
            "SELECT start_byte, end_byte FROM review_range"
            " WHERE package_uid=? AND layer=? ORDER BY start_byte, end_byte",
            (package_uid, layer),
        )
        merged: list[list[int]] = []
        for s, e in cur:
            if not merged or s > merged[-1][1]:
                merged.append([s, e])
            else:
                merged[-1][1] = max(merged[-1][1], e)
        return merged

    def _package_evidence(self, package_uid: str) -> dict:
        p = self.conn.execute(
            "SELECT package_uid, package_id, doc_id, team_id, source_digest,"
            " root_kind, content_hash FROM package WHERE package_uid=?",
            (package_uid,),
        ).fetchone()
        return dict(p) if p else {"package_uid": package_uid}

    def _record_evidence(self, record_uid: str) -> dict:
        r = self.conn.execute(
            "SELECT record_uid, package_uid, kind, file_name, line_no,"
            " record_index, payload_json FROM record WHERE record_uid=?",
            (record_uid,),
        ).fetchone()
        if r is None:
            return {"record_uid": record_uid}
        out = dict(r)
        out["payload"] = json.loads(r["payload_json"])
        del out["payload_json"]
        return out

    def _proposal_evidence(self, row: sqlite3.Row) -> dict:
        out = dict(row)
        out["package"] = self._package_evidence(out["package_uid"])
        out["record_position"] = {
            "file": out.pop("file_name"),
            "line": out.pop("line_no"),
            "record_index": out.pop("record_index"),
        }
        out["merged_review_ranges"] = self._merged_reviews(
            out["package_uid"], out["layer"]
        )
        return out

    def _proposal_region_trace(self, proposal_uid: Optional[str]) -> Optional[dict]:
        if proposal_uid is None:
            return None
        p = self.conn.execute(
            "SELECT p.proposal_uid, p.package_uid, p.local_id, p.layer, p.label,"
            " p.start_byte, p.end_byte, rp.region_uid, o.winner_kind,"
            " o.winner_candidate, f.fragment_uid AS official_uid"
            " FROM proposal p"
            " JOIN region_proposal rp ON rp.proposal_uid = p.proposal_uid"
            " JOIN region_outcome o ON o.region_uid = rp.region_uid"
            " LEFT JOIN official_fragment f ON f.region_uid = rp.region_uid"
            " AND f.candidate_uid = o.winner_candidate"
            " WHERE p.proposal_uid=?",
            (proposal_uid,),
        ).fetchone()
        if p is None:
            return None
        trace = dict(p)
        trace["adopted"] = (
            p["winner_kind"] == "candidate"
            and self._is_winning_proposal(proposal_uid, p["region_uid"])
        )
        return trace

    def _is_winning_proposal(self, proposal_uid: str, region_uid: str) -> bool:
        p = self.conn.execute(
            "SELECT start_byte, end_byte, label FROM proposal WHERE proposal_uid=?",
            (proposal_uid,),
        ).fetchone()
        o = self.conn.execute(
            "SELECT winner_candidate FROM region_outcome WHERE region_uid=?",
            (region_uid,),
        ).fetchone()
        c = self.conn.execute(
            "SELECT 1 FROM region_candidate WHERE candidate_uid=?"
            " AND start_byte=? AND end_byte=? AND label=?",
            (o["winner_candidate"], p["start_byte"], p["end_byte"], p["label"]),
        ).fetchone()
        return c is not None

    # ------------------------------------------------------------------
    # 流式导出（JSONL，逐行产出，不整表驻留）
    # ------------------------------------------------------------------

    def export_stream(self, what: str, **filters) -> Iterator[str]:
        """流式导出 official_fragments / official_relations / review_items。

        使用服务端游标逐行读取并立即序列化，不把结果集整体驻留内存。
        """
        if what == "official_fragments":
            sql = (
                "SELECT fragment_uid, doc_id, layer, label, start_byte, end_byte,"
                " region_uid, text_sha256, seen_text FROM official_fragment"
                " WHERE 1=1"
            )
            sql, params = self._apply_filters(
                sql, filters, (("doc_id", "doc_id"), ("layer", "layer"))
            )
            sql += " ORDER BY doc_id, layer, start_byte, end_byte"
            cur = self.conn.execute(sql, params)
            for r in cur:
                yield json.dumps(dict(r), ensure_ascii=False, sort_keys=True) + "\n"
            return

        if what == "official_relations":
            sql = (
                "SELECT o.rel_group_uid, o.doc_id, o.rel_type, o.from_fragment,"
                " o.to_fragment, g.for_votes, g.against_votes, g.conflict_votes,"
                " g.denominator, g.min_support, g.strict_majority"
                " FROM official_relation o JOIN relation_group g"
                " ON g.rel_group_uid = o.rel_group_uid WHERE 1=1"
            )
            sql, params = self._apply_filters(
                sql, filters, (("doc_id", "o.doc_id"), ("rel_type", "o.rel_type"))
            )
            sql += " ORDER BY o.doc_id, o.rel_type"
            cur = self.conn.execute(sql, params)
            for r in cur:
                item = dict(r)
                item["from"] = self._fragment_ref(r["from_fragment"])
                item["to"] = self._fragment_ref(r["to_fragment"])
                yield json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
            return

        if what == "review_items":
            sql = "SELECT * FROM review_item WHERE 1=1"
            sql, params = self._apply_filters(
                sql,
                filters,
                (
                    ("doc_id", "doc_id"),
                    ("layer", "layer"),
                    ("rel_type", "rel_type"),
                    ("item_kind", "item_kind"),
                ),
            )
            sql += " ORDER BY doc_id, item_kind, layer, rel_type"
            cur = self.conn.execute(sql, params)
            for r in cur:
                item = dict(r)
                item["detail"] = json.loads(r["detail_json"])
                yield json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
            return

        raise ValueError(f"unknown export target: {what}")

    @staticmethod
    def _apply_filters(sql: str, filters: dict, mapping) -> tuple[str, list]:
        params: list = []
        for key, column in mapping:
            if filters.get(key) is not None:
                sql += f" AND {column}=?"
                params.append(filters[key])
        return sql, params
