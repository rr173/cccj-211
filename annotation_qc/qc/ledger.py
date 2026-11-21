# -*- coding: utf-8 -*-
"""离线分仓台账：把已审定的公文分成训练 / 评测 / 隔离三张清单。

设计要点（与需求逐条对应）：

- **只读上游**：选入公文必须给出档号 + 已登记发布 ID；台账读取该发布下的
  审定片段与公文身份（``source_doc`` 登记的整篇底稿摘要），不读取、也不以
  当前审定稿代替。一个档号同时只选一个发布，可改选、可撤出。
- **同组证据**：整篇底稿摘要相同的公文同组；达到最短字节数的审定片段按原样
  UTF-8 字节做摘要（不做清洗），摘要相同即同组；建账时可配置片段摘要豁免表
  （整篇底稿摘要不可豁免）；人工同源证据独立登记/撤销，撤销不抹掉同对公文
  的其它证据；关联可传递。共同摘要只存「摘要 → 公文」索引，不展开两两记录。
- **交付约束**：确认交付按档号永久锁定仓位，并把当时实际交付的整篇与合格
  片段摘要留作历史证据；改选/撤出不覆盖历史摘要。组内只有一种已交付仓位
  时全组服从；两种都有时整组在用公文隔离；没有历史仓位的组按组内 UTF-8
  字节序最小档号的 SHA-256 首字节奇偶分仓（偶训练、奇评测），不做配额平衡。
- **变更批**：选入/改选/撤出/证据变动合为一批，按整批完成后的状态计算仓位；
  任何一条非法（未知发布、档号不属于该发布、非法证据变更）整批不入账。
- **请求幂等**：操作携带请求 ID；同一请求重放取回原回执，不同内容不能共用
  ID。变更与交付确认均以明确的当前台账版本为前提；旧版不能二次确认成新交付。
- **局部重算**：变更只重算受影响的连通分量（含证据撤销后分裂的子组与新连入
  的组），不重读不相干的发布明细、不遍历不相干的分量；诊断信息记录每次运行
  读取的发布/公文与访问的图节点/证据量。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from collections import deque
from typing import Optional

from .ledger_common import (
    LedgerError,
    connect,
    db_path,
    exclusive,
    ledger_version,
    meta_get,
    meta_set,
    transaction,
    utc_now,
)
from .ledger_upstream import Upstream
from .util import canonical_json, sha256_hex, stable_id

TRAIN = "train"
EVAL = "eval"
QUARANTINE = "quarantine"

_NEEDED_UPSTREAM_TABLES = ("release", "release_fragment", "source_doc")


def _bin_of(doc_id: str) -> int:
    """无历史仓位组的分仓：档号 UTF-8 字节 SHA-256 首字节奇偶。"""
    return hashlib.sha256(doc_id.encode("utf-8")).digest()[0] & 1


def _new_stats(mode: str = "incremental") -> dict:
    return {
        "mode": mode,
        "upstream_releases_read": set(),
        "upstream_docs_read": set(),
        "upstream_fragment_rows": 0,
        "ledger_digest_rows_scanned": 0,
        "ledger_evidence_rows_scanned": 0,
        "components_touched": [],
        "components_recomputed": [],
        "members_recomputed": 0,
        "elapsed_ms": 0,
    }


def _stats_out(stats: dict) -> dict:
    out = dict(stats)
    out["upstream_releases_read"] = sorted(stats["upstream_releases_read"])
    out["upstream_docs_read"] = sorted(stats["upstream_docs_read"])
    return out


def init_ledger(ledger: str, workspace: str, *, layer: str,
                min_segment_bytes: int, exempt_digests=()) -> dict:
    """建账：固定标注层、片段最短字节数与片段摘要豁免表。"""
    if not isinstance(layer, str) or not layer:
        raise LedgerError("BAD_CONFIG", "layer 必须是非空字符串")
    if not isinstance(min_segment_bytes, int) or min_segment_bytes < 0:
        raise LedgerError("BAD_CONFIG", "min_segment_bytes 必须是非负整数")
    exempt = set()
    for d in exempt_digests:
        if not isinstance(d, str) or len(d) != 64 or \
                any(c not in "0123456789abcdef" for c in d):
            raise LedgerError("BAD_CONFIG", f"豁免摘要须为 64 位小写十六进制: {d!r}")
        exempt.add(d)
    upstream = Upstream(workspace)  # 验证上游存在且可读
    try:
        missing = [
            t for t in _NEEDED_UPSTREAM_TABLES
            if upstream.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (t,)).fetchone() is None
        ]
        if missing:
            raise LedgerError(
                "UPSTREAM_SCHEMA_MISMATCH",
                f"上游缺少台账所需的数据表: {missing}")
    finally:
        upstream.close()
    if os.path.isfile(db_path(ledger)):
        raise LedgerError("LEDGER_EXISTS", f"台账已存在: {db_path(ledger)}")
    conn = connect(ledger, create=True)
    try:
        with transaction(conn):
            meta_set(conn, "schema", "1")
            meta_set(conn, "ledger_version", "0")
            meta_set(conn, "layer", layer)
            meta_set(conn, "min_segment_bytes", str(min_segment_bytes))
            meta_set(conn, "upstream_workspace", os.path.abspath(workspace))
            meta_set(conn, "created_at", utc_now())
            for d in sorted(exempt):
                conn.execute("INSERT INTO lg_exempt(digest) VALUES (?)", (d,))
    finally:
        conn.close()
    return {
        "ledger": os.path.abspath(ledger),
        "upstream_workspace": os.path.abspath(workspace),
        "layer": layer,
        "min_segment_bytes": min_segment_bytes,
        "exempt_digests": sorted(exempt),
        "ledger_version": 0,
    }


class Ledger:
    """台账句柄。读者只能看到已落定的台账（WAL + 单事务写入）。"""

    def __init__(self, ledger: str, workspace: Optional[str] = None):
        self.dir = os.path.abspath(ledger)
        self.conn = connect(ledger)
        self.layer = meta_get(self.conn, "layer")
        self.min_bytes = int(meta_get(self.conn, "min_segment_bytes") or "0")
        ws = workspace or meta_get(self.conn, "upstream_workspace")
        self.upstream = Upstream(ws)
        self.exempt = {
            r["digest"] for r in self.conn.execute("SELECT digest FROM lg_exempt")
        }
        self.version = ledger_version(self.conn)
        self._ucache_rel = {}
        self._ucache_doc = {}

    def close(self) -> None:
        self.conn.close()
        self.upstream.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # 请求入口：幂等 + 版本前提 + 单事务
    # ------------------------------------------------------------------

    def apply_change(self, request: dict) -> dict:
        """提交一个变更批（选入/改选/撤出/人工证据变动）。"""
        return self._execute("change", request)

    def confirm(self, request: dict) -> dict:
        """确认某一版清单中的一部分已对外交付。"""
        return self._execute("confirm", request)

    def _execute(self, kind: str, request: dict) -> dict:
        if not isinstance(request, dict):
            raise LedgerError("BAD_REQUEST", "请求必须是 JSON 对象")
        rid = request.get("request_id")
        if not isinstance(rid, str) or not rid:
            raise LedgerError("BAD_REQUEST", "request_id 必须是非空字符串")
        base = request.get("base_version")
        if not isinstance(base, int) or isinstance(base, bool):
            raise LedgerError("BAD_REQUEST", "base_version 必须是整数")
        request_hash = sha256_hex(canonical_json(request).encode("utf-8"))
        started = time.monotonic()
        with exclusive(self.dir):
            row = self.conn.execute(
                "SELECT request_hash, receipt_json FROM lg_request"
                " WHERE request_id=?", (rid,)).fetchone()
            if row is not None:
                if row["request_hash"] == request_hash:
                    # 同一请求重放：即使版本已前进，也取回原回执
                    return json.loads(row["receipt_json"])
                raise LedgerError(
                    "REQUEST_ID_CONFLICT",
                    f"request_id {rid!r} 已被不同内容的请求使用")
            self.version = ledger_version(self.conn)  # 锁内刷新，防并发写
            if base != self.version:
                raise LedgerError(
                    "STALE_BASE_VERSION",
                    f"请求基于台账版本 v{base}，当前版本 v{self.version}；"
                    f"旧版不能二次确认成新交付，请按当前版本重新提交",
                    detail={"base_version": base,
                            "current_version": self.version})
            stats = _new_stats()
            new_version = self.version + 1
            self._ucache_rel = {}
            self._ucache_doc = {}
            with transaction(self.conn):
                if kind == "change":
                    core = self._apply_change(request, new_version, stats)
                else:
                    core = self._confirm(request, new_version, stats)
                meta_set(self.conn, "ledger_version", str(new_version))
                stats["elapsed_ms"] = round(
                    (time.monotonic() - started) * 1000, 3)
                receipt = {
                    "ok": True,
                    "kind": kind,
                    "request_id": rid,
                    "version": new_version,
                    **core,
                    "diagnostics": _stats_out(stats),
                }
                self.conn.execute(
                    "INSERT INTO lg_request(request_id, request_hash, kind,"
                    " ledger_version, receipt_json) VALUES (?,?,?,?,?)",
                    (rid, request_hash, kind, new_version,
                     json.dumps(receipt, ensure_ascii=False, sort_keys=True)))
                self.conn.execute(
                    "INSERT INTO lg_op(version, kind, request_id, stats_json)"
                    " VALUES (?,?,?,?)",
                    (new_version, kind, rid,
                     json.dumps(_stats_out(stats), ensure_ascii=False,
                                sort_keys=True)))
            self.version = new_version
            return receipt

    # ------------------------------------------------------------------
    # 变更批
    # ------------------------------------------------------------------

    def _apply_change(self, req: dict, new_version: int, stats: dict) -> dict:
        selects = req.get("select") or []
        withdraws = req.get("withdraw") or []
        ev_reg = req.get("evidence_register") or []
        ev_rev = req.get("evidence_revoke") or []
        if not (selects or withdraws or ev_reg or ev_rev):
            raise LedgerError("EMPTY_BATCH", "变更批不含任何操作")
        errors = self._validate_change(selects, withdraws, ev_reg, ev_rev, stats)
        if errors:
            raise LedgerError(
                "BATCH_REJECTED",
                f"变更批含 {len(errors)} 处非法，整批不入账",
                errors=errors)

        conn = self.conn
        changed_docs = set()
        digests_to_expand = set()
        evidence_endpoint_docs = set()

        # 选入 / 改选：读取该发布下的审定片段与公文身份
        for s in selects:
            doc_id, release_id = s["doc_id"], s["release_id"]
            for r in conn.execute(
                    "SELECT digest FROM lg_digest WHERE doc_id=?"
                    " AND source='inuse'", (doc_id,)):
                digests_to_expand.add(r["digest"])
            info = self._release_info(release_id, stats)
            identity = self._doc_identity(doc_id, stats)
            frags = self.upstream.release_fragments(
                release_id, doc_id, self.layer)
            stats["upstream_fragment_rows"] += len(frags)
            conn.execute(
                "DELETE FROM lg_digest WHERE doc_id=? AND source='inuse'",
                (doc_id,))
            conn.execute(
                "INSERT INTO lg_selection(doc_id, release_id, rev_id, rev_seq,"
                " status, whole_digest, updated_version) VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(doc_id) DO UPDATE SET release_id=excluded.release_id,"
                " rev_id=excluded.rev_id, rev_seq=excluded.rev_seq,"
                " status='selected', whole_digest=excluded.whole_digest,"
                " updated_version=excluded.updated_version",
                (doc_id, release_id, info["rev_id"], info["rev_seq"],
                 "selected", identity["sha256"], new_version))
            seg = {}
            for f in frags:
                data = f["seen_text"].encode("utf-8")
                if len(data) < self.min_bytes:
                    continue  # 未达到约定最短字节数
                digest = sha256_hex(data)
                if digest != f["text_sha256"]:
                    raise LedgerError(
                        "UPSTREAM_DIGEST_MISMATCH",
                        f"发布 {release_id} 中 {doc_id} 的片段"
                        f" {f['fragment_uid']} 文本与登记摘要不符")
                if digest in self.exempt:
                    continue  # 豁免表内的套话（整篇底稿摘要不在此列）
                seg.setdefault(digest, []).append(f["fragment_uid"])
            conn.execute(
                "INSERT INTO lg_digest(digest, doc_id, source, kind,"
                " release_id, fragment_uids_json, delivery_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (identity["sha256"], doc_id, "inuse", "whole",
                 release_id, None, ""))
            for digest, uids in sorted(seg.items()):
                conn.execute(
                    "INSERT INTO lg_digest(digest, doc_id, source, kind,"
                    " release_id, fragment_uids_json, delivery_id)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (digest, doc_id, "inuse", "segment", release_id,
                     canonical_json(sorted(uids)), ""))
            digests_to_expand.add(identity["sha256"])
            digests_to_expand.update(seg)
            changed_docs.add(doc_id)

        for doc_id in withdraws:
            conn.execute(
                "UPDATE lg_selection SET status='withdrawn',"
                " updated_version=? WHERE doc_id=?",
                (new_version, doc_id))
            changed_docs.add(doc_id)

        for e in ev_reg:
            conn.execute(
                "INSERT INTO lg_evidence(evidence_id, doc_a, doc_b,"
                " provenance, status, created_version, revoked_version)"
                " VALUES (?,?,?,?,'active',?,NULL)",
                (e["evidence_id"], e["doc_a"], e["doc_b"], e["provenance"],
                 new_version))
            evidence_endpoint_docs.add(e["doc_a"])
            evidence_endpoint_docs.add(e["doc_b"])

        for evidence_id in ev_rev:
            row = conn.execute(
                "SELECT doc_a, doc_b FROM lg_evidence WHERE evidence_id=?",
                (evidence_id,)).fetchone()
            conn.execute(
                "UPDATE lg_evidence SET status='revoked', revoked_version=?"
                " WHERE evidence_id=?", (new_version, evidence_id))
            evidence_endpoint_docs.add(row["doc_a"])
            evidence_endpoint_docs.add(row["doc_b"])

        # 受影响的连通分量：变更档号所在分量 ∪ 共用旧/新摘要的档号所在分量
        # ∪ 人工证据邻接档号所在分量（均为点查，不遍历不相干分量）
        dirty_components = set()
        new_docs = set()

        def mark(doc_id: str) -> None:
            row = conn.execute(
                "SELECT component_id FROM lg_component WHERE doc_id=?",
                (doc_id,)).fetchone()
            if row is not None:
                dirty_components.add(row["component_id"])
            elif conn.execute(
                    "SELECT 1 FROM lg_selection WHERE doc_id=?",
                    (doc_id,)).fetchone() is not None:
                new_docs.add(doc_id)

        for d in changed_docs:
            mark(d)
        for digest in digests_to_expand:
            for r in conn.execute(
                    "SELECT DISTINCT doc_id FROM lg_digest WHERE digest=?",
                    (digest,)):
                mark(r["doc_id"])
        for d in changed_docs | evidence_endpoint_docs:
            for r in conn.execute(
                    "SELECT doc_a, doc_b FROM lg_evidence"
                    " WHERE doc_a=? OR doc_b=?", (d, d)):
                mark(r["doc_a"])
                mark(r["doc_b"])

        stats["components_touched"] = sorted(dirty_components)
        results = _recompute(conn, dirty_components, new_docs,
                             new_version, stats)
        return {"components": results}

    def _validate_change(self, selects, withdraws, ev_reg, ev_rev,
                         stats) -> list:
        errors = []

        def err(code, message, **kw):
            errors.append({"code": code, "message": message, **kw})

        seen_select = {}
        for i, s in enumerate(selects):
            if not isinstance(s, dict):
                err("BAD_SELECT", f"select[{i}] 必须是对象")
                continue
            doc_id, release_id = s.get("doc_id"), s.get("release_id")
            if not isinstance(doc_id, str) or not doc_id:
                err("BAD_SELECT", f"select[{i}].doc_id 必须是非空字符串")
                continue
            if not isinstance(release_id, str) or not release_id:
                err("BAD_SELECT", f"select[{i}].release_id 必须是非空字符串",
                    doc_id=doc_id)
                continue
            if doc_id in seen_select:
                err("DUPLICATE_OP", f"同一批中重复选入 {doc_id}",
                    doc_id=doc_id)
                continue
            seen_select[doc_id] = release_id

        withdraw_set = set()
        for w in withdraws:
            if not isinstance(w, str) or not w:
                err("BAD_WITHDRAW", f"撤出项必须是非空档号字符串: {w!r}")
                continue
            if w in seen_select:
                err("CONFLICTING_OP", f"同一批中既选入又撤出 {w}", doc_id=w)
            if w in withdraw_set:
                err("DUPLICATE_OP", f"同一批中重复撤出 {w}", doc_id=w)
            withdraw_set.add(w)

        reg_ids = set()
        for i, e in enumerate(ev_reg):
            if not isinstance(e, dict):
                err("BAD_EVIDENCE", f"evidence_register[{i}] 必须是对象")
                continue
            eid = e.get("evidence_id")
            a, b = e.get("doc_a"), e.get("doc_b")
            prov = e.get("provenance")
            ok = True
            for name, val in (("evidence_id", eid), ("doc_a", a),
                              ("doc_b", b), ("provenance", prov)):
                if not isinstance(val, str) or not val:
                    err("BAD_EVIDENCE",
                        f"evidence_register[{i}].{name} 必须是非空字符串")
                    ok = False
            if not ok:
                continue
            if a == b:
                err("EVIDENCE_SELF", "人工同源证据的两个档号不能相同",
                    evidence_id=eid)
            if eid in reg_ids:
                err("DUPLICATE_OP", f"同一批中重复登记证据 {eid}",
                    evidence_id=eid)
            reg_ids.add(eid)

        rev_set = set()
        for eid in ev_rev:
            if not isinstance(eid, str) or not eid:
                err("BAD_EVIDENCE", f"撤销项必须是非空证据 ID: {eid!r}")
                continue
            if eid in rev_set:
                err("DUPLICATE_OP", f"同一批中重复撤销证据 {eid}",
                    evidence_id=eid)
            rev_set.add(eid)
        for eid in reg_ids & rev_set:
            err("CONFLICTING_OP", f"同一批中既登记又撤销证据 {eid}",
                evidence_id=eid)
        if errors:
            return errors

        # 上游与台账状态校验（点查）
        for doc_id, release_id in seen_select.items():
            info = self._release_info(release_id, stats, missing_ok=True)
            if info is None:
                err("UNKNOWN_RELEASE", f"未知发布: {release_id}",
                    doc_id=doc_id, release_id=release_id)
                continue
            if doc_id not in info["doc_ids"]:
                err("DOC_NOT_IN_RELEASE",
                    f"档号 {doc_id} 不属于发布 {release_id}",
                    doc_id=doc_id, release_id=release_id)
                continue
            if self._doc_identity(doc_id, stats, missing_ok=True) is None:
                err("UNKNOWN_DOC", f"上游未登记档号: {doc_id}", doc_id=doc_id)

        for w in withdraw_set:
            row = self.conn.execute(
                "SELECT status FROM lg_selection WHERE doc_id=?",
                (w,)).fetchone()
            if row is None:
                err("NOT_SELECTED", f"档号未选入，不能撤出: {w}", doc_id=w)
            elif row["status"] == "withdrawn":
                err("ALREADY_WITHDRAWN", f"档号已撤出: {w}", doc_id=w)

        for e in ev_reg:
            eid, a, b = e["evidence_id"], e["doc_a"], e["doc_b"]
            if self.conn.execute(
                    "SELECT 1 FROM lg_evidence WHERE evidence_id=?",
                    (eid,)).fetchone() is not None:
                err("DUPLICATE_EVIDENCE_ID",
                    f"证据 ID 已使用（含已撤销）: {eid}", evidence_id=eid)
            for d in (a, b):
                if self._doc_identity(d, stats, missing_ok=True) is None:
                    err("UNKNOWN_DOC", f"证据端点不是已知档号: {d}",
                        evidence_id=eid, doc_id=d)

        for eid in rev_set:
            row = self.conn.execute(
                "SELECT status FROM lg_evidence WHERE evidence_id=?",
                (eid,)).fetchone()
            if row is None:
                err("UNKNOWN_EVIDENCE", f"未知证据: {eid}", evidence_id=eid)
            elif row["status"] != "active":
                err("EVIDENCE_NOT_ACTIVE", f"证据已撤销: {eid}",
                    evidence_id=eid)
        return errors

    # ------------------------------------------------------------------
    # 交付确认
    # ------------------------------------------------------------------

    def _confirm(self, req: dict, new_version: int, stats: dict) -> dict:
        list_version = req.get("list_version")
        doc_ids = req.get("doc_ids")
        if not isinstance(list_version, int) or isinstance(list_version, bool):
            raise LedgerError("BAD_REQUEST", "list_version 必须是整数")
        if not isinstance(doc_ids, list) or not doc_ids:
            raise LedgerError("BAD_REQUEST", "doc_ids 必须是非空数组")
        if list_version != self.version:
            raise LedgerError(
                "STALE_LIST_VERSION",
                f"只能确认当前清单版本 v{self.version} 的交付；"
                f"旧版（v{list_version}）不能二次确认成新交付",
                detail={"list_version": list_version,
                        "current_version": self.version})
        errors = []
        seen = set()
        rows = {}
        for d in doc_ids:
            if not isinstance(d, str) or not d:
                errors.append({"code": "BAD_DOC", "message": f"非法档号: {d!r}"})
                continue
            if d in seen:
                errors.append({"code": "DUPLICATE_OP",
                               "message": f"同一批中重复确认 {d}", "doc_id": d})
                continue
            seen.add(d)
            row = self.conn.execute(
                "SELECT * FROM lg_list WHERE doc_id=?", (d,)).fetchone()
            rows[d] = row
            if row is None:
                errors.append({"code": "NOT_LISTED",
                               "message": f"档号不在当前清单（未选入或已撤出）: {d}",
                               "doc_id": d})
            elif row["position"] == QUARANTINE:
                errors.append({"code": "QUARANTINED",
                               "message": f"档号处于隔离组，不能交付: {d}",
                               "doc_id": d})
        if errors:
            raise LedgerError("CONFIRM_REJECTED",
                              f"交付确认含 {len(errors)} 处非法，整批不入账",
                              errors=errors)

        deliveries = []
        dirty = set()
        for d in doc_ids:
            row = rows[d]
            pos = row["position"]
            lock = self.conn.execute(
                "SELECT position FROM lg_lock WHERE doc_id=?",
                (d,)).fetchone()
            if lock is not None and lock["position"] != pos:
                # 防御：已锁定仓位不可改写（正常流程到不了这里）
                raise LedgerError(
                    "LOCK_CONFLICT",
                    f"档号 {d} 已锁定 {lock['position']}，不能改写为 {pos}",
                    detail={"doc_id": d})
            delivery_id = stable_id("dlv", req["request_id"], d)
            dig_rows = self.conn.execute(
                "SELECT digest, kind, release_id, fragment_uids_json"
                " FROM lg_digest WHERE doc_id=? AND source='inuse'"
                " ORDER BY kind, digest", (d,)).fetchall()
            whole = None
            segments = []
            for r in dig_rows:
                # 历史摘要随交付凭据留存；改选/撤出都不覆盖
                self.conn.execute(
                    "INSERT INTO lg_digest(digest, doc_id, source, kind,"
                    " release_id, fragment_uids_json, delivery_id)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (r["digest"], d, "delivered", r["kind"], r["release_id"],
                     r["fragment_uids_json"], delivery_id))
                if r["kind"] == "whole":
                    whole = r["digest"]
                else:
                    segments.append({
                        "digest": r["digest"],
                        "release_id": r["release_id"],
                        "fragment_uids": json.loads(r["fragment_uids_json"]),
                    })
            self.conn.execute(
                "INSERT INTO lg_delivery(delivery_id, doc_id, position,"
                " list_version, request_id, created_version)"
                " VALUES (?,?,?,?,?,?)",
                (delivery_id, d, pos, list_version, req["request_id"],
                 new_version))
            if lock is None:
                self.conn.execute(
                    "INSERT INTO lg_lock(doc_id, position, first_delivery_id)"
                    " VALUES (?,?,?)", (d, pos, delivery_id))
            deliveries.append({
                "doc_id": d,
                "delivery_id": delivery_id,
                "position": pos,
                "list_version": list_version,
                "whole_digest": whole,
                "segment_digests": segments,
            })
            comp = self.conn.execute(
                "SELECT component_id FROM lg_component WHERE doc_id=?",
                (d,)).fetchone()
            if comp is not None:
                dirty.add(comp["component_id"])
        stats["components_touched"] = sorted(dirty)
        results = _recompute(self.conn, dirty, set(), new_version, stats)
        return {"list_version": list_version, "deliveries": deliveries,
                "components": results}

    # ------------------------------------------------------------------
    # 上游点查（带诊断计数）
    # ------------------------------------------------------------------

    def _release_info(self, release_id, stats, missing_ok=False):
        if release_id not in self._ucache_rel:
            self._ucache_rel[release_id] = self.upstream.release_info(release_id)
            stats["upstream_releases_read"].add(release_id)
        info = self._ucache_rel[release_id]
        if info is None and not missing_ok:
            raise LedgerError("UNKNOWN_RELEASE", f"未知发布: {release_id}")
        return info

    def _doc_identity(self, doc_id, stats, missing_ok=False):
        if doc_id not in self._ucache_doc:
            self._ucache_doc[doc_id] = self.upstream.doc_identity(doc_id)
            stats["upstream_docs_read"].add(doc_id)
        identity = self._ucache_doc[doc_id]
        if identity is None and not missing_ok:
            raise LedgerError("UNKNOWN_DOC", f"上游未登记档号: {doc_id}")
        return identity

    # ------------------------------------------------------------------
    # 清单与查询
    # ------------------------------------------------------------------

    def lists(self) -> dict:
        """当前三张清单：逐篇给出档号、发布 ID 与发布中的审定版本。"""
        out = {"version": self.version, "train": [], "eval": [],
               "quarantine": []}
        for r in self.conn.execute(
                "SELECT doc_id, release_id, rev_id, rev_seq, component_id,"
                " position FROM lg_list ORDER BY doc_id"):
            out[r["position"]].append({
                "doc_id": r["doc_id"],
                "release_id": r["release_id"],
                "rev_id": r["rev_id"],
                "rev_seq": r["rev_seq"],
                "component_id": r["component_id"],
            })
        return out

    def explain_doc(self, doc_id: str) -> dict:
        """公文为什么分在一起、为什么不能交付。"""
        sel = self.conn.execute(
            "SELECT * FROM lg_selection WHERE doc_id=?",
            (doc_id,)).fetchone()
        comp = self.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_id,)).fetchone()
        lst = self.conn.execute(
            "SELECT * FROM lg_list WHERE doc_id=?", (doc_id,)).fetchone()
        out = {
            "doc_id": doc_id,
            "in_ledger": sel is not None,
            "selection": None if sel is None else {
                "release_id": sel["release_id"],
                "rev_id": sel["rev_id"],
                "rev_seq": sel["rev_seq"],
                "status": sel["status"],
                "whole_digest": sel["whole_digest"],
            },
            "component_id": None if comp is None else comp["component_id"],
            "listed": None if lst is None else {
                "position": lst["position"],
                "release_id": lst["release_id"],
                "rev_id": lst["rev_id"],
                "rev_seq": lst["rev_seq"],
            },
        }
        if comp is not None:
            members = []
            for r in self.conn.execute(
                    "SELECT s.doc_id, s.status,"
                    "       (SELECT position FROM lg_lock l"
                    "         WHERE l.doc_id = s.doc_id) AS lock_pos,"
                    "       (SELECT position FROM lg_list t"
                    "         WHERE t.doc_id = s.doc_id) AS list_pos"
                    " FROM lg_selection s"
                    " WHERE s.doc_id IN"
                    "       (SELECT doc_id FROM lg_component"
                    "         WHERE component_id=?) ORDER BY s.doc_id",
                    (comp["component_id"],)):
                members.append({
                    "doc_id": r["doc_id"],
                    "status": r["status"],
                    "locked_position": r["lock_pos"],
                    "listed_position": r["list_pos"],
                })
            out["component_members"] = members
        # 为什么不能交付
        reason = None
        if sel is None:
            reason = "not_selected"
        elif lst is None:
            reason = "withdrawn"
        elif lst["position"] == QUARANTINE:
            reason = "quarantine"
            out["quarantine"] = self.quarantine_reason(doc_id)
        out["deliverable"] = reason is None
        out["not_deliverable_reason"] = reason
        out["deliveries"] = [
            {"delivery_id": r["delivery_id"], "position": r["position"],
             "list_version": r["list_version"], "request_id": r["request_id"]}
            for r in self.conn.execute(
                "SELECT * FROM lg_delivery WHERE doc_id=?"
                " ORDER BY created_version, delivery_id", (doc_id,))
        ]
        return out

    def quarantine_reason(self, doc_id: str) -> Optional[dict]:
        """隔离原因：两个不同仓位的历史确认 + 连接查询成员的证据路径。"""
        comp = self.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_id,)).fetchone()
        if comp is None:
            return None
        cid = comp["component_id"]
        locks = []
        for r in self.conn.execute(
                "SELECT k.doc_id, k.position, k.first_delivery_id,"
                "       d.list_version, d.request_id"
                " FROM lg_lock k JOIN lg_delivery d"
                "   ON d.delivery_id = k.first_delivery_id"
                " WHERE k.doc_id IN"
                "       (SELECT doc_id FROM lg_component WHERE component_id=?)"
                " ORDER BY k.doc_id", (cid,)):
            locks.append({
                "doc_id": r["doc_id"],
                "position": r["position"],
                "delivery_id": r["first_delivery_id"],
                "list_version": r["list_version"],
                "request_id": r["request_id"],
            })
        positions = {l["position"] for l in locks}
        if len(positions) < 2:
            return {"component_id": cid, "quarantined": False, "locks": locks}
        targets = {}
        for l in locks:  # 每个仓位取字节序最小档号作为代表
            cur = targets.get(l["position"])
            if cur is None or l["doc_id"].encode("utf-8") < cur.encode("utf-8"):
                targets[l["position"]] = l["doc_id"]
        paths = {}
        for pos, target in sorted(targets.items()):
            paths[pos] = {
                "locked_doc": target,
                "path": self.path_between(doc_id, target),
            }
        return {
            "component_id": cid,
            "quarantined": True,
            "locks": locks,
            "paths_from_member": paths,
        }

    def path_between(self, doc_a: str, doc_b: str) -> Optional[list]:
        """两篇公文之间的证据路径（不要求最短）。

        摘要证据定位到具体档号与发布中的记录（历史摘要注明交付凭据）；
        人工边给出仍有效的证据 ID 与出处。
        """
        if doc_a == doc_b:
            return []
        ca = self.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_a,)).fetchone()
        cb = self.conn.execute(
            "SELECT component_id FROM lg_component WHERE doc_id=?",
            (doc_b,)).fetchone()
        if ca is None or cb is None or ca["component_id"] != cb["component_id"]:
            return None
        start, goal = ("doc", doc_a), ("doc", doc_b)
        prev = {start: None}
        queue = deque([start])
        delivery_cache = {}
        while queue:
            node = queue.popleft()
            if node == goal:
                break
            for nxt, edge in self._neighbors(node, delivery_cache):
                if nxt not in prev:
                    prev[nxt] = (node, edge)
                    queue.append(nxt)
        if goal not in prev:
            return None
        nodes = [goal]
        while prev[nodes[-1]] is not None:
            nodes.append(prev[nodes[-1]][0])
        nodes.reverse()
        # 合并为「公文 -证据-> 公文」的可读步骤
        steps = []
        i = 0
        while i < len(nodes) - 1:
            edge = prev[nodes[i + 1]][1]
            if nodes[i + 1][0] == "dig":
                edge2 = prev[nodes[i + 2]][1]
                steps.append({
                    "from_doc": nodes[i][1],
                    "to_doc": nodes[i + 2][1],
                    "via": {
                        "type": edge["type"],
                        "digest": edge["digest"],
                        "sides": [edge["held_by"], edge2["held_by"]],
                    },
                })
                i += 2
            else:
                steps.append({
                    "from_doc": nodes[i][1],
                    "to_doc": nodes[i + 1][1],
                    "via": {
                        "type": "manual_evidence",
                        "evidence_id": edge["evidence_id"],
                        "provenance": edge["provenance"],
                    },
                })
                i += 1
        return steps

    def _neighbors(self, node, delivery_cache):
        kind, value = node
        if kind == "doc":
            for r in self.conn.execute(
                    "SELECT doc_id, digest, kind, source, release_id,"
                    " fragment_uids_json, delivery_id FROM lg_digest"
                    " WHERE doc_id=?", (value,)):
                yield ("dig", r["digest"]), {
                    "type": "whole_digest" if r["kind"] == "whole"
                            else "segment_digest",
                    "digest": r["digest"],
                    "from_doc": value,
                    "held_by": self._locator(r, delivery_cache),
                }
            for r in self.conn.execute(
                    "SELECT evidence_id, doc_a, doc_b, provenance"
                    " FROM lg_evidence WHERE status='active'"
                    " AND (doc_a=? OR doc_b=?)", (value, value)):
                other = r["doc_b"] if r["doc_a"] == value else r["doc_a"]
                if not (self._qualifies(value) and self._qualifies(other)):
                    continue  # 休眠边：端点不都在用或曾交付
                yield ("doc", other), {
                    "type": "manual_evidence",
                    "evidence_id": r["evidence_id"],
                    "provenance": r["provenance"],
                    "from_doc": value,
                    "to_doc": other,
                }
        else:
            for r in self.conn.execute(
                    "SELECT doc_id, kind, source, release_id,"
                    " fragment_uids_json, delivery_id FROM lg_digest"
                    " WHERE digest=? ORDER BY source DESC", (value,)):
                yield ("doc", r["doc_id"]), {
                    "type": "whole_digest" if r["kind"] == "whole"
                            else "segment_digest",
                    "digest": value,
                    "to_doc": r["doc_id"],
                    "held_by": self._locator(r, delivery_cache),
                }

    def _locator(self, row, delivery_cache):
        loc = {
            "doc_id": row["doc_id"],
            "source": row["source"],
            "release_id": row["release_id"],
        }
        if row["fragment_uids_json"] is not None:
            loc["fragment_uids"] = json.loads(row["fragment_uids_json"])
        if row["source"] == "delivered":
            did = row["delivery_id"]
            if did not in delivery_cache:
                d = self.conn.execute(
                    "SELECT delivery_id, position, list_version, request_id"
                    " FROM lg_delivery WHERE delivery_id=?", (did,)).fetchone()
                delivery_cache[did] = None if d is None else {
                    "delivery_id": d["delivery_id"],
                    "position": d["position"],
                    "list_version": d["list_version"],
                    "request_id": d["request_id"],
                }
            loc["delivery"] = delivery_cache[did]
        return loc

    def _qualifies(self, doc_id: str) -> bool:
        """证据端点资格：在用（selected）或曾交付。"""
        row = self.conn.execute(
            "SELECT status FROM lg_selection WHERE doc_id=?",
            (doc_id,)).fetchone()
        if row is not None and row["status"] == "selected":
            return True
        return self.conn.execute(
            "SELECT 1 FROM lg_delivery WHERE doc_id=? LIMIT 1",
            (doc_id,)).fetchone() is not None

    def credentials(self, doc_id: str) -> list:
        """取回历史交付凭据（含当时实际交付的整篇与合格片段摘要）。"""
        out = []
        for d in self.conn.execute(
                "SELECT * FROM lg_delivery WHERE doc_id=?"
                " ORDER BY created_version, delivery_id", (doc_id,)):
            digests = []
            for r in self.conn.execute(
                    "SELECT digest, kind, release_id, fragment_uids_json"
                    " FROM lg_digest WHERE delivery_id=?"
                    " ORDER BY kind, digest", (d["delivery_id"],)):
                digests.append({
                    "digest": r["digest"],
                    "kind": r["kind"],
                    "release_id": r["release_id"],
                    "fragment_uids": None if r["fragment_uids_json"] is None
                                     else json.loads(r["fragment_uids_json"]),
                })
            out.append({
                "delivery_id": d["delivery_id"],
                "doc_id": d["doc_id"],
                "position": d["position"],
                "list_version": d["list_version"],
                "request_id": d["request_id"],
                "created_version": d["created_version"],
                "digests": digests,
            })
        return out

    def status(self) -> dict:
        conn = self.conn

        def count(sql, args=()):
            return conn.execute(sql, args).fetchone()["c"]

        return {
            "ledger_version": self.version,
            "layer": self.layer,
            "min_segment_bytes": self.min_bytes,
            "upstream_workspace": meta_get(conn, "upstream_workspace"),
            "exempt_digests": count("SELECT COUNT(*) c FROM lg_exempt"),
            "docs_selected": count(
                "SELECT COUNT(*) c FROM lg_selection WHERE status='selected'"),
            "docs_withdrawn": count(
                "SELECT COUNT(*) c FROM lg_selection WHERE status='withdrawn'"),
            "components": count(
                "SELECT COUNT(DISTINCT component_id) c FROM lg_component"),
            "listed_train": count(
                "SELECT COUNT(*) c FROM lg_list WHERE position='train'"),
            "listed_eval": count(
                "SELECT COUNT(*) c FROM lg_list WHERE position='eval'"),
            "listed_quarantine": count(
                "SELECT COUNT(*) c FROM lg_list WHERE position='quarantine'"),
            "locks": count("SELECT COUNT(*) c FROM lg_lock"),
            "deliveries": count("SELECT COUNT(*) c FROM lg_delivery"),
            "evidence_active": count(
                "SELECT COUNT(*) c FROM lg_evidence WHERE status='active'"),
            "evidence_revoked": count(
                "SELECT COUNT(*) c FROM lg_evidence WHERE status='revoked'"),
            "recent_ops": [
                json.loads(r["stats_json"]) | {
                    "version": r["version"], "kind": r["kind"],
                    "request_id": r["request_id"]}
                for r in conn.execute(
                    "SELECT * FROM lg_op ORDER BY seq DESC LIMIT 5")
            ],
        }

    # ------------------------------------------------------------------
    # 全量重算（对照 / 修复）
    # ------------------------------------------------------------------

    def rebuild_all(self) -> dict:
        """全库重建所有连通分量与清单（管理用途；诊断可区分局部与全量）。"""
        started = time.monotonic()
        with exclusive(self.dir):
            stats = _new_stats(mode="full")
            with transaction(self.conn):
                all_docs = [r["doc_id"] for r in self.conn.execute(
                    "SELECT doc_id FROM lg_selection")]
                results = _recompute(self.conn, set(), set(all_docs),
                                     self.version, stats,
                                     record_history=False)
                stats["elapsed_ms"] = round(
                    (time.monotonic() - started) * 1000, 3)
                self.conn.execute(
                    "INSERT INTO lg_op(version, kind, request_id, stats_json)"
                    " VALUES (?,?,?,?)",
                    (self.version, "rebuild", None,
                     json.dumps(_stats_out(stats), ensure_ascii=False,
                                sort_keys=True)))
            return {"version": self.version, "components": results,
                    "diagnostics": _stats_out(stats)}

    def verify(self) -> dict:
        """对照检查：复制当前台账，全量重算后与增量维护的状态逐篇比对。"""
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        tmpdir = tempfile.mkdtemp(prefix="ledger-verify-")
        try:
            shutil.copy(db_path(self.dir), os.path.join(tmpdir, "ledger.db"))
            other = Ledger(tmpdir, self.upstream.workspace)
            try:
                stats = _new_stats(mode="full")
                all_docs = [r["doc_id"] for r in other.conn.execute(
                    "SELECT doc_id FROM lg_selection")]
                _recompute(other.conn, set(), set(all_docs), self.version,
                           stats, record_history=False)
                diffs = []
                for table, keys in (("lg_component", ("component_id",)),
                                    ("lg_list", ("component_id", "position",
                                                 "release_id", "rev_id"))):
                    mine = {
                        r["doc_id"]: tuple(r[k] for k in keys)
                        for r in self.conn.execute(
                            f"SELECT * FROM {table}")
                    }
                    theirs = {
                        r["doc_id"]: tuple(r[k] for k in keys)
                        for r in other.conn.execute(
                            f"SELECT * FROM {table}")
                    }
                    for doc_id in sorted(set(mine) | set(theirs)):
                        if mine.get(doc_id) != theirs.get(doc_id):
                            diffs.append({
                                "table": table, "doc_id": doc_id,
                                "incremental": mine.get(doc_id),
                                "full_rebuild": theirs.get(doc_id),
                            })
                return {
                    "consistent": not diffs,
                    "differences": diffs,
                    "full_rebuild_diagnostics": _stats_out(stats),
                }
            finally:
                other.close()
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ----------------------------------------------------------------------
# 连通分量局部重算
# ----------------------------------------------------------------------

def _recompute(conn, dirty_components, extra_docs, new_version, stats,
               record_history=True) -> list:
    """只重算受影响的连通分量。

    dirty_components：受影响的旧分量 ID；extra_docs：尚无分量记录的新档号。
    成员、摘要边、人工边全部按点查取出，不遍历不相干分量。
    """
    members = set(extra_docs)
    if dirty_components:
        ph = ",".join("?" * len(dirty_components))
        for r in conn.execute(
                f"SELECT doc_id FROM lg_component"
                f" WHERE component_id IN ({ph})",
                sorted(dirty_components)):
            members.add(r["doc_id"])
    if not members:
        return []
    stats["members_recomputed"] += len(members)
    mlist = sorted(members)
    ph = ",".join("?" * len(mlist))

    # 摘要边（在用 + 历史交付）：digest -> docs，不展开成两两记录
    dig2docs = {}
    for r in conn.execute(
            f"SELECT digest, doc_id FROM lg_digest WHERE doc_id IN ({ph})",
            mlist):
        dig2docs.setdefault(r["digest"], []).append(r["doc_id"])
        stats["ledger_digest_rows_scanned"] += 1

    parent = {d: d for d in mlist}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for docs in dig2docs.values():
        first = docs[0]
        for d in docs[1:]:
            union(first, d)

    # 人工同源边：仍有效，且两个端点都属于在用或曾交付档号
    qualified = set()
    for r in conn.execute(
            f"SELECT doc_id FROM lg_selection"
            f" WHERE status='selected' AND doc_id IN ({ph})", mlist):
        qualified.add(r["doc_id"])
    for r in conn.execute(
            f"SELECT DISTINCT doc_id FROM lg_delivery"
            f" WHERE doc_id IN ({ph})", mlist):
        qualified.add(r["doc_id"])
    for r in conn.execute(
            f"SELECT doc_a, doc_b FROM lg_evidence"
            f" WHERE status='active' AND (doc_a IN ({ph}) OR doc_b IN ({ph}))",
            mlist + mlist):
        stats["ledger_evidence_rows_scanned"] += 1
        a, b = r["doc_a"], r["doc_b"]
        if a in members and b in members \
                and a in qualified and b in qualified:
            union(a, b)

    locks = {}
    for r in conn.execute(
            f"SELECT doc_id, position FROM lg_lock WHERE doc_id IN ({ph})",
            mlist):
        locks[r["doc_id"]] = r["position"]
    sels = {}
    for r in conn.execute(
            f"SELECT * FROM lg_selection WHERE doc_id IN ({ph})", mlist):
        sels[r["doc_id"]] = r

    groups = {}
    for d in mlist:
        groups.setdefault(find(d), []).append(d)

    conn.execute(f"DELETE FROM lg_component WHERE doc_id IN ({ph})", mlist)
    conn.execute(f"DELETE FROM lg_list WHERE doc_id IN ({ph})", mlist)

    results = []
    for g in groups.values():
        cid = min(g, key=lambda s: s.encode("utf-8"))
        positions = {locks[d] for d in g if d in locks}
        if len(positions) >= 2:
            pos = QUARANTINE
        elif len(positions) == 1:
            pos = next(iter(positions))
        else:
            pos = TRAIN if _bin_of(cid) == 0 else EVAL
        listed = sorted(
            (d for d in g
             if sels.get(d) is not None and sels[d]["status"] == "selected"),
            key=lambda s: s.encode("utf-8"))
        listed_set = set(listed)
        for d in g:
            conn.execute(
                "INSERT INTO lg_component(doc_id, component_id) VALUES (?,?)",
                (d, cid))
        for d in listed:
            s = sels[d]
            conn.execute(
                "INSERT INTO lg_list(doc_id, component_id, position,"
                " release_id, rev_id, rev_seq, updated_version)"
                " VALUES (?,?,?,?,?,?,?)",
                (d, cid, pos, s["release_id"], s["rev_id"], s["rev_seq"],
                 new_version))
        if record_history:
            for d in g:
                conn.execute(
                    "INSERT INTO lg_list_history(version, doc_id,"
                    " component_id, position) VALUES (?,?,?,?)",
                    (new_version, d, cid,
                     pos if d in listed_set else None))
        results.append({
            "component_id": cid,
            "position": pos,
            "members_total": len(g),
            "listed": listed,
            "locked_positions": sorted(positions),
        })
    results.sort(key=lambda x: x["component_id"].encode("utf-8"))
    stats["components_recomputed"].extend(r["component_id"] for r in results)
    return results
