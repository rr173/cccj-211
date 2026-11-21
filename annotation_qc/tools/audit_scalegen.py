# -*- coding: utf-8 -*-
"""运行时生成大批文档与发布（产物写到临时目录，不提交）。

直接写最小上游库（只含盲审读取的表），用于核对：

- 总体很大时抽样不在 Python 中保留整个总体（分值入 SQLite 临时表，
  只取前 k 篇）；
- 导出用服务端游标逐行产出，内存不随文档数线性增长。

每篇文档带 1 条正式片段与少量有向引用；文档原文写入 ``sources/``，
供盲审按精确字节范围取文。

用法::

    python -I tools/audit_scalegen.py --root /tmp/audit-scale --docs 30000
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3


def generate(root: str, docs: int) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "sources"), exist_ok=True)
    workspace = os.path.join(root, "workspace")
    os.makedirs(workspace, exist_ok=True)
    db = os.path.join(workspace, "workspace.db")
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
        release_id = "release_scale"
        doc_ids = []
        frag_rows = []
        rel_rows = []
        for i in range(docs):
            doc_id = f"DOC{i:06d}"
            doc_ids.append(doc_id)
            text = f"BULK-{i:06d}-正文条款甲乙丙丁".encode("utf-8")
            rel_path = f"sources/{doc_id}.txt"
            with open(os.path.join(root, rel_path), "wb") as fh:
                fh.write(text)
            digest = hashlib.sha256(text).hexdigest()
            conn.execute(
                "INSERT INTO source_doc(doc_id, rel_path, sha256, byte_length,"
                " verified) VALUES (?,?,?,?,?)",
                (doc_id, rel_path, digest, len(text), 1))
            # 每个文档 1 条片段，起点 0、终点在 ASCII 前缀内，便于取文
            seen = text[:10].decode("ascii")
            frag_uid = f"frag-{doc_id}"
            frag_rows.append((release_id, frag_uid, doc_id, "clause", "L1",
                              0, 10,
                              hashlib.sha256(seen.encode()).hexdigest(),
                              seen, "auto", "auto", "rev_scale", "[]"))
            if i % 4 == 1:
                # 每 4 篇一条指向前一篇片段的有向引用
                rel_rows.append((release_id, doc_id, "ref",
                                 f"frag-DOC{i-1:06d}", frag_uid, "auto",
                                 "rev_scale", "[]"))
            if len(frag_rows) >= 500:
                conn.executemany(
                    "INSERT INTO release_fragment(release_id, fragment_uid,"
                    " doc_id, layer, label, start_byte, end_byte,"
                    " text_sha256, seen_text, origin, source_kind, source_rev,"
                    " evidence_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    frag_rows)
                frag_rows.clear()
            if len(rel_rows) >= 500:
                conn.executemany(
                    "INSERT INTO release_relation(release_id, doc_id,"
                    " rel_type, from_fragment, to_fragment, source_kind,"
                    " source_rev, evidence_json)"
                    " VALUES (?,?,?,?,?,?,?,?)", rel_rows)
                rel_rows.clear()
        if frag_rows:
            conn.executemany(
                "INSERT INTO release_fragment(release_id, fragment_uid,"
                " doc_id, layer, label, start_byte, end_byte, text_sha256,"
                " seen_text, origin, source_kind, source_rev, evidence_json)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", frag_rows)
        if rel_rows:
            conn.executemany(
                "INSERT INTO release_relation(release_id, doc_id, rel_type,"
                " from_fragment, to_fragment, source_kind, source_rev,"
                " evidence_json) VALUES (?,?,?,?,?,?,?,?)", rel_rows)
        conn.execute(
            "INSERT INTO release(release_id, rev_id, rev_seq, created_at,"
            " doc_ids_json, request_json) VALUES (?,?,?,?,?,?)",
            (release_id, "rev_scale", 1, "2026-01-01T00:00:00Z",
             json.dumps(doc_ids), "{}"))
        conn.commit()
    finally:
        conn.close()
    return {"root": root, "workspace": workspace, "release_id": release_id,
            "docs": docs}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--docs", type=int, default=30000)
    args = ap.parse_args()
    info = generate(args.root, args.docs)
    print(json.dumps(info, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
