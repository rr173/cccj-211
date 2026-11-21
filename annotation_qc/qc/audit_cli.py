# -*- coding: utf-8 -*-
"""发布后独立盲审命令行（qc-audit）。

    qc-audit create    --audit A --workspace WS --request req.json
    qc-audit batches   --audit A
    qc-audit show      --audit A --batch B
    qc-audit coverage  --audit A --batch B [--out file]
    qc-audit unsettled --audit A --batch B [--out file]
    qc-audit results   --audit A --batch B [--out file]
    qc-audit snapshot  --audit A --batch B [--out file]
    qc-audit blind     --audit A --batch B [--out file]
    qc-audit source    --audit A --batch B --blind-doc BD
                       [--start N --end M] [--manifest M] [--source-root R]
    qc-audit resolve   --audit A --batch B --blind-id ID
    qc-audit submit    --audit A --request req.json [--manifest M]

请求 JSON::

    {"request_id": "r1", "release_id": "release_…",
     "sample_size": 3, "seed": 20261121}

    {"request_id": "s1", "batch_id": "aub_…", "reviewer": "auditor",
     "results": [
       {"subject": {"kind": "fragment", "blind_id": "bf_…"},
        "verdict": "issue", "note": "标签与正文不符",
        "evidence": {"blind_doc_id": "bd_…", "start": 0, "end": 10,
                     "text": "0123456789"}},
       {"subject": {"kind": "document", "blind_id": "bd_…"},
        "verdict": "conforming", "note": "通读未见异常"}
     ]}

``blind`` 导出绝不含正式档号、发布/版本、团队、投票、裁决来源或理由；
档案人员需要正式身份与证据定位时用 ``snapshot`` / ``results``。
"""
from __future__ import annotations

import argparse
import json
import sys

from .audit import (
    blind_source,
    batch_coverage,
    create_batch,
    get_batch,
    init_store,
    list_batches,
    resolve_blind,
    stream_blind,
    stream_results,
    stream_snapshot,
    stream_unreviewed,
    submit_results,
)
from .audit_common import AuditError, open_audit


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="qc-audit", description="发布后独立盲审")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="创建（或重开）盲审批次")
    p_create.add_argument("--audit", required=True)
    p_create.add_argument("--workspace", required=True,
                          help="裁决工作区（只读上游）")
    p_create.add_argument("--request", required=True, help="创建请求 JSON")
    p_create.set_defaults(func=_cmd_create)

    p_batches = sub.add_parser("batches", help="批次列表")
    p_batches.add_argument("--audit", required=True)
    p_batches.set_defaults(func=_cmd_batches)

    p_show = sub.add_parser("show", help="批次总览")
    p_show.add_argument("--audit", required=True)
    p_show.add_argument("--batch", required=True)
    p_show.set_defaults(func=_cmd_show)

    for name, what, helptext in (
            ("coverage", "coverage", "抽中范围与未审数"),
            ("unsettled", "unreviewed", "未审记录（流式）"),
            ("results", "results", "审计结论与证据（流式）"),
            ("snapshot", "snapshot", "批次快照（流式）"),
            ("blind", "blind", "盲审导出（流式）")):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--audit", required=True)
        p.add_argument("--batch", required=True)
        p.add_argument("--out", default=None)
        p.set_defaults(func=_cmd_stream, what=what)

    p_source = sub.add_parser("source", help="按盲文档标识取原文/精确字节范围")
    p_source.add_argument("--audit", required=True)
    p_source.add_argument("--batch", required=True)
    p_source.add_argument("--blind-doc", required=True)
    p_source.add_argument("--start", type=int, default=None)
    p_source.add_argument("--end", type=int, default=None)
    p_source.add_argument("--manifest", default=None)
    p_source.add_argument("--source-root", default=None)
    p_source.set_defaults(func=_cmd_source)

    p_resolve = sub.add_parser("resolve", help="盲审标识 -> 正式身份（档案）")
    p_resolve.add_argument("--audit", required=True)
    p_resolve.add_argument("--batch", required=True)
    p_resolve.add_argument("--blind-id", required=True)
    p_resolve.set_defaults(func=_cmd_resolve)

    p_submit = sub.add_parser("submit", help="提交审核结论（幂等）")
    p_submit.add_argument("--audit", required=True)
    p_submit.add_argument("--request", required=True)
    p_submit.add_argument("--manifest", default=None)
    p_submit.add_argument("--source-root", default=None)
    p_submit.set_defaults(func=_cmd_submit)

    p_init = sub.add_parser("init", help="显式初始化空审计库")
    p_init.add_argument("--audit", required=True)
    p_init.set_defaults(func=_cmd_init)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except AuditError as exc:
        print(json.dumps({"ok": False, **exc.to_dict()}, ensure_ascii=False,
                         indent=2), file=sys.stderr)
        return 2


def _emit(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise AuditError("BAD_REQUEST", "请求 JSON 必须是对象")
    return data


def _cmd_init(args) -> int:
    _emit({"ok": True, **init_store(args.audit)})
    return 0


def _cmd_create(args) -> int:
    conn = open_audit(args.audit)
    try:
        result = create_batch(args.audit, conn, args.workspace,
                              _load(args.request))
        _emit(result)
    finally:
        conn.close()
    return 0


def _cmd_batches(args) -> int:
    conn = open_audit(args.audit)
    try:
        _emit(list_batches(conn))
    finally:
        conn.close()
    return 0


def _cmd_show(args) -> int:
    conn = open_audit(args.audit)
    try:
        _emit(get_batch(conn, args.batch))
    finally:
        conn.close()
    return 0


def _cmd_stream(args) -> int:
    conn = open_audit(args.audit)
    try:
        what = args.what
        if what == "coverage":
            payload = batch_coverage(conn, args.batch)
            if args.out:
                with open(args.out, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, ensure_ascii=False, indent=2,
                              sort_keys=True)
            else:
                _emit(payload)
            return 0
        stream = {
            "unreviewed": stream_unreviewed,
            "results": stream_results,
            "snapshot": stream_snapshot,
            "blind": stream_blind,
        }[what](conn, args.batch)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                for line in stream:
                    fh.write(line)
        else:
            for line in stream:
                sys.stdout.write(line)
        return 0
    finally:
        conn.close()


def _cmd_source(args) -> int:
    conn = open_audit(args.audit)
    try:
        _emit(blind_source(
            conn, args.batch, args.blind_doc, args.start, args.end,
            manifest_path=args.manifest, source_root=args.source_root))
    finally:
        conn.close()
    return 0


def _cmd_resolve(args) -> int:
    conn = open_audit(args.audit)
    try:
        _emit(resolve_blind(conn, args.batch, args.blind_id))
    finally:
        conn.close()
    return 0


def _cmd_submit(args) -> int:
    conn = open_audit(args.audit)
    try:
        result = submit_results(
            args.audit, conn, _load(args.request),
            manifest_path=args.manifest, source_root=args.source_root)
        _emit(result)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
