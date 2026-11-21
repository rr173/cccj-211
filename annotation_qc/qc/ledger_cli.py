# -*- coding: utf-8 -*-
"""分仓台账命令行。

    qc-ledger init        --ledger L --workspace WS --layer clause \\
                          --min-bytes 16 [--exempt exempt.json]
    qc-ledger apply       --ledger L --batch batch.json
    qc-ledger confirm     --ledger L --request confirm.json
    qc-ledger lists       --ledger L [--out DIR]
    qc-ledger explain     --ledger L --doc D
    qc-ledger path        --ledger L --from A --to B
    qc-ledger quarantine  --ledger L --doc D
    qc-ledger credentials --ledger L --doc D
    qc-ledger status      --ledger L
    qc-ledger verify      --ledger L
    qc-ledger rebuild     --ledger L

变更批 JSON（apply）::

    {"request_id": "b1", "base_version": 0,
     "select": [{"doc_id": "D1", "release_id": "rel_…"}],
     "withdraw": ["D2"],
     "evidence_register": [{"evidence_id": "E1", "doc_a": "D1",
                            "doc_b": "D3", "provenance": "出处"}],
     "evidence_revoke": ["E0"]}

交付确认 JSON（confirm）::

    {"request_id": "c1", "base_version": 1, "list_version": 1,
     "doc_ids": ["D1", "D2"]}
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .ledger import Ledger, init_ledger
from .ledger_common import LedgerError
from .util import sha256_hex


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="qc-ledger", description="公文标注语料分仓台账")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="建账：固定标注层/最短字节数/豁免表")
    p_init.add_argument("--ledger", required=True)
    p_init.add_argument("--workspace", required=True, help="裁决工作区（只读上游）")
    p_init.add_argument("--layer", required=True, help="建账选定的标注层")
    p_init.add_argument("--min-bytes", type=int, required=True,
                        help="合格片段的最短 UTF-8 字节数")
    p_init.add_argument("--exempt", default=None,
                        help="片段摘要豁免表 JSON："
                             '{"digests": [...], "texts": [...]}')
    p_init.set_defaults(func=_cmd_init)

    p_apply = sub.add_parser("apply", help="提交变更批（选入/改选/撤出/证据）")
    p_apply.add_argument("--ledger", required=True)
    p_apply.add_argument("--batch", required=True, help="变更批 JSON 文件，- 为标准输入")
    p_apply.set_defaults(func=_cmd_apply)

    p_confirm = sub.add_parser("confirm", help="确认部分清单已对外交付")
    p_confirm.add_argument("--ledger", required=True)
    p_confirm.add_argument("--request", required=True, help="确认请求 JSON 文件，- 为标准输入")
    p_confirm.set_defaults(func=_cmd_confirm)

    p_lists = sub.add_parser("lists", help="导出当前训练/评测/隔离清单")
    p_lists.add_argument("--ledger", required=True)
    p_lists.add_argument("--out", default=None, help="写出 JSONL 的目录（缺省打印 JSON）")
    p_lists.set_defaults(func=_cmd_lists)

    p_explain = sub.add_parser("explain", help="公文为什么分在一起/不能交付")
    p_explain.add_argument("--ledger", required=True)
    p_explain.add_argument("--doc", required=True)
    p_explain.set_defaults(func=_cmd_explain)

    p_path = sub.add_parser("path", help="两篇公文之间的证据路径")
    p_path.add_argument("--ledger", required=True)
    p_path.add_argument("--from", dest="frm", required=True)
    p_path.add_argument("--to", required=True)
    p_path.set_defaults(func=_cmd_path)

    p_quar = sub.add_parser("quarantine", help="隔离原因：两个仓位的历史确认与证据路径")
    p_quar.add_argument("--ledger", required=True)
    p_quar.add_argument("--doc", required=True)
    p_quar.set_defaults(func=_cmd_quarantine)

    p_cred = sub.add_parser("credentials", help="取回历史交付凭据")
    p_cred.add_argument("--ledger", required=True)
    p_cred.add_argument("--doc", required=True)
    p_cred.set_defaults(func=_cmd_credentials)

    p_status = sub.add_parser("status", help="台账总览与最近诊断")
    p_status.add_argument("--ledger", required=True)
    p_status.set_defaults(func=_cmd_status)

    p_verify = sub.add_parser("verify", help="全量重算对照检查（在副本上进行）")
    p_verify.add_argument("--ledger", required=True)
    p_verify.set_defaults(func=_cmd_verify)

    p_rebuild = sub.add_parser("rebuild", help="全库重建所有连通分量（管理用途）")
    p_rebuild.add_argument("--ledger", required=True)
    p_rebuild.set_defaults(func=_cmd_rebuild)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except LedgerError as exc:
        print(json.dumps({"ok": False, **exc.to_dict()}, ensure_ascii=False,
                         indent=2), file=sys.stderr)
        return 2


def _emit(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _read_json(path: str):
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _cmd_init(args) -> int:
    exempt = []
    if args.exempt:
        data = _read_json(args.exempt)
        for d in data.get("digests", []):
            exempt.append(d)
        for t in data.get("texts", []):
            exempt.append(sha256_hex(t.encode("utf-8")))
    result = init_ledger(args.ledger, args.workspace, layer=args.layer,
                         min_segment_bytes=args.min_bytes,
                         exempt_digests=exempt)
    _emit({"ok": True, **result})
    return 0


def _cmd_apply(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.apply_change(_read_json(args.batch)))
    return 0


def _cmd_confirm(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.confirm(_read_json(args.request)))
    return 0


def _cmd_lists(args) -> int:
    with Ledger(args.ledger) as ledger:
        lists = ledger.lists()
    if not args.out:
        _emit(lists)
        return 0
    os.makedirs(args.out, exist_ok=True)
    for name in ("train", "eval", "quarantine"):
        with open(os.path.join(args.out, f"{name}.jsonl"), "w",
                  encoding="utf-8", newline="\n") as fh:
            for row in lists[name]:
                fh.write(json.dumps(row, ensure_ascii=False,
                                    sort_keys=True) + "\n")
    with open(os.path.join(args.out, "manifest.json"), "w",
              encoding="utf-8", newline="\n") as fh:
        json.dump({
            "version": lists["version"],
            "counts": {k: len(lists[k]) for k in ("train", "eval", "quarantine")},
        }, fh, ensure_ascii=False, indent=2, sort_keys=True)
    _emit({"ok": True, "version": lists["version"], "out": args.out,
           "counts": {k: len(lists[k]) for k in ("train", "eval", "quarantine")}})
    return 0


def _cmd_explain(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.explain_doc(args.doc))
    return 0


def _cmd_path(args) -> int:
    with Ledger(args.ledger) as ledger:
        path = ledger.path_between(args.frm, args.to)
    _emit({"from": args.frm, "to": args.to, "connected": path is not None,
           "path": path})
    return 0


def _cmd_quarantine(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.quarantine_reason(args.doc))
    return 0


def _cmd_credentials(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit({"doc_id": args.doc, "deliveries": ledger.credentials(args.doc)})
    return 0


def _cmd_status(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.status())
    return 0


def _cmd_verify(args) -> int:
    with Ledger(args.ledger) as ledger:
        result = ledger.verify()
    _emit(result)
    return 0 if result["consistent"] else 3


def _cmd_rebuild(args) -> int:
    with Ledger(args.ledger) as ledger:
        _emit(ledger.rebuild_all())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
