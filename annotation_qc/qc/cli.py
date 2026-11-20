# -*- coding: utf-8 -*-
"""命令行入口：构建、只读查询、流式导出。

用法见 docs/usage.md，常用形式::

    python -m qc.cli build   --manifest manifest.json --packages in/ --policy policy.json --out out/warehouse.db
    python -m qc.cli query   --db out/warehouse.db --what regions --doc D1 --layer clause
    python -m qc.cli show    --db out/warehouse.db --region <region_uid>
    python -m qc.cli show-rel --db out/warehouse.db --group <rel_group_uid>
    python -m qc.cli export  --db out/warehouse.db --what official_fragments --out f.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys

from .build import build
from .policy import load_policy
from .query import QueryDB


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="qc", description="离线质检入仓管道")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build", help="构建正式产物库（失败不发布）")
    p_build.add_argument("--manifest", required=True, help="原文清单 JSON")
    p_build.add_argument("--packages", required=True, help="标注包根目录或单个 jsonl")
    p_build.add_argument("--policy", required=True, help="策略声明 JSON")
    p_build.add_argument("--out", required=True, help="正式输出 db 路径")
    p_build.add_argument("--reject-report", default=None, help="拒收报告输出路径")
    p_build.set_defaults(func=_cmd_build)

    p_query = sub.add_parser("query", help="只读查询")
    p_query.add_argument("--db", required=True)
    p_query.add_argument(
        "--what",
        required=True,
        choices=[
            "meta",
            "documents",
            "fragments",
            "relations",
            "regions",
            "reviews",
            "blocked",
        ],
    )
    p_query.add_argument("--doc", default=None)
    p_query.add_argument("--layer", default=None)
    p_query.add_argument("--rel-type", default=None)
    p_query.add_argument("--kind", default=None,
                         choices=["region", "relation"],
                         help="复核项种类过滤")
    p_query.set_defaults(func=_cmd_query)

    p_show = sub.add_parser("show", help="展开某个争议区域的完整证据")
    p_show.add_argument("--db", required=True)
    p_show.add_argument("--region", required=True)
    p_show.set_defaults(func=_cmd_show_region)

    p_showrel = sub.add_parser("show-rel", help="展开某个关系归票组的完整证据")
    p_showrel.add_argument("--db", required=True)
    p_showrel.add_argument("--group", required=True)
    p_showrel.set_defaults(func=_cmd_show_relation)

    p_trace = sub.add_parser("trace-rel", help="追踪单条原始关系记录")
    p_trace.add_argument("--db", required=True)
    p_trace.add_argument("--record", required=True)
    p_trace.set_defaults(func=_cmd_trace_relation)

    p_export = sub.add_parser("export", help="流式 JSONL 导出")
    p_export.add_argument("--db", required=True)
    p_export.add_argument(
        "--what",
        required=True,
        choices=["official_fragments", "official_relations", "review_items"],
    )
    p_export.add_argument("--doc", default=None)
    p_export.add_argument("--layer", default=None)
    p_export.add_argument("--rel-type", default=None)
    p_export.add_argument("--kind", default=None)
    p_export.add_argument("--out", default=None, help="输出文件；缺省输出到 stdout")
    p_export.set_defaults(func=_cmd_export)

    args = parser.parse_args(argv)
    return args.func(args)


def _cmd_build(args) -> int:
    policy = load_policy(args.policy)
    result = build(
        manifest_path=args.manifest,
        packages_root=args.packages,
        output_db=args.out,
        policy=policy,
        reject_report=args.reject_report,
    )
    if result.errors:
        print(
            json.dumps(
                {
                    "published": False,
                    "error_count": len(result.errors),
                    "reject_report": result.reject_report,
                    "first_errors": [e.to_dict() for e in result.errors[:10]],
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {"published": True, "db": result.output_db,
             "counters": result.counters},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _cmd_query(args) -> int:
    with QueryDB(args.db) as qdb:
        if args.what == "meta":
            payload = qdb.meta()
        elif args.what == "documents":
            payload = qdb.documents()
        elif args.what == "fragments":
            payload = qdb.official_fragments(args.doc, args.layer)
        elif args.what == "relations":
            payload = qdb.official_relations(args.doc, args.rel_type)
        elif args.what == "regions":
            payload = qdb.list_regions(args.doc, args.layer)
        elif args.what == "reviews":
            payload = qdb.review_items(
                args.doc, args.layer, args.rel_type, args.kind
            )
        elif args.what == "blocked":
            payload = qdb.blocked_relations(args.doc, args.rel_type)
        else:  # pragma: no cover
            raise AssertionError(args.what)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _cmd_show_region(args) -> int:
    with QueryDB(args.db) as qdb:
        print(
            json.dumps(
                qdb.region_detail(args.region),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def _cmd_show_relation(args) -> int:
    with QueryDB(args.db) as qdb:
        print(
            json.dumps(
                qdb.relation_group_detail(args.group),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def _cmd_trace_relation(args) -> int:
    with QueryDB(args.db) as qdb:
        print(
            json.dumps(
                qdb.relation_trace_for_record(args.record),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def _cmd_export(args) -> int:
    filters = {}
    if args.doc is not None:
        filters["doc_id"] = args.doc
    if args.layer is not None:
        filters["layer"] = args.layer
    if args.rel_type is not None:
        filters["rel_type"] = args.rel_type
    if args.kind is not None:
        filters["item_kind"] = args.kind
    with QueryDB(args.db) as qdb:
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                for line in qdb.export_stream(args.what, **filters):
                    fh.write(line)
        else:
            for line in qdb.export_stream(args.what, **filters):
                sys.stdout.write(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
