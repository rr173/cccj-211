# -*- coding: utf-8 -*-
"""裁决工作区命令行。

    qc-ws init     --workspace ws/ --base-db out/warehouse.db --policy policy.json
    qc-ws view     --workspace ws/ --rev HEAD [--doc D1] [--layer clause]
    qc-ws import   --workspace ws/ --ticket ticket.json [--manifest m.json]
    qc-ws preview  --workspace ws/ --ticket t.json [--ticket-id T]
    qc-ws merge    --workspace ws/ --ticket t.json [--ticket-id T]
    qc-ws status   --workspace ws/ [--rev R]
    qc-ws release  --workspace ws/ --rev R --doc D1 --doc D2
    qc-ws blockers --workspace ws/ --rev R --doc D1
    qc-ws export   --workspace ws/ --release REL --what fragments|relations|provenance
    qc-ws show     --workspace ws/ --rev R [--region UID | --rel-group UID]
"""
from __future__ import annotations

import argparse
import json
import sys

from .ws_common import WsError, connect, resolve_revision
from .ws_init import init_workspace
from .ws_import import import_ticket
from .ws_merge import merge, preview
from .ws_release import (
    blockers,
    get_release,
    list_releases,
    release,
    stream_release,
)
from .ws_source import WorkspaceSources
from .ws_ticket import load_ticket


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="qc-ws", description="公文标注裁决工作区")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="用现有构建库初始化独立工作区")
    p_init.add_argument("--workspace", required=True)
    p_init.add_argument("--base-db", required=True)
    p_init.add_argument("--policy", required=True)
    p_init.add_argument("--reviewer", default=None)
    p_init.set_defaults(func=_cmd_init)

    p_view = sub.add_parser("view", help="查看指定版本的区域/片段/引用/待办")
    p_view.add_argument("--workspace", required=True)
    p_view.add_argument("--rev", default="HEAD")
    p_view.add_argument("--doc", default=None)
    p_view.add_argument("--layer", default=None)
    p_view.set_defaults(func=_cmd_view)

    p_import = sub.add_parser("import", help="导入基于指定版本的工单")
    p_import.add_argument("--workspace", required=True)
    p_import.add_argument("--ticket", required=True)
    p_import.add_argument("--base", default=None)
    p_import.add_argument("--manifest", default=None)
    p_import.add_argument("--source-root", default=None)
    p_import.set_defaults(func=_cmd_import)

    p_preview = sub.add_parser("preview", help="按工单基版预览整批提案")
    p_preview.add_argument("--workspace", required=True)
    p_preview.add_argument("--ticket", default=None)
    p_preview.add_argument("--ticket-id", default=None)
    p_preview.add_argument("--manifest", default=None)
    p_preview.add_argument("--source-root", default=None)
    p_preview.set_defaults(func=_cmd_preview)

    p_merge = sub.add_parser("merge", help="合批一份工单")
    p_merge.add_argument("--workspace", required=True)
    p_merge.add_argument("--ticket", default=None)
    p_merge.add_argument("--ticket-id", default=None)
    p_merge.add_argument("--manifest", default=None)
    p_merge.add_argument("--source-root", default=None)
    p_merge.set_defaults(func=_cmd_merge)

    p_status = sub.add_parser("status", help="版本与批次总览")
    p_status.add_argument("--workspace", required=True)
    p_status.add_argument("--rev", default="HEAD")
    p_status.set_defaults(func=_cmd_status)

    p_release = sub.add_parser("release", help="登记审定发布")
    p_release.add_argument("--workspace", required=True)
    p_release.add_argument("--rev", default="HEAD")
    p_release.add_argument("--doc", action="append", required=True)
    p_release.set_defaults(func=_cmd_release)

    p_blockers = sub.add_parser("blockers", help="列出挡住发布的事项")
    p_blockers.add_argument("--workspace", required=True)
    p_blockers.add_argument("--rev", default="HEAD")
    p_blockers.add_argument("--doc", action="append", default=None)
    p_blockers.set_defaults(func=_cmd_blockers)

    p_releases = sub.add_parser("releases", help="已登记发布列表")
    p_releases.add_argument("--workspace", required=True)
    p_releases.set_defaults(func=_cmd_releases)

    p_export = sub.add_parser("export", help="流式导出某次发布")
    p_export.add_argument("--workspace", required=True)
    p_export.add_argument("--release", required=True)
    p_export.add_argument(
        "--what", required=True,
        choices=["fragments", "relations", "provenance"])
    p_export.add_argument("--doc", default=None)
    p_export.add_argument("--out", default=None)
    p_export.set_defaults(func=_cmd_export)

    p_show = sub.add_parser("show", help="展开证据")
    p_show.add_argument("--workspace", required=True)
    p_show.add_argument("--rev", default="HEAD")
    p_show.add_argument("--region", default=None)
    p_show.add_argument("--rel-group", default=None)
    p_show.add_argument("--record", default=None)
    p_show.add_argument("--release", default=None)
    p_show.set_defaults(func=_cmd_show)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except WsError as exc:
        print(json.dumps({"ok": False, **exc.to_dict()}, ensure_ascii=False,
                         indent=2), file=sys.stderr)
        return 2


def _emit(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _sources(conn, args) -> WorkspaceSources:
    return WorkspaceSources(conn, manifest_path=getattr(args, "manifest", None),
                            source_root=getattr(args, "source_root", None))


def _cmd_init(args) -> int:
    result = init_workspace(
        args.workspace, args.base_db, args.policy, reviewer=args.reviewer)
    _emit({"ok": True, **result})
    return 0


def _cmd_import(args) -> int:
    conn = connect(args.workspace)
    try:
        data = load_ticket(args.ticket)
        result = import_ticket(conn, _sources(conn, args), data,
                               base_hint=args.base)
        _emit({"ok": True, **result})
    finally:
        conn.close()
    return 0


def _ticket_kwargs(args) -> dict:
    if args.ticket_id:
        return {"ticket_uid": args.ticket_id}
    data = load_ticket(args.ticket)
    return {"data": data}


def _cmd_preview(args) -> int:
    conn = connect(args.workspace)
    try:
        if not args.ticket_id and not args.ticket:
            raise WsError("TICKET_REQUIRED", "provide --ticket or --ticket-id")
        result = preview(conn, _sources(conn, args), **_ticket_kwargs(args))
        _emit({"ok": True, **result})
    finally:
        conn.close()
    return 0


def _cmd_merge(args) -> int:
    conn = connect(args.workspace)
    try:
        if not args.ticket_id and not args.ticket:
            raise WsError("TICKET_REQUIRED", "provide --ticket or --ticket-id")
        result = merge(conn, _sources(conn, args), args.workspace,
                       **_ticket_kwargs(args))
        _emit({"ok": result["status"] == "merged", **result})
        return 0 if result["status"] == "merged" else 3
    finally:
        conn.close()


def _cmd_view(args) -> int:
    from .ws_view import view_revision

    conn = connect(args.workspace)
    try:
        _emit(view_revision(conn, args.rev, doc_id=args.doc, layer=args.layer))
    finally:
        conn.close()
    return 0


def _cmd_status(args) -> int:
    conn = connect(args.workspace)
    try:
        revision = resolve_revision(conn, args.rev)
        head = resolve_revision(conn, "HEAD")
        revisions = [
            dict(r) for r in conn.execute(
                "SELECT rev_id, seq, parent_rev, base_rev, ticket_uid,"
                " created_at FROM revision ORDER BY seq")
        ]
        batches = [
            dict(r) for r in conn.execute(
                "SELECT batch_id, ticket_uid, base_rev, current_rev,"
                " result_rev, status, created_at FROM merge_batch"
                " ORDER BY created_at")
        ]
        counts = {
            table: conn.execute(f"SELECT COUNT(*) c FROM {table}").fetchone()["c"]
            for table in ("region_state", "fragment_state", "relation_state",
                          "blocked_state", "state_evidence")
        }
        _emit({
            "head_rev": head["rev_id"],
            "head_seq": head["seq"],
            "revision": {k: revision[k] for k in revision.keys()},
            "revisions": revisions,
            "batches": batches,
            "row_counts": counts,
            "releases": list_releases(conn),
        })
    finally:
        conn.close()
    return 0


def _cmd_release(args) -> int:
    conn = connect(args.workspace)
    try:
        result = release(conn, args.workspace, rev=args.rev, doc_ids=args.doc)
        _emit({"ok": result.get("released", False), **result})
        return 0 if result.get("released") else 3
    finally:
        conn.close()


def _cmd_blockers(args) -> int:
    conn = connect(args.workspace)
    try:
        result = blockers(conn, rev=args.rev, doc_ids=args.doc)
        _emit({"ok": not result["blocked"], **result})
        return 0 if not result["blocked"] else 3
    finally:
        conn.close()


def _cmd_releases(args) -> int:
    conn = connect(args.workspace)
    try:
        _emit(list_releases(conn))
    finally:
        conn.close()
    return 0


def _cmd_export(args) -> int:
    conn = connect(args.workspace)
    try:
        get_release(conn, args.release)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                for line in stream_release(conn, args.release, what=args.what,
                                           doc_id=args.doc):
                    fh.write(line)
        else:
            for line in stream_release(conn, args.release, what=args.what,
                                       doc_id=args.doc):
                sys.stdout.write(line)
    finally:
        conn.close()
    return 0


def _cmd_show(args) -> int:
    from .ws_view import show_record, show_region, show_relation, show_release

    conn = connect(args.workspace)
    try:
        if args.release:
            payload = show_release(conn, args.release)
        elif args.region:
            payload = show_region(conn, args.rev, args.region)
        elif args.rel_group:
            payload = show_relation(conn, args.rev, args.rel_group)
        elif args.record:
            payload = show_record(conn, args.rev, args.record)
        else:
            raise WsError("SHOW_TARGET", "provide --region/--rel-group/"
                          "--record/--release")
        _emit(payload)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
