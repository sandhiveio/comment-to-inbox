import argparse
import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path

from comment_to_inbox.adapter import AdapterNotConfigured, FakeInstagram, ZernioAdapter
from comment_to_inbox.clock import SystemClock, add_seconds
from comment_to_inbox.config import ProjectConfig, load_config
from comment_to_inbox.db import connect, init_db, transaction
from comment_to_inbox.dry_run import run_dry
from comment_to_inbox.exporting import (
    DEFAULT_BATCH_SIZE,
    FakeLeadExporter,
    export_once,
)
from comment_to_inbox.flow import clear_handoff
from comment_to_inbox.inbox_poll import ZernioInbox
from comment_to_inbox.ingest import accept_event
from comment_to_inbox.notifying import FakeOperatorNotifier, notify_once
from comment_to_inbox.report import render_status
from comment_to_inbox.stats import parse_day, previous_closed_utc_day, render_stats_csv
from comment_to_inbox.worker import Worker

DEMO_COMMENT_ID = "demo-comment"
DEMO_MESSAGE_ID = "demo-message"
DEMO_PERSON_ID = "demo-reader"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="comment_to_inbox")
    parser.add_argument("--config", default=None)
    parser.add_argument("--database", default=None)
    subcommands = parser.add_subparsers(dest="command", required=True)

    serve = subcommands.add_parser("serve", help="run the single send worker")
    serve.add_argument(
        "--adapter",
        default="fake",
        choices=("fake", "zernio"),
        help="fake is the demo adapter; zernio sends through the configured Inbox API",
    )
    serve.set_defaults(func=_serve)

    demo = subcommands.add_parser("demo", help="run the scripted comment-to-link scenario")
    demo.set_defaults(func=_demo)

    status = subcommands.add_parser("status", help="print runs, actions, and results")
    status.set_defaults(func=_status)

    stats = subcommands.add_parser(
        "stats",
        help="print one CSV row of daily counters for this project",
    )
    stats.add_argument(
        "--day",
        default=None,
        help="UTC day YYYY-MM-DD; default is the previous closed UTC day",
    )
    stats.add_argument("--format", default="csv", choices=("csv",))
    stats.set_defaults(func=_stats)

    export_leads = subcommands.add_parser(
        "export-leads",
        help="export one batch of leads and exit",
    )
    export_leads.add_argument(
        "--once",
        action="store_true",
        required=True,
        help="process one limited batch, then exit",
    )
    export_leads.set_defaults(func=_export_leads)

    notify = subcommands.add_parser(
        "notify-operators",
        help="deliver one batch of operator handoffs and exit",
    )
    notify.add_argument(
        "--once",
        action="store_true",
        required=True,
        help="process one limited batch, then exit",
    )
    notify.set_defaults(func=_notify_operators)

    clear = subcommands.add_parser(
        "clear-handoff",
        help="clear an active operator handoff without restarting the run",
    )
    clear.add_argument("--person-id", required=True)
    clear.set_defaults(func=_clear_handoff)

    sync = subcommands.add_parser(
        "sync-posts",
        help="preview new posts, or add them to the config as disabled",
    )
    sync.add_argument(
        "--apply",
        action="store_true",
        help="write newly discovered posts as disabled",
    )
    sync.set_defaults(func=_sync_posts)

    dry = subcommands.add_parser(
        "dry-run",
        help="read comments and describe events without sending",
    )
    dry.set_defaults(func=_dry_run)

    ingest = subcommands.add_parser("ingest", help="accept one normalized event")
    ingest_commands = ingest.add_subparsers(dest="ingest_command", required=True)
    comment = ingest_commands.add_parser("comment")
    comment.add_argument("--external-id", required=True)
    comment.add_argument("--person-id", required=True)
    comment.add_argument("--post-id", required=True)
    comment.add_argument("--comment-id", required=True)
    comment.add_argument("--text", required=True)
    comment.add_argument("--occurred-at", required=True)
    comment.set_defaults(func=_ingest_comment)
    message = ingest_commands.add_parser("message")
    message.add_argument("--external-id", required=True)
    message.add_argument("--person-id", required=True)
    message.add_argument("--text", required=True)
    message.add_argument("--occurred-at", required=True)
    message.add_argument(
        "--conversation-id",
        default="",
        help="Zernio conversation id for this sender; required before a live direct message",
    )
    message.set_defaults(func=_ingest_message)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2


def _serve(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config, connection, database = _open(args)
    worker = _worker(connection, config, adapter_name=args.adapter)
    if args.adapter == "zernio":
        worker.inbox = ZernioInbox(config)
    try:
        asyncio.run(worker.serve())
    finally:
        connection.close()
    return 0


def _demo(args: argparse.Namespace) -> int:
    config, connection, database = _open(args)
    try:
        asyncio.run(_run_demo(connection, config, database))
    finally:
        connection.close()
    return 0


def _export_leads(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _config, connection, _database = _open(args)
    mode = os.environ.get("CTI_EXPORT_RESULT", "success")
    if mode not in ("success", "temporary", "permanent"):
        print("error: CTI_EXPORT_RESULT must be success, temporary, or permanent")
        return 2
    script = [] if mode == "success" else [mode] * DEFAULT_BATCH_SIZE
    try:
        stats = export_once(connection, FakeLeadExporter(script), SystemClock())
    finally:
        connection.close()
    print(
        "export_once claimed={claimed} exported={exported} retry={retry} "
        "permanent={permanent} timed_out={timed_out}".format(**stats)
    )
    return 0


def _notify_operators(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _config, connection, _database = _open(args)
    mode = os.environ.get("CTI_NOTIFY_RESULT", "success")
    if mode not in ("success", "temporary"):
        print("error: CTI_NOTIFY_RESULT must be success or temporary")
        return 2
    script = [] if mode == "success" else ["temporary"] * 10
    try:
        stats, lines = notify_once(connection, FakeOperatorNotifier(script), SystemClock())
    finally:
        connection.close()
    for line in lines:
        print(line)
    print(
        "notify_once claimed={claimed} delivered={delivered} retry={retry} "
        "timed_out={timed_out}".format(**stats)
    )
    return 0


def _clear_handoff(args: argparse.Namespace) -> int:
    config, connection, _database = _open(args)
    try:
        with transaction(connection):
            cleared = clear_handoff(
                connection, config.account_id, args.person_id, SystemClock().now()
            )
    finally:
        connection.close()
    print(f"clear_handoff person_id={args.person_id} cleared={cleared}")
    return 0


def _sync_posts(args: argparse.Namespace) -> int:
    path = args.config or os.environ.get("CTI_CONFIG") or "config/project.json"
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    key = str(raw.get("zernio_api_key") or "").strip()
    if not key:
        print("error: zernio_api_key is empty")
        return 2
    print("error: live post listing is not configured for this demo")
    return 2


def _dry_run(args: argparse.Namespace) -> int:
    path = args.config or os.environ.get("CTI_CONFIG") or "config/local.json"
    return run_dry(path)


def _stats(args: argparse.Namespace) -> int:
    config, connection, _database = _open(args)
    try:
        day = previous_closed_utc_day() if args.day is None else parse_day(args.day)
        print(render_stats_csv(connection, config.account_id, day), end="")
    finally:
        connection.close()
    return 0


def _status(args: argparse.Namespace) -> int:
    config, connection, database = _open(args)
    try:
        print(render_status(connection, config, database))
    finally:
        connection.close()
    return 0


def _ingest_comment(args: argparse.Namespace) -> int:
    config, connection, _database = _open(args)
    try:
        result = accept_event(
            connection,
            config,
            {
                "account_id": config.account_id,
                "external_id": args.external_id,
                "kind": "comment",
                "person_id": args.person_id,
                "post_id": args.post_id,
                "comment_id": args.comment_id,
                "body": args.text,
                "occurred_at": args.occurred_at,
            },
            SystemClock().now(),
        )
    finally:
        connection.close()
    _print_ingest(result)
    return 0


def _ingest_message(args: argparse.Namespace) -> int:
    config, connection, _database = _open(args)
    try:
        result = accept_event(
            connection,
            config,
            {
                "account_id": config.account_id,
                "external_id": args.external_id,
                "kind": "message",
                "person_id": args.person_id,
                "body": args.text,
                "occurred_at": args.occurred_at,
                "conversation_id": args.conversation_id,
            },
            SystemClock().now(),
        )
    finally:
        connection.close()
    _print_ingest(result)
    return 0


async def _run_demo(
    connection: sqlite3.Connection, config: ProjectConfig, database: str
) -> None:
    clock = SystemClock()
    worker = _worker(connection, config, clock=clock)
    worker.recover()
    existing = connection.execute(
        """
        SELECT id FROM inbound_events
        WHERE account_id = ? AND external_id = ?
        """,
        (config.account_id, DEMO_COMMENT_ID),
    ).fetchone()
    if existing is None:
        post_id = config.post_ids[0]
        comment = accept_event(
            connection,
            config,
            {
                "account_id": config.account_id,
                "external_id": DEMO_COMMENT_ID,
                "kind": "comment",
                "person_id": DEMO_PERSON_ID,
                "post_id": post_id,
                "comment_id": "demo-comment-record",
                "body": f"Please send the {config.keyword}",
                "occurred_at": clock.now(),
            },
            clock.now(),
        )
        _print_ingest(comment)
        await worker.run_until_idle()
        run = connection.execute(
            "SELECT boundary_at FROM runs WHERE account_id = ? AND person_id = ?",
            (config.account_id, DEMO_PERSON_ID),
        ).fetchone()
        occurred_at = add_seconds(run["boundary_at"], 1)
        message = accept_event(
            connection,
            config,
            {
                "account_id": config.account_id,
                "external_id": DEMO_MESSAGE_ID,
                "kind": "message",
                "person_id": DEMO_PERSON_ID,
                "body": "My email is reader@example.com",
                "occurred_at": occurred_at,
            },
            clock.now(),
        )
        _print_ingest(message)
        await worker.run_until_idle()
    else:
        print("demo events already exist; leaving stored rows unchanged")
    stats, lines = notify_once(connection, FakeOperatorNotifier(), clock)
    for line in lines:
        print(line)
    print(
        "notify_once claimed={claimed} delivered={delivered} retry={retry} "
        "timed_out={timed_out}".format(**stats)
    )
    print(render_status(connection, config, database))


def _print_ingest(result: dict) -> None:
    print(
        "ingest status={status} disposition={disposition} run_id={run_id} "
        "run_state={run_state}".format(
            status=result["status"],
            disposition=result["disposition"],
            run_id=result["run_id"],
            run_state=result["run_state"],
        )
    )


def _open(args: argparse.Namespace) -> tuple[ProjectConfig, sqlite3.Connection, str]:
    config_path = args.config or os.environ.get("CTI_CONFIG") or "config/project.json"
    database = args.database or os.environ.get("CTI_DATABASE") or "data/inbox.sqlite"
    config = load_config(config_path)
    connection = connect(database)
    init_db(connection)
    return config, connection, str(Path(database))


def _worker(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    clock: SystemClock | None = None,
    adapter_name: str = "fake",
) -> Worker:
    if adapter_name == "fake":
        adapter = FakeInstagram()
    elif adapter_name == "zernio":
        try:
            adapter = ZernioAdapter(config)
        except AdapterNotConfigured as exc:
            raise ValueError(str(exc)) from exc
    else:
        raise ValueError("adapter must be fake or zernio")
    return Worker(connection, config, adapter, clock or SystemClock())
