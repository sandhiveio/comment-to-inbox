import asyncio
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from comment_to_inbox.adapter import FakeInstagram
from comment_to_inbox.clock import ManualClock, add_seconds
from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.db import connect, init_db
from comment_to_inbox.ingest import accept_event
from comment_to_inbox.worker import Worker


def make_config(**overrides) -> ProjectConfig:
    allow = overrides.pop("comment_allow", None)
    keyword = overrides.pop("keyword", "guide")
    values = {
        "account_id": "ig-test",
        "post_ids": ("post-1", "post-2"),
        "link_url": "https://example.com/guide",
        "rate_limit_per_minute": 100,
        "send_timeout_seconds": 5,
        "retry_delay_seconds": 5,
        "action_deadline_seconds": 86400,
        "comment_allow": allow or (keyword,),
        "blocklist": (
            "stop",
            "unsubscribe",
            "do not message me",
            "leave me alone",
        ),
        "escalation": (
            "what do i need to do",
            "what i need to do",
            "did not receive",
            "didn't receive",
            "didn't get it",
            "nothing arrived",
            "operator",
            "help",
            "spam",
        ),
        "public_reply_variants": ("Thanks. I sent you a message.",),
        "public_fallback_variants": (
            "We couldn't message you. If you'd still like the details, please send us a DM.",
        ),
        "email_request_variants": ("Reply with your email.",),
        "email_retry_variants": ("I still need an email address.",),
        "link_dm_variants": ("Here is the link: https://example.com/guide",),
    }
    for old, new in (
        ("public_reply_text", "public_reply_variants"),
        ("email_request_text", "email_request_variants"),
        ("email_retry_text", "email_retry_variants"),
        ("link_dm_text", "link_dm_variants"),
    ):
        if old in overrides:
            text = overrides.pop(old)
            values[new] = text if isinstance(text, tuple) else (text,)
    values.update(overrides)
    return ProjectConfig(**values)


@dataclass
class World:
    path: Path
    conn: sqlite3.Connection
    config: ProjectConfig
    clock: ManualClock
    adapter: FakeInstagram
    worker: Worker


@pytest.fixture
def world(tmp_path):
    database = tmp_path / "inbox.sqlite"
    config = make_config()
    connection = connect(database)
    init_db(connection)
    clock = ManualClock("2026-01-01T00:00:00.000000Z")
    adapter = FakeInstagram()
    holder = World(
        path=database,
        conn=connection,
        config=config,
        clock=clock,
        adapter=adapter,
        worker=Worker(connection, config, adapter, clock),
    )
    yield holder
    connection.close()


def replace_config(world: World, **overrides) -> None:
    world.config = make_config(**overrides)
    world.worker = Worker(world.conn, world.config, world.adapter, world.clock)


def accept(world: World, **event) -> dict:
    event.setdefault("account_id", world.config.account_id)
    return accept_event(world.conn, world.config, event, world.clock.now())


def accept_comment(
    world: World,
    *,
    person_id: str = "person-1",
    post_id: str = "post-1",
    external_id: str = "comment-1",
    comment_id: str = "ig-comment-1",
    body: str = "I need the guide",
    occurred_at: str | None = None,
) -> dict:
    return accept(
        world,
        external_id=external_id,
        kind="comment",
        person_id=person_id,
        post_id=post_id,
        comment_id=comment_id,
        body=body,
        occurred_at=occurred_at or world.clock.now(),
    )


def accept_message(
    world: World,
    *,
    person_id: str = "person-1",
    external_id: str,
    body: str,
    occurred_at: str,
    conversation_id: str = "",
) -> dict:
    return accept(
        world,
        external_id=external_id,
        kind="message",
        person_id=person_id,
        body=body,
        occurred_at=occurred_at,
        conversation_id=conversation_id,
    )


def drain(world: World) -> str:
    return asyncio.run(world.worker.run_until_idle())


def run_row(world: World, person_id: str = "person-1"):
    return world.conn.execute(
        "SELECT * FROM runs WHERE account_id = ? AND person_id = ?",
        (world.config.account_id, person_id),
    ).fetchone()


def action_rows(world: World):
    return world.conn.execute(
        "SELECT * FROM outbound_actions ORDER BY id"
    ).fetchall()


def result_rows(world: World):
    return world.conn.execute(
        "SELECT * FROM action_results ORDER BY id"
    ).fetchall()


def prime_awaiting_email(world: World, person_id: str = "person-1"):
    accepted = accept_comment(world, person_id=person_id)
    assert accepted["disposition"] == "accepted_comment"
    assert drain(world) == "idle"
    run = run_row(world, person_id)
    assert run["state"] == "awaiting_email"
    assert run["boundary_at"]
    return run


def after(boundary: str, seconds: float = 1) -> str:
    return add_seconds(boundary, seconds)
