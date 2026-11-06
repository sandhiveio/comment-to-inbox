import logging
import threading

from comment_to_inbox.db import connect
from comment_to_inbox.notifying import (
    FakeOperatorNotifier,
    KnockOperatorNotifier,
    NotifierNotConfigured,
    NotifyTemporaryError,
    OperatorHandoffNotice,
    notify_once,
)
from comment_to_inbox.report import render_status

from tests.conftest import accept_comment, accept_message, after, drain, prime_awaiting_email


def _exhaust(world, body="still no address"):
    waiting = prime_awaiting_email(world)
    boundary = waiting["boundary_at"]
    last = None
    for index in range(1, 5):
        last = accept_message(
            world,
            external_id=f"nope-{index}",
            body=body if index == 4 else f"nope {index}",
            occurred_at=after(boundary, index),
        )
        if index < 4:
            assert drain(world) == "idle"
            boundary = world.conn.execute(
                "SELECT boundary_at FROM runs"
            ).fetchone()["boundary_at"]
    assert last["run_state"] == "exhausted"
    return last


def test_exhaustion_queues_one_handoff_and_a_duplicate_does_not(world):
    _exhaust(world, body="reach me somehow")
    handoffs = world.conn.execute("SELECT * FROM operator_handoffs").fetchall()
    notes = world.conn.execute("SELECT * FROM operator_notifications").fetchall()
    assert len(handoffs) == 1
    assert len(notes) == 1
    assert handoffs[0]["state"] == "active"
    assert handoffs[0]["reason"] == "email_exhausted"
    assert handoffs[0]["account_id"] == world.config.account_id
    assert notes[0]["handoff_id"] == handoffs[0]["id"]
    assert notes[0]["state"] == "queued"
    again = accept_message(
        world,
        external_id="nope-4",
        body="reach me somehow",
        occurred_at=after(world.clock.now(), 30),
    )
    assert again["status"] == "duplicate"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_handoffs").fetchone()["n"] == 1
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_notifications").fetchone()["n"] == 1


def test_delivery_failure_keeps_the_handoff_and_retries_the_same_id(world):
    _exhaust(world)
    handoff_id = world.conn.execute("SELECT id FROM operator_handoffs").fetchone()["id"]
    failing = FakeOperatorNotifier(["temporary"])
    stats, lines = notify_once(world.conn, failing, world.clock, batch_size=1, retry_delay_seconds=30)
    assert stats["retry"] == 1
    assert lines == []
    handoff = world.conn.execute("SELECT * FROM operator_handoffs").fetchone()
    note = world.conn.execute("SELECT * FROM operator_notifications").fetchone()
    assert handoff["state"] == "active"
    assert note["state"] == "queued"
    assert note["last_error"] == "NotifyTemporaryError"
    assert "secret" not in (note["last_error"] or "")
    assert note["next_attempt_at"] == "2026-01-01T00:00:30.000000Z"
    early, _ = notify_once(world.conn, FakeOperatorNotifier(), world.clock, batch_size=1)
    assert early["claimed"] == 0
    world.clock.advance(30)
    stats, lines = notify_once(world.conn, FakeOperatorNotifier(), world.clock, batch_size=1)
    assert stats["delivered"] == 1
    assert lines == [
        f"operator_handoff project=ig-test reason=email_exhausted conversation={handoff_id}"
    ]
    assert failing.calls[0].conversation_id == handoff_id
    assert world.conn.execute("SELECT state FROM operator_handoffs").fetchone()["state"] == "active"


def test_slow_notifier_does_not_block_instagram_or_another_ingest(world):
    _exhaust(world)
    started = threading.Event()
    release = threading.Event()

    class Blocking(FakeOperatorNotifier):
        def notify(self, notice):
            self.calls.append(notice)
            started.set()
            release.wait(2)
            raise NotifyTemporaryError("destination unavailable")

    def run() -> None:
        connection = connect(world.path)
        try:
            notify_once(connection, Blocking(), world.clock, batch_size=1, timeout_seconds=5)
        finally:
            connection.close()

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(2)
    accepted = accept_comment(
        world,
        person_id="person-2",
        external_id="comment-2",
        comment_id="ig-comment-2",
    )
    assert accepted["disposition"] == "accepted_comment"
    assert drain(world) == "idle"
    release.set()
    thread.join(3)
    assert world.conn.execute(
        "SELECT state FROM runs WHERE person_id = 'person-2'"
    ).fetchone()["state"] == "awaiting_email"
    note = world.conn.execute("SELECT state FROM operator_notifications").fetchone()
    assert note["state"] == "queued"


def test_printed_notice_omits_message_text_and_email(world, caplog):
    secret = "hidden.person@example.com"
    text = "call me tonight, no address yet"
    _exhaust(world, body=text)
    caplog.set_level(logging.ERROR)
    stats, lines = notify_once(
        world.conn, FakeOperatorNotifier(["temporary"]), world.clock, batch_size=1
    )
    assert stats["retry"] == 1
    rendered = "\n".join(lines)
    status = render_status(world.conn, world.config, str(world.path))
    logs = "\n".join(record.getMessage() for record in caplog.records)
    for blob in (rendered, status, logs):
        assert secret not in blob
        assert "call me tonight" not in blob
    assert "reason=email_exhausted" in status
    assert "handoff state=active count=1" in status


def test_timeout_leaves_the_notification_for_a_later_attempt(world):
    _exhaust(world)
    handoff_id = world.conn.execute("SELECT id FROM operator_handoffs").fetchone()["id"]
    release = threading.Event()

    class Hanger(FakeOperatorNotifier):
        def notify(self, notice):
            self.calls.append(notice)
            release.wait(2)

    stats, lines = notify_once(
        world.conn, Hanger(), world.clock, batch_size=1, timeout_seconds=0.4
    )
    assert stats["timed_out"] == 1
    assert lines == []
    row = world.conn.execute("SELECT state FROM operator_notifications").fetchone()
    assert row["state"] == "claimed"
    assert world.conn.execute("SELECT state FROM operator_handoffs").fetchone()["state"] == "active"
    release.set()
    world.clock.advance(30)
    stats, lines = notify_once(world.conn, FakeOperatorNotifier(), world.clock, batch_size=1)
    assert lines == [
        OperatorHandoffNotice("ig-test", "email_exhausted", handoff_id).render()
    ]
    assert stats["delivered"] == 1


def test_knock_adapter_is_not_configured():
    try:
        KnockOperatorNotifier().notify(
            OperatorHandoffNotice("ig-test", "email_exhausted", "conv-1")
        )
    except NotifierNotConfigured:
        return
    raise AssertionError("KnockOperatorNotifier ran without credentials")
