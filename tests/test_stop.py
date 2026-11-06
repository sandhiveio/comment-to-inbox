from comment_to_inbox.db import connect
from comment_to_inbox.ingest import accept_event

from tests.conftest import (
    accept_comment,
    accept_message,
    action_rows,
    after,
    drain,
    prime_awaiting_email,
    run_row,
)


def test_exact_stop_cancels_unstarted_sends(world):
    waiting = prime_awaiting_email(world)
    calls_before = len(world.adapter.calls)
    queued = accept_message(
        world,
        external_id="email",
        body="reader@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert queued["run_state"] == "link_dm_pending"
    stopped = accept_message(
        world,
        external_id="stop",
        body="  UnSubscribe  ",
        occurred_at=after(waiting["boundary_at"], 2),
    )
    assert stopped["disposition"] == "applied_stop"
    assert run_row(world)["state"] == "stopped"
    link = [row for row in action_rows(world) if row["kind"] == "link_dm"][0]
    assert link["state"] == "cancelled"
    opt_out = world.conn.execute("SELECT command FROM opt_outs").fetchone()
    assert opt_out["command"] == "unsubscribe"
    assert drain(world) == "idle"
    assert len(world.adapter.calls) == calls_before


def test_stop_is_not_parsed_as_an_email_prefix(world):
    waiting = prime_awaiting_email(world)
    stopped = accept_message(
        world,
        external_id="exact-stop",
        body="STOP",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert stopped["disposition"] == "applied_stop"
    assert run_row(world)["email"] is None

    world.conn.execute("DELETE FROM opt_outs")
    world.conn.execute(
        "UPDATE runs SET state = 'awaiting_email', email = NULL WHERE id = ?",
        (run_row(world)["id"],),
    )
    mixed = accept_message(
        world,
        external_id="mixed",
        body="stop user@example.com",
        occurred_at=after(waiting["boundary_at"], 3),
    )
    assert mixed["disposition"] == "applied_email"
    assert mixed["email"] == "user@example.com"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM opt_outs").fetchone()["n"] == 0


def test_stop_during_private_reply_does_not_post_the_public_confirmation(world):
    def on_send(request):
        if request.kind != "email_request":
            return
        connection = connect(world.path)
        try:
            accept_event(
                connection,
                world.config,
                {
                    "account_id": world.config.account_id,
                    "external_id": "stop-now",
                    "kind": "message",
                    "person_id": "person-1",
                    "body": "STOP",
                    "occurred_at": "2026-01-01T00:00:01.000000Z",
                },
                world.clock.now(),
            )
        finally:
            connection.close()

    world.adapter.on_send = on_send
    accept_comment(world)
    assert drain(world) == "idle"
    assert [call.kind for call in world.adapter.calls] == ["email_request"]
    assert run_row(world)["state"] == "stopped"
    assert not any(row["kind"] == "public_reply" for row in action_rows(world))
    assert action_rows(world)[0]["state"] == "succeeded"


def test_worker_rechecks_opt_out_before_calling_the_adapter(world):
    accepted = accept_comment(world)
    world.conn.execute(
        """
        INSERT INTO opt_outs (account_id, person_id, command, source_event_id, created_at)
        VALUES (?, 'person-1', 'stop', ?, ?)
        """,
        (world.config.account_id, accepted["event_id"], world.clock.now()),
    )
    assert drain(world) == "idle"
    assert world.adapter.calls == []
    assert action_rows(world)[0]["state"] == "cancelled"
    assert run_row(world)["state"] == "stopped"


def test_opt_out_blocks_a_later_keyword_comment(world):
    stopped = accept_comment(world, external_id="stop-comment", body="STOP")
    assert stopped["disposition"] == "applied_stop"
    assert run_row(world) is None
    later = accept_comment(world, external_id="later", body="guide", comment_id="c-later")
    assert later["disposition"] == "ignored_opt_out"
    assert run_row(world) is None
