from comment_to_inbox.db import connect
from comment_to_inbox.ingest import accept_event

from tests.conftest import accept_comment, accept_message, action_rows, drain, run_row


SENT_AT = "2026-01-01T00:00:10.000000Z"


def _insert(world, external_id: str, body: str, occurred_at: str) -> None:
    connection = connect(world.path)
    try:
        accept_event(
            connection,
            world.config,
            {
                "account_id": world.config.account_id,
                "external_id": external_id,
                "kind": "message",
                "person_id": "person-1",
                "body": body,
                "occurred_at": occurred_at,
            },
            world.clock.now(),
        )
    finally:
        connection.close()


def test_dm_saved_during_send_is_reread_using_provider_time(world):
    observed = {}

    def on_send(request):
        if request.kind != "email_request":
            return
        connection = connect(world.path)
        try:
            observed["state"] = connection.execute(
                "SELECT state FROM outbound_actions WHERE id = ?",
                (request.action_id,),
            ).fetchone()["state"]
            _insert(world, "old-dm", "old@example.com", "2026-01-01T00:00:09.000000Z")
            _insert(world, "new-dm", "new@example.com", "2026-01-01T00:00:11.000000Z")
            observed["stored"] = (
                connection.execute(
                    "SELECT id FROM inbound_events WHERE external_id = 'new-dm'"
                ).fetchone()
                is not None
            )
        finally:
            connection.close()

    world.adapter.on_send = on_send
    world.adapter.fixed_sent_at = SENT_AT
    accept_comment(world)
    assert drain(world) == "idle"

    assert observed["state"] == "sending"
    assert observed["stored"] is True
    finished = run_row(world)
    assert finished["state"] == "completed"
    assert finished["email"] == "new@example.com"
    dispositions = {
        row["external_id"]: row["disposition"]
        for row in world.conn.execute("SELECT external_id, disposition FROM inbound_events")
    }
    assert dispositions["old-dm"] == "ignored_stale"
    assert dispositions["new-dm"] == "applied_email"
    assert [call.kind for call in world.adapter.calls] == [
        "email_request",
        "link_dm",
        "public_reply",
    ]


def test_first_valid_email_in_a_saved_burst_does_not_burn_a_retry(world):
    def on_send(request):
        if request.kind != "email_request":
            return
        _insert(world, "hello", "hello there", "2026-01-01T00:00:11.000000Z")
        _insert(
            world,
            "emails",
            "see a@example.com or b@example.com",
            "2026-01-01T00:00:12.000000Z",
        )

    world.adapter.on_send = on_send
    world.adapter.fixed_sent_at = SENT_AT
    accept_comment(world)
    assert drain(world) == "idle"
    finished = run_row(world)
    assert finished["email"] == "a@example.com"
    assert finished["invalid_replies"] == 0
    assert finished["state"] == "completed"
    slots = [row["slot"] for row in action_rows(world) if row["kind"] == "email_request"]
    assert slots == [0]


def test_email_saved_while_a_rerequest_is_in_flight_belongs_to_the_previous_ask(world):
    seen = {"email_requests": 0}

    def on_send(request):
        if request.kind != "email_request":
            return
        seen["email_requests"] += 1
        if seen["email_requests"] != 2:
            return
        world.adapter.fixed_sent_at = "2026-01-01T00:00:15.000000Z"
        _insert(world, "gap-email", "gap@example.com", "2026-01-01T00:00:12.000000Z")

    world.adapter.on_send = on_send
    from tests.conftest import prime_awaiting_email

    waiting = prime_awaiting_email(world)
    assert seen["email_requests"] == 1
    accept_message(
        world,
        external_id="nope",
        body="nope",
        occurred_at="2026-01-01T00:00:10.000000Z",
    )
    assert run_row(world)["state"] == "email_request_pending"
    assert waiting["boundary_at"] < "2026-01-01T00:00:10.000000Z"
    assert drain(world) == "idle"

    finished = run_row(world)
    assert finished["email"] == "gap@example.com"
    assert finished["invalid_replies"] == 1
    assert finished["state"] == "completed"
    email_slots = [row["slot"] for row in action_rows(world) if row["kind"] == "email_request"]
    assert email_slots == [0, 1]
    assert any(row["kind"] == "link_dm" and row["state"] == "succeeded" for row in action_rows(world))
