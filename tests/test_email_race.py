import threading

from comment_to_inbox.db import connect
from comment_to_inbox.ingest import accept_event

from tests.conftest import accept_message, action_rows, after, prime_awaiting_email, run_row


def test_concurrent_replies_keep_one_email(world):
    waiting = prime_awaiting_email(world)
    occurred_at = after(waiting["boundary_at"], 5)
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def send(external_id: str, body: str) -> None:
        try:
            barrier.wait(timeout=5)
            connection = connect(world.path)
            try:
                results.append(
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
                )
            finally:
                connection.close()
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [
        threading.Thread(target=send, args=("email-a", "a@example.com")),
        threading.Thread(target=send, args=("email-b", "b@example.com")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    finished = run_row(world)
    assert finished["state"] == "link_dm_pending"
    assert finished["email"] in {"a@example.com", "b@example.com"}
    assert finished["invalid_replies"] == 0
    link_rows = [row for row in action_rows(world) if row["kind"] == "link_dm"]
    assert len(link_rows) == 1
    dispositions = {row["disposition"] for row in results}
    assert dispositions == {"applied_email", "ignored_after_close"}
    leads = world.conn.execute("SELECT email FROM leads").fetchall()
    assert [row["email"] for row in leads] == [finished["email"]]
    assert world.conn.execute("SELECT COUNT(*) AS n FROM lead_exports").fetchone()["n"] == 1


def test_duplicate_invalid_dm_does_not_consume_a_second_attempt(world):
    waiting = prime_awaiting_email(world)
    occurred_at = after(waiting["boundary_at"], 2)
    first = accept_message(
        world, external_id="nope-1", body="nope", occurred_at=occurred_at
    )
    second = accept_message(
        world, external_id="nope-1", body="nope", occurred_at=occurred_at
    )
    assert first["disposition"] == "applied_invalid"
    assert second["status"] == "duplicate"
    assert run_row(world)["invalid_replies"] == 1
    email_actions = [row for row in action_rows(world) if row["kind"] == "email_request"]
    assert [(row["slot"], row["state"]) for row in email_actions] == [
        (0, "succeeded"),
        (1, "queued"),
    ]
