import asyncio

from tests.conftest import accept_comment, action_rows, replace_config, run_row


def test_per_minute_limit_defers_until_the_next_slot(world):
    replace_config(world, rate_limit_per_minute=1)
    accept_comment(world)

    assert asyncio.run(world.worker.step()) == "sent"
    assert [call.kind for call in world.adapter.calls] == ["email_request"]
    public_reply = action_rows(world)[1]
    assert public_reply["kind"] == "public_reply"
    assert public_reply["state"] == "queued"

    assert asyncio.run(world.worker.step()) == "deferred"
    assert len(world.adapter.calls) == 1
    deferred = action_rows(world)[1]
    assert deferred["state"] == "queued"
    assert deferred["not_before"] == "2026-01-01T00:01:00.000000Z"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 1

    world.clock.advance(59)
    assert asyncio.run(world.worker.step()) == "idle"
    assert len(world.adapter.calls) == 1

    world.clock.advance(1)
    assert asyncio.run(world.worker.step()) == "sent"
    assert [call.kind for call in world.adapter.calls] == ["email_request", "public_reply"]
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 2
    assert run_row(world)["state"] == "awaiting_email"


def test_retry_consumes_budget_and_waits_for_a_free_slot(world):
    replace_config(world, rate_limit_per_minute=1, retry_delay_seconds=5)
    world.adapter.script = ["explicit_failure", "success"]
    accept_comment(world)

    assert asyncio.run(world.worker.step()) == "sent"
    failed = action_rows(world)[0]
    assert failed["state"] == "queued"
    assert failed["not_before"] == "2026-01-01T00:00:05.000000Z"
    assert len(world.adapter.calls) == 1

    world.clock.advance(5)
    assert asyncio.run(world.worker.step()) == "deferred"
    assert len(world.adapter.calls) == 1
    assert action_rows(world)[0]["not_before"] == "2026-01-01T00:01:00.000000Z"

    world.clock.advance(55)
    assert asyncio.run(world.worker.step()) == "sent"
    assert len(world.adapter.calls) == 2
    results = world.conn.execute(
        """
        SELECT outcome FROM action_results
        WHERE action_id = ?
        ORDER BY attempt_no
        """,
        (failed["id"],),
    ).fetchall()
    assert [row["outcome"] for row in results] == ["explicit_failure", "success"]
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 2

    assert asyncio.run(world.worker.step()) == "deferred"
    public_reply = action_rows(world)[1]
    assert public_reply["kind"] == "public_reply"
    assert public_reply["state"] == "queued"
    assert public_reply["not_before"] == "2026-01-01T00:02:00.000000Z"
