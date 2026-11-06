import asyncio
import time

from comment_to_inbox.adapter import OUTCOME_UNKNOWN, SendResult

from tests.conftest import accept_comment, action_rows, drain, replace_config, result_rows, run_row


def test_explicit_failure_is_retried_after_the_delay(world):
    world.adapter.script = ["explicit_failure", "success"]
    accept_comment(world)
    assert asyncio.run(world.worker.step()) == "sent"
    assert action_rows(world)[0]["state"] == "queued"
    assert result_rows(world)[0]["outcome"] == "explicit_failure"
    assert asyncio.run(world.worker.step()) == "idle"
    assert len(world.adapter.calls) == 1

    world.clock.advance(5)
    assert asyncio.run(world.worker.step()) == "sent"
    assert action_rows(world)[0]["state"] == "succeeded"
    assert [row["outcome"] for row in result_rows(world)] == ["explicit_failure", "success"]
    assert len(world.adapter.calls) == 2
    assert run_row(world)["state"] == "awaiting_email"


def test_unknown_result_is_not_retried(world):
    world.adapter.script = [OUTCOME_UNKNOWN]
    accept_comment(world)
    assert asyncio.run(world.worker.step()) == "sent"
    assert action_rows(world)[0]["state"] == "unknown"
    assert result_rows(world)[0]["outcome"] == "unknown"
    assert asyncio.run(world.worker.step()) == "idle"
    assert len(world.adapter.calls) == 1
    assert run_row(world)["state"] == "private_reply_unknown"
    assert len(action_rows(world)) == 1
    handoff = world.conn.execute("SELECT reason FROM operator_handoffs").fetchone()
    assert handoff["reason"] == "private_reply_unknown"


def test_timeout_is_unknown_and_is_not_retried(world):
    replace_config(world, send_timeout_seconds=0.05)

    async def hang(_request):
        await asyncio.sleep(5)

    world.adapter.on_send = hang
    accept_comment(world)
    assert asyncio.run(world.worker.step()) == "sent"
    assert action_rows(world)[0]["state"] == "unknown"
    assert result_rows(world)[0]["detail"] == "timeout"
    assert asyncio.run(world.worker.step()) == "idle"
    assert len(world.adapter.calls) == 1
    assert run_row(world)["state"] == "private_reply_unknown"


def test_restart_marks_an_unconfirmed_send_unknown(world):
    accept_comment(world)
    claimed = world.worker.claim()
    assert action_rows(world)[0]["state"] == "sending"
    assert result_rows(world)[0]["outcome"] == "pending"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 1

    world.worker.recover()
    assert action_rows(world)[0]["state"] == "unknown"
    assert result_rows(world)[0]["outcome"] == "unknown"
    assert result_rows(world)[0]["detail"] == "unconfirmed after restart"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 1
    assert asyncio.run(world.worker.step()) == "idle"
    assert world.adapter.calls == []
    assert claimed.request.kind == "email_request"
    assert run_row(world)["state"] == "private_reply_unknown"
    assert world.conn.execute("SELECT reason FROM operator_handoffs").fetchone()["reason"] == "private_reply_unknown"


def test_deadline_expires_a_queued_action_without_a_call(world):
    replace_config(world, action_deadline_seconds=0)
    accept_comment(world)
    assert asyncio.run(world.worker.step()) == "expired"
    assert world.adapter.calls == []
    assert action_rows(world)[0]["state"] == "expired"
    assert run_row(world)["state"] == "expired"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 0


def test_one_outbound_request_at_a_time(world):
    async def slow(_request):
        await asyncio.sleep(0.1)

    world.adapter.on_send = slow
    accept_comment(world, person_id="person-1", external_id="c1", comment_id="c1")
    accept_comment(world, person_id="person-2", external_id="c2", comment_id="c2")

    async def both():
        started = time.monotonic()
        await asyncio.gather(world.worker.step(), world.worker.step())
        return time.monotonic() - started

    elapsed = asyncio.run(both())
    assert world.worker.max_inflight == 1
    assert elapsed >= 0.18
    assert [call.kind for call in world.adapter.calls] == ["email_request", "email_request"]


def test_explicit_failure_result_object_is_distinct_from_unknown():
    assert SendResult.explicit_failure("nope").outcome == "explicit_failure"
    assert SendResult.unknown("lost").outcome == "unknown"
    assert SendResult.success("id", "2026-01-01T00:00:00.000000Z").outcome == "success"
