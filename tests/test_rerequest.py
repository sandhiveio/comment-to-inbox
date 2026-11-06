from tests.conftest import accept_message, action_rows, after, drain, run_row, prime_awaiting_email


def test_three_rerequests_then_stop_asking(world):
    waiting = prime_awaiting_email(world)
    boundary = waiting["boundary_at"]
    for index in range(1, 5):
        result = accept_message(
            world,
            external_id=f"nope-{index}",
            body=f"nope {index}",
            occurred_at=after(boundary, index),
        )
        if index < 4:
            assert result["run_state"] == "email_request_pending"
            assert drain(world) == "idle"
            boundary = run_row(world)["boundary_at"]
        else:
            assert result["disposition"] == "applied_invalid"
            assert result["run_state"] == "exhausted"

    finished = run_row(world)
    assert finished["state"] == "exhausted"
    assert finished["invalid_replies"] == 4
    assert finished["email"] is None
    email_actions = [row for row in action_rows(world) if row["kind"] == "email_request"]
    assert [row["slot"] for row in email_actions] == [0, 1, 2, 3]
    assert email_actions[0]["channel"] == "private_reply"
    assert email_actions[0]["body"] == world.config.email_request_text
    assert {row["channel"] for row in email_actions[1:]} == {"direct_message"}
    assert {row["body"] for row in email_actions[1:]} == {world.config.email_retry_text}
    assert all(row["state"] == "succeeded" for row in email_actions)
    assert not any(row["kind"] == "link_dm" for row in action_rows(world))
    assert drain(world) == "idle"
    assert len([call for call in world.adapter.calls if call.kind == "email_request"]) == 4
