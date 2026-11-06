from pathlib import Path

from comment_to_inbox.config import load_config

from tests.conftest import accept_comment, accept_message, action_rows, after, drain, run_row


def test_keyword_comment_ends_with_the_link_dm(world):
    accepted = accept_comment(world, body="I need the guide")
    assert accepted["disposition"] == "accepted_comment"
    assert drain(world) == "idle"

    waiting = run_row(world)
    assert waiting["state"] == "awaiting_email"
    reply = accept_message(
        world,
        external_id="email-1",
        body="write a@example.com or b@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert reply["disposition"] == "applied_email"
    assert reply["email"] == "a@example.com"
    assert drain(world) == "idle"

    finished = run_row(world)
    assert finished["state"] == "completed"
    assert finished["email"] == "a@example.com"
    assert finished["invalid_replies"] == 0

    actions = action_rows(world)
    assert [(row["kind"], row["slot"], row["channel"], row["state"]) for row in actions] == [
        ("email_request", 0, "private_reply", "succeeded"),
        ("public_reply", 0, "comment_reply", "succeeded"),
        ("link_dm", 0, "direct_message", "succeeded"),
    ]
    assert actions[0]["body"] == world.config.email_request_text
    assert actions[1]["body"] == world.config.public_reply_text
    assert actions[2]["body"] == world.config.link_dm_text
    assert [call.kind for call in world.adapter.calls] == [
        "email_request",
        "public_reply",
        "link_dm",
    ]
    assert world.conn.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"] == 3
    lead = world.conn.execute("SELECT * FROM leads").fetchone()
    assert lead["email"] == "a@example.com"
    assert lead["post_id"] == "post-1"
    assert lead["person_id"] == "person-1"
    export = world.conn.execute("SELECT * FROM lead_exports").fetchone()
    assert export["lead_id"] == lead["id"]
    assert export["state"] == "queued"

    from comment_to_inbox.report import render_status

    status = render_status(world.conn, world.config, "test.sqlite")
    assert "a@example.com" not in status
    assert "email=yes" in status
    assert "export state=queued count=1" in status


def test_project_files_bind_different_accounts():
    root = Path(__file__).resolve().parents[1]
    alpha = load_config(root / "config" / "project.json")
    beta = load_config(root / "config" / "project-b.json")
    assert alpha.account_id == "ig-demo-alpha"
    assert beta.account_id == "ig-demo-beta"
    assert alpha.link_url != beta.link_url
    assert "test" in alpha.comment_allow and "pricing" in alpha.comment_allow
    assert "test" in beta.comment_allow and "pricing" in beta.comment_allow
    assert alpha.post_ids != beta.post_ids
