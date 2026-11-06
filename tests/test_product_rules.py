import json

from comment_to_inbox.clock import add_seconds
from comment_to_inbox.config import load_config
from comment_to_inbox.exporting import FakeLeadExporter, export_once
from comment_to_inbox.parsing import standalone_command
from comment_to_inbox.posts import DiscoveredPost, FakePostSource, sync_posts

from tests.conftest import (
    accept_comment,
    accept_message,
    action_rows,
    after,
    drain,
    prime_awaiting_email,
    replace_config,
    run_row,
)


def test_sync_posts_previews_then_adds_disabled_entries(tmp_path):
    path = tmp_path / "project.json"
    path.write_text(
        json.dumps(
            {
                "account_id": "ig-demo-alpha",
                "posts": [
                    {"id": "post-alpha-1", "url": "https://www.instagram.com/p/post-alpha-1/", "enabled": True}
                ],
            }
        ),
        encoding="utf-8",
    )
    source = FakePostSource(
        [
            [
                DiscoveredPost("post-alpha-1", "https://www.instagram.com/p/post-alpha-1/"),
                DiscoveredPost("post-new", "https://www.instagram.com/p/post-new/"),
            ],
            [DiscoveredPost("post-new", "https://www.instagram.com/p/post-new/")],
        ]
    )
    found, lines = sync_posts(path, source, apply=False)
    assert [post.id for post in found] == ["post-new"]
    assert any(line.startswith("sync_posts preview") for line in lines)
    assert "post-new" not in path.read_text(encoding="utf-8")
    found, lines = sync_posts(path, FakePostSource(source.pages), apply=True)
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["posts"][0]["enabled"] is True
    assert saved["posts"][1] == {
        "id": "post-new",
        "url": "https://www.instagram.com/p/post-new/",
        "enabled": False,
    }
    assert any(line.startswith("sync_posts applied") for line in lines)
    config = load_config(
        _full_config(tmp_path, saved["posts"])
    )
    assert "post-new" not in config.post_ids
    assert "post-alpha-1" in config.post_ids


def _full_config(tmp_path, posts):
    path = tmp_path / "full.json"
    path.write_text(
        json.dumps(
            {
                "account_id": "ig-demo-alpha",
                "comment_allowlist": ["pricing", "test"],
                "blocklist": ["stop", "unsubscribe"],
                "escalation_list": ["help"],
                "posts": posts,
                "public_reply_variants": ["Thanks."],
                "email_request_variants": ["Email?"],
                "email_retry_variants": ["Email again?"],
                "link_url": "https://example.com/alpha-guide",
                "link_dm_variants": ["Link https://example.com/alpha-guide"],
                "rate_limit_per_minute": 30,
                "send_timeout_seconds": 5,
                "retry_delay_seconds": 2,
                "action_deadline_seconds": 604800,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_disabled_and_early_comments_do_not_start_a_run(world, tmp_path):
    path = _full_config(
        tmp_path,
        [
            {
                "id": "post-1",
                "url": "https://www.instagram.com/p/post-1/",
                "enabled": True,
                "enabled_at": "2026-01-02T00:00:00.000000Z",
            },
            {"id": "post-new", "url": "https://www.instagram.com/p/post-new/", "enabled": False},
        ],
    )
    world.config = load_config(path)
    early = accept_comment(world, body="test", occurred_at="2026-01-01T00:00:00.000000Z")
    assert early["disposition"] == "ignored_before_enabled"
    assert run_row(world) is None
    hidden = accept_comment(
        world,
        external_id="hidden",
        comment_id="hidden",
        post_id="post-new",
        body="test",
        occurred_at="2026-01-03T00:00:00.000000Z",
    )
    assert hidden["disposition"] == "ignored_post"
    assert action_rows(world) == []


def test_standalone_opt_out_and_ambiguous_spam(world):
    assert standalone_command("STOP!", world.config.blocklist) == "stop"
    assert standalone_command("Is this spam?", world.config.blocklist) is None
    assert standalone_command("stop@example.com", world.config.blocklist) is None
    waiting = prime_awaiting_email(world)
    escalated = accept_message(
        world,
        external_id="spam-question",
        body="Is this spam?",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert escalated["disposition"] == "applied_escalation"
    assert run_row(world)["state"] == "handed_off"
    assert run_row(world)["invalid_replies"] == 0


def test_email_wait_expires_and_a_later_comment_does_not_restart(world):
    replace_config(world, email_wait_seconds=60, action_deadline_seconds=86400)
    waiting = prime_awaiting_email(world)
    deadline = waiting["email_deadline_at"]
    assert deadline == add_seconds(waiting["boundary_at"], 60)
    world.clock.advance(61)
    late = accept_message(
        world,
        external_id="late-email",
        body="late@example.com",
        occurred_at=after(deadline, -1),
    )
    assert late["disposition"] == "ignored_expired"
    assert run_row(world)["state"] == "expired"
    assert run_row(world)["email"] is None
    assert not any(row["kind"] == "link_dm" for row in action_rows(world))
    again = accept_comment(world, external_id="again", comment_id="again", body="guide")
    assert again["disposition"] == "ignored_existing_run"


def test_stop_cancels_a_pending_lead_export(world):
    waiting = prime_awaiting_email(world)
    accepted = accept_message(
        world,
        external_id="email-1",
        body="reader@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert accepted["disposition"] == "applied_email"
    stopped = accept_message(
        world,
        external_id="stop-export",
        body="UNSUBSCRIBE",
        occurred_at=after(waiting["boundary_at"], 2),
    )
    assert stopped["disposition"] == "applied_stop"
    export = world.conn.execute("SELECT state FROM lead_exports").fetchone()
    assert export["state"] == "cancelled"
    stats = export_once(world.conn, FakeLeadExporter(), world.clock)
    assert stats["claimed"] == 0
    assert world.conn.execute("SELECT state FROM lead_exports").fetchone()["state"] == "cancelled"


def test_private_reply_outcomes(world):
    world.adapter.script = ["permanent_failure"]
    accept_comment(world)
    assert drain(world) == "idle"
    actions = action_rows(world)
    assert actions[0]["kind"] == "email_request"
    assert actions[0]["state"] == "cancelled"
    assert actions[1]["kind"] == "public_reply"
    assert "couldn't message you" in actions[1]["body"]
    assert run_row(world)["state"] == "private_reply_failed"
    assert run_row(world)["email"] is None
    handoff = world.conn.execute(
        "SELECT reason, state FROM operator_handoffs"
    ).fetchone()
    assert handoff["reason"] == "private_reply_failed"
    assert handoff["state"] == "active"
    notification = world.conn.execute(
        "SELECT state FROM operator_notifications"
    ).fetchone()
    assert notification["state"] == "queued"
    counts = {
        row["event"]: row["count"]
        for row in world.conn.execute("SELECT event, count FROM daily_counts")
    }
    assert counts["accepted_comment"] == 1
    assert counts["private_reply_failed"] == 1
    assert counts["public_fallback_queued"] == 1
    assert counts["public_fallback_confirmed"] == 1

    replace_config(world)
    world.adapter.script = ["unknown"]
    world.adapter.calls.clear()
    accept_comment(world, person_id="person-2", external_id="c2", comment_id="c2")
    assert drain(world) == "idle"
    unknown = [row for row in action_rows(world) if row["person_id"] == "person-2"]
    assert len(unknown) == 1
    assert unknown[0]["state"] == "unknown"
    assert run_row(world, "person-2")["state"] == "private_reply_unknown"
    unknown_handoff = world.conn.execute(
        "SELECT reason, state FROM operator_handoffs WHERE person_id = 'person-2'"
    ).fetchone()
    assert unknown_handoff["reason"] == "private_reply_unknown"
    assert unknown_handoff["state"] == "active"
    assert not any(
        row["kind"] == "public_reply" and row["person_id"] == "person-2"
        for row in action_rows(world)
    )


def test_retryable_private_failure_does_not_send_the_fallback(world):
    world.adapter.script = ["explicit_failure", "success"]
    accept_comment(world)
    assert drain(world) == "idle"
    kinds = [row["kind"] for row in action_rows(world)]
    assert kinds == ["email_request"]
    world.clock.advance(5)
    assert drain(world) == "idle"
    kinds = [row["kind"] for row in action_rows(world)]
    assert kinds == ["email_request", "public_reply"]
    assert "couldn't message you" not in action_rows(world)[1]["body"]
    assert run_row(world)["state"] == "awaiting_email"
    confirmed = {
        row["event"]: row["count"]
        for row in world.conn.execute("SELECT event, count FROM daily_counts")
    }
    assert confirmed["public_reply_confirmed"] == 1
    assert confirmed.get("public_fallback_confirmed", 0) == 0


def test_public_reply_failure_does_not_block_email_or_repeat_the_private_reply(world):
    replace_config(world, retry_delay_seconds=0)
    world.adapter.script = ["success", "explicit_failure", "success"]
    waiting = prime_awaiting_email(world)
    private = [row for row in action_rows(world) if row["kind"] == "email_request"]
    assert len(private) == 1
    assert private[0]["state"] == "succeeded"
    reply = accept_message(
        world,
        external_id="email-1",
        body="reader@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert reply["disposition"] == "applied_email"
    assert drain(world) == "idle"
    assert run_row(world)["state"] == "completed"
    assert len([row for row in action_rows(world) if row["kind"] == "email_request"]) == 1
