import json
import sqlite3
import urllib.error

import pytest

from comment_to_inbox.config import load_config
from comment_to_inbox.db import connect, init_db
from comment_to_inbox.dry_run import _describe, _get_ok
from comment_to_inbox.flow import clear_handoff
from comment_to_inbox.notifying import FakeOperatorNotifier, notify_once
from comment_to_inbox.parsing import comment_decision, phrase_matches

from tests.conftest import (
    accept_comment,
    accept_message,
    action_rows,
    after,
    drain,
    make_config,
    prime_awaiting_email,
    replace_config,
    run_row,
)


def test_phrase_rules_ignore_email_url_and_word_prefixes():
    assert phrase_matches("Pricing, STOP!", "stop")
    assert phrase_matches("PRICING — What I need to do next?", "what i need to do")
    assert phrase_matches("TEST!", "test")
    assert not phrase_matches("stopwatch", "stop")
    assert not phrase_matches("contest", "test")
    assert not phrase_matches("reach me at stop@example.com", "stop")
    assert not phrase_matches("see https://example.com/stop for details", "stop")


def test_allowlist_comment_runs_the_existing_flow(world):
    replace_config(world, comment_allow=("pricing", "test"))
    started = accept_comment(world, body="TEST!")
    assert started["disposition"] == "accepted_comment"
    assert drain(world) == "idle"
    waiting = run_row(world)
    assert waiting["state"] == "awaiting_email"
    reply = accept_message(
        world,
        external_id="email-test",
        body="qa.reader@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert reply["disposition"] == "applied_email"
    assert drain(world) == "idle"
    assert run_row(world)["state"] == "completed"
    assert [row["kind"] for row in action_rows(world)] == [
        "email_request",
        "public_reply",
        "link_dm",
    ]
    missed = accept_comment(
        world,
        person_id="person-2",
        external_id="contest",
        comment_id="contest-comment",
        body="I love this contest",
    )
    assert missed["disposition"] == "ignored_keyword"
    assert run_row(world, "person-2") is None


def test_escalation_beats_allowlist_and_does_not_require_a_run(world):
    replace_config(world, comment_allow=("pricing", "test"))
    plain = accept_comment(world, external_id="need", comment_id="c-need", body="What I need to do")
    assert plain["disposition"] == "applied_escalation"
    assert plain["run_id"] is None
    assert action_rows(world) == []
    longer = accept_comment(
        world,
        person_id="person-2",
        external_id="longer",
        comment_id="c-longer",
        body="test — WHAT I NEED TO DO next?",
    )
    assert longer["disposition"] == "applied_escalation"
    assert run_row(world, "person-2") is None
    assert action_rows(world) == []
    handoffs = world.conn.execute("SELECT * FROM operator_handoffs ORDER BY created_at").fetchall()
    notes = world.conn.execute("SELECT * FROM operator_notifications").fetchall()
    assert len(handoffs) == 2
    assert len(notes) == 2
    assert {row["reason"] for row in handoffs} == {"escalation"}
    assert all(row["run_id"] is None for row in handoffs)
    again = accept_comment(
        world,
        external_id="need-again",
        comment_id="c-need-2",
        body="what i need to do please",
    )
    assert again["disposition"] == "applied_escalation"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_notifications").fetchone()["n"] == 2


def test_keyword_typo_escalates_only_when_enabled(world):
    missed = accept_comment(world, external_id="typo-off", comment_id="c-typo-off", body="guid")
    assert missed["disposition"] == "ignored_keyword"
    replace_config(world, keyword="guide", keyword_typo_escalation=True)
    exact = accept_comment(world, external_id="exact", comment_id="c-exact", body="the GUIDE!")
    assert exact["disposition"] == "accepted_comment"
    typo = accept_comment(
        world,
        person_id="person-typo",
        external_id="typo-on",
        comment_id="c-typo-on",
        body="please send the giude",
    )
    assert typo["disposition"] == "applied_escalation"
    longer = accept_comment(
        world,
        person_id="person-book",
        external_id="book",
        comment_id="c-book",
        body="guidebook",
    )
    assert longer["disposition"] == "ignored_keyword"
    both = accept_comment(
        world,
        person_id="person-both",
        external_id="both",
        comment_id="c-both",
        body="guide and guid",
    )
    assert both["disposition"] == "accepted_comment"
    replace_config(world, keyword="ok", keyword_typo_escalation=True)
    short = accept_comment(
        world,
        person_id="person-short",
        external_id="short",
        comment_id="c-short",
        body="okk",
    )
    assert short["disposition"] == "ignored_keyword"


def test_dm_escalation_does_not_consume_a_reask(world):
    waiting = prime_awaiting_email(world)
    escalated = accept_message(
        world,
        external_id="need-dm",
        body="What I need to do",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert escalated["disposition"] == "applied_escalation"
    run = run_row(world)
    assert run["state"] == "handed_off"
    assert run["invalid_replies"] == 0
    assert run["email"] is None
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_handoffs").fetchone()["n"] == 1
    later = accept_message(
        world,
        external_id="email-after",
        body="reader@example.com",
        occurred_at=after(waiting["boundary_at"], 2),
    )
    assert later["disposition"] == "ignored_handoff"
    assert run_row(world)["invalid_replies"] == 0
    assert not any(row["kind"] == "link_dm" for row in action_rows(world))


def test_blocklist_in_comments_and_messages_sends_nothing(world):
    blocked = accept_comment(world, body="STOP!")
    assert blocked["disposition"] == "applied_stop"
    assert run_row(world) is None
    assert action_rows(world) == []
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_handoffs").fetchone()["n"] == 0
    started = accept_comment(
        world,
        person_id="person-2",
        external_id="comment-2",
        comment_id="ig-comment-2",
    )
    assert started["disposition"] == "accepted_comment"
    assert drain(world) == "idle"
    waiting = run_row(world, "person-2")
    stopped = accept_message(
        world,
        person_id="person-2",
        external_id="email-stop",
        body="stop@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert stopped["disposition"] == "applied_email"
    assert stopped["email"] == "stop@example.com"
    phrase = accept_message(
        world,
        person_id="person-3",
        external_id="leave",
        body="leave me alone!",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert phrase["disposition"] == "applied_stop"
    assert phrase["run_id"] is None


def test_duplicate_escalation_event_does_not_queue_a_second_notice(world):
    first = accept_comment(world, body="What I need to do")
    assert first["status"] == "ok"
    duplicate = accept_comment(world, body="What I need to do")
    assert duplicate["status"] == "duplicate"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM operator_handoffs").fetchone()["n"] == 1
    handoff_id = world.conn.execute("SELECT id FROM operator_handoffs").fetchone()["id"]
    failing = FakeOperatorNotifier(["temporary"])
    stats, lines = notify_once(world.conn, failing, world.clock, batch_size=1, retry_delay_seconds=30)
    assert stats["retry"] == 1
    assert lines == []
    assert world.conn.execute("SELECT state FROM operator_handoffs").fetchone()["state"] == "active"
    world.clock.advance(30)
    stats, lines = notify_once(world.conn, FakeOperatorNotifier(), world.clock, batch_size=1)
    assert stats["delivered"] == 1
    assert lines == [f"operator_handoff project=ig-test reason=escalation conversation={handoff_id}"]
    assert "What I need to do" not in lines[0]


def test_dry_run_matches_live_comment_classification(world):
    replace_config(world, comment_allow=("pricing", "test"), account_username="demo")
    samples = [
        ("TEST!", "accepted_comment", False),
        ("contest", "ignored_keyword", False),
        ("What I need to do", "applied_escalation", False),
        ("test — WHAT I NEED TO DO next?", "applied_escalation", False),
        ("STOP!", "applied_stop", False),
        ("Is this spam?", "applied_escalation", False),
        ("stop@example.com", "ignored_keyword", False),
        (world.config.public_reply_text, "ignored_echo", False),
    ]
    for index, (text, disposition, is_own) in enumerate(samples):
        decision, _phrase = comment_decision(
            text,
            is_own=is_own,
            echo_texts=world.config.public_reply_variants,
            blocklist=world.config.blocklist,
            escalation=world.config.escalation,
            allowlist=world.config.comment_allow,
        )
        described = _describe(
            {"from": {"id": f"p-{index}", "username": "reader"}, "message": text, "createdTime": "t"},
            world.config,
            account_username="demo",
            seen_people=set(),
            blocked_people=set(),
            escalated_people=set(),
        )
        accepted = accept_comment(
            world,
            person_id=f"person-{index}",
            external_id=f"sample-{index}",
            comment_id=f"c-{index}",
            body=text,
        )
        assert accepted["disposition"] == disposition
        if decision == "allow":
            assert "Would accept" in described
        elif decision == "escalate":
            assert "operator handoff" in described
        elif decision == "block":
            assert "Would block" in described
        elif decision == "echo":
            assert "outbound reply" in described
        else:
            assert "Ignored" in described


def test_dry_run_timeout_on_one_post_does_not_abort(monkeypatch, capsys):
    def timed_out(*_args, **_kwargs):
        raise urllib.error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr("comment_to_inbox.dry_run._get", timed_out)
    assert _get_ok("https://zernio.com/api/v1", "key", "/inbox/comments/post", {}) is None
    output = capsys.readouterr().out
    assert "could not be reached" in output
    assert "timed out" in output


def test_clear_handoff_keeps_the_run_rule_and_opt_out(world):
    replace_config(world, comment_allow=("pricing", "test"))
    accept_comment(world, body="What I need to do")
    accept_comment(
        world,
        person_id="person-2",
        external_id="stop-2",
        comment_id="c-stop",
        body="leave me alone",
    )
    from comment_to_inbox.db import transaction

    with transaction(world.conn):
        assert clear_handoff(world.conn, world.config.account_id, "person-1", world.clock.now()) == 1
    assert world.conn.execute(
        "SELECT state FROM operator_handoffs WHERE person_id = 'person-1'"
    ).fetchone()["state"] == "cleared"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM opt_outs").fetchone()["n"] == 1
    later = accept_comment(world, external_id="later-allow", comment_id="c-later", body="test")
    assert later["disposition"] == "accepted_comment"
    blocked = accept_comment(
        world,
        person_id="person-2",
        external_id="later-2",
        comment_id="c-later-2",
        body="test",
    )
    assert blocked["disposition"] == "ignored_opt_out"
    assert run_row(world, "person-2") is None
    assert action_rows(world) == [] or all(row["person_id"] != "person-2" for row in action_rows(world))


def test_clear_does_not_replay_cancelled_actions(world):
    accepted = accept_comment(world)
    assert accepted["disposition"] == "accepted_comment"
    escalated = accept_comment(
        world,
        external_id="escalate-open",
        comment_id="c-esc",
        body="operator",
    )
    assert escalated["disposition"] == "applied_escalation"
    assert action_rows(world)[0]["state"] == "cancelled"
    from comment_to_inbox.db import transaction

    with transaction(world.conn):
        clear_handoff(world.conn, world.config.account_id, "person-1", world.clock.now())
    assert drain(world) == "idle"
    assert world.adapter.calls == []
    assert action_rows(world)[0]["state"] == "cancelled"
    assert run_row(world)["state"] == "handed_off"
    again = accept_comment(world, external_id="second-post", post_id="post-2", comment_id="c2", body="guide")
    assert again["disposition"] == "ignored_existing_run"


def test_rendered_reply_is_stable_across_retries(world):
    replace_config(
        world,
        email_request_variants=("Alpha reply.", "Beta reply."),
    )
    world.adapter.script = ["explicit_failure", "success"]
    accept_comment(world)
    body = action_rows(world)[0]["body"]
    assert body in {"Alpha reply.", "Beta reply."}
    import asyncio

    assert asyncio.run(world.worker.step()) == "sent"
    assert world.adapter.calls[0].body == body
    replace_config(
        world,
        email_request_variants=("Changed after enqueue.",),
    )
    world.clock.advance(5)
    assert asyncio.run(world.worker.step()) == "sent"
    assert [call.body for call in world.adapter.calls] == [body, body]
    assert action_rows(world)[0]["body"] == body


def test_flat_config_and_old_volume_still_load(tmp_path):
    path = tmp_path / "flat.json"
    path.write_text(
        json.dumps(
            {
                "account_id": "ig-flat",
                "keyword": "guide",
                "blocklist": ["stop", "unsubscribe"],
                "escalation_list": ["help"],
                "post_ids": ["post-1"],
                "public_reply_text": "Thanks.",
                "email_request_text": "Email?",
                "email_retry_text": "Email again?",
                "link_url": "https://example.com/guide",
                "link_dm_text": "Link https://example.com/guide",
                "rate_limit_per_minute": 30,
                "send_timeout_seconds": 5,
                "retry_delay_seconds": 2,
                "action_deadline_seconds": 604800,
            }
        ),
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.comment_allow == ("guide",)
    assert config.public_reply_variants == ("Thanks.",)
    assert config.blocklist == ("stop", "unsubscribe")
    assert config.escalation == ("help",)
    assert config.keyword_typo_escalation is False

    missing = tmp_path / "missing-lists.json"
    missing.write_text(path.read_text(encoding="utf-8").replace('"blocklist": ["stop", "unsubscribe"],', ""), encoding="utf-8")
    with pytest.raises(ValueError, match="blocklist is required"):
        load_config(missing)
    assert config.link_dm_variants == ("Link https://example.com/guide",)

    database = tmp_path / "old.sqlite"
    raw = sqlite3.connect(database)
    raw.execute("CREATE TABLE runs (id INTEGER PRIMARY KEY, person_id TEXT NOT NULL)")
    raw.execute("INSERT INTO runs (id, person_id) VALUES (7, 'person-9')")
    raw.execute(
        """
        CREATE TABLE operator_handoffs (
            id TEXT PRIMARY KEY,
            run_id INTEGER NOT NULL UNIQUE,
            account_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    raw.execute(
        """
        INSERT INTO operator_handoffs (id, run_id, account_id, reason, state, created_at)
        VALUES ('handoff-old', 7, 'ig-flat', 'email_exhausted', 'active', '2026-01-01T00:00:00.000000Z')
        """
    )
    raw.commit()
    raw.close()
    connection = connect(database)
    init_db(connection)
    row = connection.execute("SELECT * FROM operator_handoffs").fetchone()
    assert row["id"] == "handoff-old"
    assert row["person_id"] == "person-9"
    assert row["state"] == "active"
    assert row["reason"] == "email_exhausted"
    connection.close()
    assert make_config().comment_allow == ("guide",)
