import sqlite3
import uuid

from comment_to_inbox.clock import add_seconds
from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.parsing import first_email
from comment_to_inbox.stats import record_event

# Re-requests allowed after the initial private reply.
MAX_EMAIL_RETRIES = 3
REASON_EMAIL_EXHAUSTED = "email_exhausted"
REASON_ESCALATION = "escalation"
REASON_PRIVATE_REPLY_FAILED = "private_reply_failed"
REASON_PRIVATE_REPLY_UNKNOWN = "private_reply_unknown"

ACTIVE_STATES = (
    "public_reply_pending",
    "email_request_pending",
    "awaiting_email",
    "link_dm_pending",
)


def channel_for(kind: str, slot: int) -> str:
    if kind == "public_reply":
        return "comment_reply"
    if kind == "link_dm":
        return "direct_message"
    if kind == "email_request":
        return "private_reply" if slot == 0 else "direct_message"
    raise ValueError(f"unknown action kind {kind}")


def opted_out(connection: sqlite3.Connection, account_id: str, person_id: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM opt_outs WHERE account_id = ? AND person_id = ?",
        (account_id, person_id),
    ).fetchone()
    return row is not None


def stop_person(
    connection: sqlite3.Connection,
    account_id: str,
    person_id: str,
    command: str,
    source_event_id: int,
    now: str,
) -> None:
    """Store the refusal, close an open run, and cancel queued sends."""
    connection.execute(
        """
        INSERT INTO opt_outs (account_id, person_id, command, source_event_id, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (account_id, person_id) DO NOTHING
        """,
        (account_id, person_id, command, source_event_id, now),
    )
    if connection.execute("SELECT changes()").fetchone()[0]:
        record_event(connection, account_id, "opt_out", now)
    cancel_queued(connection, account_id, person_id, now)
    connection.execute(
        """
        UPDATE lead_exports
        SET state = 'cancelled',
            claim_token = NULL,
            claim_expires_at = NULL,
            next_attempt_at = NULL,
            updated_at = ?
        WHERE state = 'queued'
          AND lead_id IN (
            SELECT id FROM leads WHERE account_id = ? AND person_id = ?
          )
        """,
        (now, account_id, person_id),
    )
    connection.execute(
        f"""
        UPDATE runs
        SET state = 'stopped', updated_at = ?
        WHERE account_id = ? AND person_id = ?
          AND state IN ({",".join("?" for _ in (*ACTIVE_STATES, "handed_off", "private_reply_failed"))})
        """,
        (now, account_id, person_id, *ACTIVE_STATES, "handed_off", "private_reply_failed"),
    )


def handoff_active(connection: sqlite3.Connection, account_id: str, person_id: str) -> bool:
    row = connection.execute(
        """
        SELECT 1 FROM operator_handoffs
        WHERE account_id = ? AND person_id = ? AND state = 'active'
        """,
        (account_id, person_id),
    ).fetchone()
    return row is not None


def escalate_person(
    connection: sqlite3.Connection,
    account_id: str,
    person_id: str,
    now: str,
) -> None:
    """Open one handoff, cancel unstarted sends, and stop automated replies.

    A second escalation while the handoff is active does not queue another
    notification. Clearing the handoff later does not revive cancelled actions.
    """
    cancel_queued(connection, account_id, person_id, now)
    connection.execute(
        f"""
        UPDATE runs
        SET state = 'handed_off', updated_at = ?
        WHERE account_id = ? AND person_id = ?
          AND state IN ({",".join("?" for _ in ACTIVE_STATES)})
        """,
        (now, account_id, person_id, *ACTIVE_STATES),
    )
    run = connection.execute(
        "SELECT * FROM runs WHERE account_id = ? AND person_id = ?",
        (account_id, person_id),
    ).fetchone()
    run_id = None if run is None else run["id"]
    _open_handoff(connection, account_id, person_id, run_id, REASON_ESCALATION, now)


def clear_handoff(
    connection: sqlite3.Connection, account_id: str, person_id: str, now: str
) -> int:
    """Mark the active handoff cleared. Runs, opt-outs, and actions stay as they are."""
    updated = connection.execute(
        """
        UPDATE operator_handoffs
        SET state = 'cleared', cleared_at = ?
        WHERE account_id = ? AND person_id = ? AND state = 'active'
        """,
        (now, account_id, person_id),
    )
    return updated.rowcount


def cancel_queued(
    connection: sqlite3.Connection, account_id: str, person_id: str, now: str
) -> None:
    connection.execute(
        """
        UPDATE outbound_actions
        SET state = 'cancelled', updated_at = ?
        WHERE account_id = ? AND person_id = ? AND state = 'queued'
        """,
        (now, account_id, person_id),
    )


def enqueue(
    connection: sqlite3.Connection,
    run: sqlite3.Row,
    kind: str,
    slot: int,
    body: str,
    now: str,
    config: ProjectConfig,
) -> None:
    connection.execute(
        """
        INSERT INTO outbound_actions (
            run_id, account_id, person_id, kind, slot, channel, state,
            not_before, deadline_at, body, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', NULL, ?, ?, ?, ?)
        """,
        (
            run["id"],
            run["account_id"],
            run["person_id"],
            kind,
            slot,
            channel_for(kind, slot),
            add_seconds(now, config.action_deadline_seconds),
            body,
            now,
            now,
        ),
    )


def set_disposition(connection: sqlite3.Connection, event_id: int, disposition: str) -> None:
    connection.execute(
        "UPDATE inbound_events SET disposition = ? WHERE id = ?",
        (disposition, event_id),
    )


def close_with_email(
    connection: sqlite3.Connection,
    run: sqlite3.Row,
    email: str,
    event_id: int,
    now: str,
    config: ProjectConfig,
) -> bool:
    """Close the wait if this transaction still owns it.

    The winning update also stores the lead, queues the link DM, and queues
    one lead export. A lost race or a duplicate inbound event leaves all
    three untouched. ``runs.email`` stays for existing readers.
    """
    updated = connection.execute(
        """
        UPDATE runs
        SET state = 'link_dm_pending', email = ?, email_event_id = ?, updated_at = ?
        WHERE id = ? AND state IN ('awaiting_email', 'email_request_pending')
        """,
        (email, event_id, now, run["id"]),
    )
    if updated.rowcount != 1:
        return False
    lead_id = str(uuid.uuid4())
    connection.execute(
        """
        INSERT INTO leads (
            id, run_id, account_id, person_id, email, post_id, accepted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            run["id"],
            run["account_id"],
            run["person_id"],
            email,
            run["post_id"],
            now,
        ),
    )
    connection.execute(
        """
        INSERT INTO lead_exports (
            lead_id, state, attempt_count, claim_token, claim_expires_at,
            next_attempt_at, last_error, created_at, updated_at
        ) VALUES (?, 'queued', 0, NULL, NULL, NULL, NULL, ?, ?)
        """,
        (lead_id, now, now),
    )
    enqueue(
        connection,
        run,
        "link_dm",
        0,
        config.pick_variant("link_dm", 0, run["id"]),
        now,
        config,
    )
    record_event(connection, run["account_id"], "email_accepted", now)
    return True


def schedule_invalid_reply(
    connection: sqlite3.Connection,
    run: sqlite3.Row,
    now: str,
    config: ProjectConfig,
) -> bool:
    """Consume one re-request cycle. The caller is inside the open wait."""
    count = int(run["invalid_replies"]) + 1
    if count <= MAX_EMAIL_RETRIES:
        updated = connection.execute(
            """
            UPDATE runs
            SET state = 'email_request_pending', invalid_replies = ?, updated_at = ?
            WHERE id = ? AND state = 'awaiting_email'
            """,
            (count, now, run["id"]),
        )
        if updated.rowcount != 1:
            return False
        enqueue(
            connection,
            run,
            "email_request",
            count,
            config.pick_variant("email_request", count, run["id"]),
            now,
            config,
        )
        return True
    updated = connection.execute(
        """
        UPDATE runs
        SET state = 'exhausted', invalid_replies = ?, updated_at = ?
        WHERE id = ? AND state = 'awaiting_email'
        """,
        (count, now, run["id"]),
    )
    if updated.rowcount != 1:
        return False
    _open_handoff(
        connection,
        run["account_id"],
        run["person_id"],
        run["id"],
        REASON_EMAIL_EXHAUSTED,
        now,
    )
    return True


def _open_handoff(
    connection: sqlite3.Connection,
    account_id: str,
    person_id: str,
    run_id: int | None,
    reason: str,
    now: str,
) -> None:
    """Store one active handoff and its queued notification together.

    An active handoff for this person suppresses a second alert. The insert
    is part of the caller's transaction.
    """
    if handoff_active(connection, account_id, person_id):
        return
    handoff_id = str(uuid.uuid4())
    connection.execute(
        """
        INSERT INTO operator_handoffs (
            id, run_id, account_id, person_id, reason, state, created_at, cleared_at
        ) VALUES (?, ?, ?, ?, ?, 'active', ?, NULL)
        """,
        (handoff_id, run_id, account_id, person_id, reason, now),
    )
    connection.execute(
        """
        INSERT INTO operator_notifications (
            handoff_id, state, attempt_count, claim_token, claim_expires_at,
            next_attempt_at, last_error, created_at, updated_at
        ) VALUES (?, 'queued', 0, NULL, NULL, NULL, NULL, ?, ?)
        """,
        (handoff_id, now, now),
    )
    record_event(connection, account_id, reason, now)


def apply_open_wait(
    connection: sqlite3.Connection, run_id: int, now: str, config: ProjectConfig
) -> None:
    """Apply parked replies that sit strictly after the current boundary.

    The earliest valid email wins, including when an earlier bubble in the
    same open window has no address. Those earlier bubbles do not consume a
    re-request. If the window has no email, only the earliest message
    consumes one re-request and the wait closes.
    """
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None or run["state"] != "awaiting_email" or not run["boundary_at"]:
        return
    if email_wait_expired(run, now):
        expire_email_waits(connection, now)
        return
    rows = connection.execute(
        """
        SELECT * FROM inbound_events
        WHERE account_id = ? AND person_id = ? AND kind = 'message'
          AND disposition = 'parked' AND occurred_at > ?
        ORDER BY occurred_at, id
        """,
        (run["account_id"], run["person_id"], run["boundary_at"]),
    ).fetchall()
    if not rows:
        return
    for index, row in enumerate(rows):
        email = first_email(row["body"])
        if email is None:
            continue
        won = close_with_email(connection, run, email, row["id"], now, config)
        set_disposition(connection, row["id"], "applied_email" if won else "lost_race")
        for earlier in rows[:index]:
            set_disposition(connection, earlier["id"], "applied_invalid")
        for later in rows[index + 1 :]:
            set_disposition(connection, later["id"], "ignored_after_close")
        return
    earliest = rows[0]
    set_disposition(connection, earliest["id"], "applied_invalid")
    fresh = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if fresh["state"] != "awaiting_email" or not schedule_invalid_reply(
        connection, fresh, now, config
    ):
        set_disposition(connection, earliest["id"], "lost_race")


def fold_previous_window(
    connection: sqlite3.Connection,
    run: sqlite3.Row,
    new_sent_at: str,
    now: str,
    config: ProjectConfig,
) -> bool:
    """Classify messages that arrived before this confirmation was stored.

    They belong to the previous request when
    ``old_boundary < occurred_at <= new_sent_at``. A valid email closes the
    run. Extra invalid bubbles do not consume another re-request: the
    re-request just confirmed is already the follow-up for that window.
    """
    old_boundary = run["boundary_at"]
    if not old_boundary:
        return False
    rows = connection.execute(
        """
        SELECT * FROM inbound_events
        WHERE account_id = ? AND person_id = ? AND kind = 'message'
          AND disposition = 'parked'
          AND occurred_at > ? AND occurred_at <= ?
        ORDER BY occurred_at, id
        """,
        (run["account_id"], run["person_id"], old_boundary, new_sent_at),
    ).fetchall()
    for index, row in enumerate(rows):
        email = first_email(row["body"])
        if email is None:
            continue
        won = close_with_email(connection, run, email, row["id"], now, config)
        if not won:
            set_disposition(connection, row["id"], "lost_race")
            return False
        for earlier in rows[:index]:
            set_disposition(connection, earlier["id"], "applied_invalid")
        set_disposition(connection, row["id"], "applied_email")
        connection.execute(
            """
            UPDATE inbound_events
            SET disposition = 'ignored_after_close'
            WHERE account_id = ? AND person_id = ? AND kind = 'message'
              AND disposition = 'parked'
            """,
            (run["account_id"], run["person_id"]),
        )
        return True
    for row in rows:
        set_disposition(connection, row["id"], "applied_invalid")
    return False


def mark_stale_messages(
    connection: sqlite3.Connection, account_id: str, person_id: str, sent_at: str
) -> None:
    connection.execute(
        """
        UPDATE inbound_events
        SET disposition = 'ignored_stale'
        WHERE account_id = ? AND person_id = ? AND kind = 'message'
          AND disposition = 'parked' AND occurred_at <= ?
        """,
        (account_id, person_id, sent_at),
    )


def open_email_wait(
    connection: sqlite3.Connection,
    run_id: int,
    sent_at: str,
    now: str,
    config: ProjectConfig,
) -> None:
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None or run["state"] in ("stopped", "handed_off") or opted_out(
        connection, run["account_id"], run["person_id"]
    ):
        return
    if handoff_active(connection, run["account_id"], run["person_id"]):
        return
    if run["state"] != "email_request_pending":
        return
    if fold_previous_window(connection, run, sent_at, now, config):
        return
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run["state"] != "email_request_pending":
        return
    mark_stale_messages(connection, run["account_id"], run["person_id"], sent_at)
    deadline = add_seconds(sent_at, config.email_wait_seconds)
    opened = connection.execute(
        """
        UPDATE runs
        SET state = 'awaiting_email',
            boundary_at = ?,
            email_deadline_at = COALESCE(email_deadline_at, ?),
            updated_at = ?
        WHERE id = ? AND state = 'email_request_pending'
        """,
        (sent_at, deadline, now, run_id),
    )
    if opened.rowcount != 1:
        return
    apply_open_wait(connection, run_id, now, config)


def email_wait_expired(run: sqlite3.Row, now: str) -> bool:
    deadline = run["email_deadline_at"] if "email_deadline_at" in run.keys() else None
    return bool(deadline) and deadline <= now


def expire_email_waits(connection: sqlite3.Connection, now: str) -> None:
    """Close email waits whose seven-day clock has finished. Re-asks do not extend it."""
    rows = connection.execute(
        """
        SELECT id, account_id, person_id FROM runs
        WHERE state = 'awaiting_email'
          AND email_deadline_at IS NOT NULL
          AND email_deadline_at <= ?
        """,
        (now,),
    ).fetchall()
    for row in rows:
        connection.execute(
            """
            UPDATE runs
            SET state = 'expired', updated_at = ?
            WHERE id = ? AND state = 'awaiting_email'
            """,
            (now, row["id"]),
        )
        if connection.execute("SELECT changes()").fetchone()[0]:
            record_event(connection, row["account_id"], "run_expired", now)
        connection.execute(
            """
            UPDATE outbound_actions
            SET state = 'cancelled', updated_at = ?
            WHERE run_id = ? AND state = 'queued' AND kind = 'email_request'
            """,
            (now, row["id"]),
        )


def enqueue_public_reply(
    connection: sqlite3.Connection,
    run_id: int,
    now: str,
    config: ProjectConfig,
    *,
    fallback: bool,
) -> None:
    """Queue one public reply. A failure of that reply does not send another private reply."""
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None or opted_out(connection, run["account_id"], run["person_id"]):
        return
    if handoff_active(connection, run["account_id"], run["person_id"]):
        return
    if run["state"] in ("stopped", "handed_off"):
        return
    pool_kind = "public_fallback" if fallback else "public_reply"
    if fallback:
        connection.execute(
            """
            UPDATE runs
            SET state = 'private_reply_failed', updated_at = ?
            WHERE id = ? AND state = 'email_request_pending'
            """,
            (now, run_id),
        )
        _open_handoff(
            connection,
            run["account_id"],
            run["person_id"],
            run["id"],
            REASON_PRIVATE_REPLY_FAILED,
            now,
        )
        if not config.public_fallback_variants:
            return
    existing = connection.execute(
        """
        SELECT 1 FROM outbound_actions
        WHERE run_id = ? AND kind = 'public_reply'
        """,
        (run_id,),
    ).fetchone()
    if existing is not None:
        return
    enqueue(
        connection,
        run,
        "public_reply",
        0,
        config.pick_variant(pool_kind, 0, run_id),
        now,
        config,
    )
    record_event(
        connection,
        run["account_id"],
        "public_fallback_queued" if fallback else "public_reply_queued",
        now,
    )


def enqueue_initial_email_request(
    connection: sqlite3.Connection, run_id: int, now: str, config: ProjectConfig
) -> None:
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None or opted_out(connection, run["account_id"], run["person_id"]):
        return
    if run["state"] in ("stopped", "handed_off"):
        return
    if handoff_active(connection, run["account_id"], run["person_id"]):
        return
    updated = connection.execute(
        """
        UPDATE runs
        SET state = 'email_request_pending', updated_at = ?
        WHERE id = ? AND state = 'public_reply_pending'
        """,
        (now, run_id),
    )
    if updated.rowcount != 1:
        return
    enqueue(
        connection,
        run,
        "email_request",
        0,
        config.pick_variant("email_request", 0, run["id"]),
        now,
        config,
    )


def complete_link(
    connection: sqlite3.Connection, run_id: int, now: str
) -> None:
    run = connection.execute(
        "SELECT account_id FROM runs WHERE id = ? AND state = 'link_dm_pending'",
        (run_id,),
    ).fetchone()
    updated = connection.execute(
        """
        UPDATE runs
        SET state = 'completed', updated_at = ?
        WHERE id = ? AND state = 'link_dm_pending'
        """,
        (now, run_id),
    )
    if updated.rowcount == 1 and run is not None:
        record_event(connection, run["account_id"], "link_confirmed", now)


def mark_private_reply_unknown(
    connection: sqlite3.Connection, run_id: int, now: str
) -> None:
    """Stop the run when the initial private reply has no confirmed outcome.

    The action stays ``unknown`` and is not retried. One operator handoff is
    opened. No public fallback is queued, because the private reply may
    already have been delivered.
    """
    run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None or opted_out(connection, run["account_id"], run["person_id"]):
        return
    if run["state"] in ("stopped", "handed_off", "private_reply_failed", "private_reply_unknown"):
        return
    updated = connection.execute(
        """
        UPDATE runs
        SET state = 'private_reply_unknown', updated_at = ?
        WHERE id = ? AND state = 'email_request_pending'
        """,
        (now, run_id),
    )
    if updated.rowcount != 1:
        return
    _open_handoff(
        connection,
        run["account_id"],
        run["person_id"],
        run["id"],
        REASON_PRIVATE_REPLY_UNKNOWN,
        now,
    )


def expire_run(connection: sqlite3.Connection, run_id: int, now: str) -> None:
    run = connection.execute(
        "SELECT account_id, state FROM runs WHERE id = ?", (run_id,)
    ).fetchone()
    updated = connection.execute(
        f"""
        UPDATE runs
        SET state = 'expired', updated_at = ?
        WHERE id = ? AND state IN ({",".join("?" for _ in ACTIVE_STATES)})
        """,
        (now, run_id, *ACTIVE_STATES),
    )
    if updated.rowcount == 1 and run is not None:
        record_event(connection, run["account_id"], "run_expired", now)
