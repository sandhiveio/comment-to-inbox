import sqlite3

from comment_to_inbox.clock import normalize_timestamp
from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.db import transaction
from comment_to_inbox.stats import record_event
from comment_to_inbox.flow import (
    apply_open_wait,
    email_wait_expired,
    enqueue,
    escalate_person,
    expire_email_waits,
    handoff_active,
    stop_person,
)
from comment_to_inbox.flow import opted_out as person_opted_out
from comment_to_inbox.parsing import (
    DECISION_ALLOW,
    DECISION_BLOCK,
    DECISION_ECHO,
    DECISION_ESCALATE,
    DECISION_ORDINARY,
    DECISION_OWN,
    comment_decision,
    message_decision,
)

COMMENT_FIELDS = ("account_id", "external_id", "kind", "person_id", "body", "occurred_at")


def accept_event(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    event: dict,
    now: str,
) -> dict:
    normalized = normalize_event(event, config)
    with transaction(connection):
        return _accept(connection, config, normalized, normalize_timestamp(now))


def normalize_event(event: dict, config: ProjectConfig) -> dict:
    missing = [field for field in COMMENT_FIELDS if not str(event.get(field, "")).strip()]
    if missing:
        raise ValueError(f"missing event fields: {', '.join(missing)}")
    if str(event["account_id"]) != config.account_id:
        raise ValueError(
            f"event account_id {event['account_id']} does not match project {config.account_id}"
        )
    kind = str(event["kind"])
    if kind not in ("comment", "message"):
        raise ValueError("kind must be comment or message")
    post_id = event.get("post_id")
    comment_id = event.get("comment_id")
    if kind == "comment" and (not post_id or not comment_id):
        raise ValueError("comment events require post_id and comment_id")
    return {
        "account_id": config.account_id,
        "external_id": str(event["external_id"]),
        "kind": kind,
        "person_id": str(event["person_id"]),
        "post_id": None if not post_id else str(post_id),
        "comment_id": None if not comment_id else str(comment_id),
        "body": str(event["body"]),
        "occurred_at": normalize_timestamp(str(event["occurred_at"])),
        "conversation_id": _conversation_id(event),
    }


def _conversation_id(event: dict) -> str | None:
    if str(event.get("kind")) != "message":
        return None
    value = str(event.get("conversation_id") or "").strip()
    return value or None


def _accept(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    event: dict,
    now: str,
) -> dict:
    existing = connection.execute(
        """
        SELECT id, disposition FROM inbound_events
        WHERE account_id = ? AND external_id = ?
        """,
        (event["account_id"], event["external_id"]),
    ).fetchone()
    if existing is not None:
        return _view(connection, existing["id"], "duplicate")

    connection.execute("SAVEPOINT insert_event")
    try:
        cursor = connection.execute(
            """
            INSERT INTO inbound_events (
                account_id, external_id, kind, person_id, post_id, comment_id,
                body, conversation_id, occurred_at, received_at, disposition
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received')
            """,
            (
                event["account_id"],
                event["external_id"],
                event["kind"],
                event["person_id"],
                event["post_id"],
                event["comment_id"],
                event["body"],
                event["conversation_id"],
                event["occurred_at"],
                now,
            ),
        )
    except sqlite3.IntegrityError:
        connection.execute("ROLLBACK TO SAVEPOINT insert_event")
        connection.execute("RELEASE SAVEPOINT insert_event")
        raced = connection.execute(
            """
            SELECT id FROM inbound_events
            WHERE account_id = ? AND external_id = ?
            """,
            (event["account_id"], event["external_id"]),
        ).fetchone()
        return _view(connection, raced["id"], "duplicate")
    else:
        connection.execute("RELEASE SAVEPOINT insert_event")

    event_id = int(cursor.lastrowid)
    if event["kind"] == "comment":
        _accept_comment(connection, config, event, event_id, now)
    else:
        _accept_message(connection, config, event, event_id, now)
    return _view(connection, event_id, "ok")


def _accept_comment(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    event: dict,
    event_id: int,
    now: str,
) -> None:
    if event["post_id"] not in config.post_ids:
        _set_disposition(connection, event_id, "ignored_post")
        return
    cutoff = dict(config.enabled_at_by_post).get(event["post_id"])
    if cutoff and event["occurred_at"] < cutoff:
        _set_disposition(connection, event_id, "ignored_before_enabled")
        return
    decision, phrase = comment_decision(
        event["body"],
        is_own=_is_own_comment(event, config),
        echo_texts=config.public_reply_variants,
        blocklist=config.blocklist,
        escalation=config.escalation,
        allowlist=config.comment_allow,
        keyword_typo_escalation=config.keyword_typo_escalation,
    )
    if decision == DECISION_OWN:
        _set_disposition(connection, event_id, "ignored_own")
        return
    if decision == DECISION_ECHO:
        _set_disposition(connection, event_id, "ignored_echo")
        return
    if decision == DECISION_BLOCK:
        stop_person(
            connection,
            event["account_id"],
            event["person_id"],
            phrase or "block",
            event_id,
            now,
        )
        _set_disposition(connection, event_id, "applied_stop")
        return
    if decision == DECISION_ESCALATE:
        escalate_person(connection, event["account_id"], event["person_id"], now)
        _set_disposition(connection, event_id, "applied_escalation")
        return
    if decision != DECISION_ALLOW:
        _set_disposition(connection, event_id, "ignored_keyword")
        return
    if person_opted_out(connection, event["account_id"], event["person_id"]):
        _set_disposition(connection, event_id, "ignored_opt_out")
        return
    if handoff_active(connection, event["account_id"], event["person_id"]):
        _set_disposition(connection, event_id, "ignored_handoff")
        return
    existing_run = connection.execute(
        "SELECT id FROM runs WHERE account_id = ? AND person_id = ?",
        (event["account_id"], event["person_id"]),
    ).fetchone()
    if existing_run is not None:
        _set_disposition(connection, event_id, "ignored_existing_run")
        return
    _create_run(connection, config, event, event_id, now)


def _create_run(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    event: dict,
    event_id: int,
    now: str,
) -> None:
    connection.execute("SAVEPOINT create_run")
    try:
        cursor = connection.execute(
            """
            INSERT INTO runs (
                account_id, person_id, source_event_id, post_id, comment_id,
                state, email, email_event_id, boundary_at, invalid_replies,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'email_request_pending', NULL, NULL, NULL, 0, ?, ?)
            """,
            (
                event["account_id"],
                event["person_id"],
                event_id,
                event["post_id"],
                event["comment_id"],
                now,
                now,
            ),
        )
        run = connection.execute(
            "SELECT * FROM runs WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
        enqueue(
            connection,
            run,
            "email_request",
            0,
            config.pick_variant("email_request", 0, int(run["id"])),
            now,
            config,
        )
    except sqlite3.IntegrityError:
        connection.execute("ROLLBACK TO SAVEPOINT create_run")
        connection.execute("RELEASE SAVEPOINT create_run")
        _set_disposition(connection, event_id, "ignored_existing_run")
        return
    connection.execute("RELEASE SAVEPOINT create_run")
    _set_disposition(connection, event_id, "accepted_comment")
    record_event(connection, event["account_id"], "accepted_comment", now)


def _accept_message(
    connection: sqlite3.Connection,
    config: ProjectConfig,
    event: dict,
    event_id: int,
    now: str,
) -> None:
    decision, phrase = message_decision(
        event["body"],
        blocklist=config.blocklist,
        escalation=config.escalation,
    )
    if decision == DECISION_BLOCK:
        stop_person(
            connection,
            event["account_id"],
            event["person_id"],
            phrase or "block",
            event_id,
            now,
        )
        _set_disposition(connection, event_id, "applied_stop")
        return
    if decision == DECISION_ESCALATE:
        escalate_person(connection, event["account_id"], event["person_id"], now)
        _set_disposition(connection, event_id, "applied_escalation")
        return
    if handoff_active(connection, event["account_id"], event["person_id"]):
        _set_disposition(connection, event_id, "ignored_handoff")
        return
    run = connection.execute(
        "SELECT * FROM runs WHERE account_id = ? AND person_id = ?",
        (event["account_id"], event["person_id"]),
    ).fetchone()
    if run is None:
        _set_disposition(connection, event_id, "ignored_no_run")
        return
    if run["state"] == "awaiting_email" and email_wait_expired(run, now):
        expire_email_waits(connection, now)
        _set_disposition(connection, event_id, "ignored_expired")
        return
    if run["state"] in ("public_reply_pending", "email_request_pending", "awaiting_email"):
        # Park first. An open wait is folded in this same transaction.
        # A send that is still unconfirmed stays parked until that
        # confirmation commits and classifies provider timestamps.
        _set_disposition(connection, event_id, "parked")
        if run["state"] == "awaiting_email":
            apply_open_wait(connection, run["id"], now, config)
        return
    _set_disposition(connection, event_id, "ignored_after_close")


def _is_own_comment(event: dict, config: ProjectConfig) -> bool:
    person = event["person_id"].casefold()
    if person == config.account_id.casefold():
        return True
    username = config.account_username.casefold()
    return bool(username) and person == username


def _set_disposition(connection: sqlite3.Connection, event_id: int, disposition: str) -> None:
    connection.execute(
        "UPDATE inbound_events SET disposition = ? WHERE id = ?",
        (disposition, event_id),
    )


def _view(connection: sqlite3.Connection, event_id: int, status: str) -> dict:
    event = connection.execute(
        "SELECT * FROM inbound_events WHERE id = ?", (event_id,)
    ).fetchone()
    run = connection.execute(
        "SELECT * FROM runs WHERE account_id = ? AND person_id = ?",
        (event["account_id"], event["person_id"]),
    ).fetchone()
    return {
        "status": status,
        "event_id": event["id"],
        "disposition": event["disposition"],
        "run_id": None if run is None else run["id"],
        "run_state": None if run is None else run["state"],
        "email": None if run is None else run["email"],
    }
