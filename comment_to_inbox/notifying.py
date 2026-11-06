"""Deliver operator handoffs outside ingest and the Instagram worker.

The inbound transaction only inserts the handoff and a queued notification.
This module claims that row, calls ``OperatorNotifier``, then records the
outcome. A failure leaves the handoff active and the notification retryable.
"""

import logging
import sqlite3
import threading
import uuid
from dataclasses import dataclass

from comment_to_inbox.clock import add_seconds

LOGGER = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10
DEFAULT_LEASE_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = 5
DEFAULT_RETRY_DELAY_SECONDS = 30


class NotifierNotConfigured(RuntimeError):
    pass


class NotifyTemporaryError(Exception):
    """The handoff was not accepted. Retry later with the same conversation id."""


@dataclass(frozen=True)
class OperatorHandoffNotice:
    """Provider-neutral payload. No message text and no email address."""

    project: str
    reason: str
    conversation_id: str

    def render(self) -> str:
        return (
            f"operator_handoff project={self.project} reason={self.reason} "
            f"conversation={self.conversation_id}"
        )


@dataclass(frozen=True)
class ClaimedNotification:
    notification_id: int
    token: str
    notice: OperatorHandoffNotice


class OperatorNotifier:
    def notify(self, notice: OperatorHandoffNotice) -> None:
        raise NotImplementedError


class FakeOperatorNotifier(OperatorNotifier):
    """Local stand-in used by the Docker demo and tests. No credentials."""

    def __init__(self, script: list[str] | None = None) -> None:
        self.script = list(script or [])
        self.calls: list[OperatorHandoffNotice] = []

    def notify(self, notice: OperatorHandoffNotice) -> None:
        self.calls.append(notice)
        outcome = self.script.pop(0) if self.script else "success"
        if outcome == "success":
            return
        raise NotifyTemporaryError("destination unavailable")


class KnockOperatorNotifier(OperatorNotifier):
    """Future adapter for the Knock workflow ``operator-handoff``.

    The workflow receives the same three fields as ``OperatorHandoffNotice``:
    project, escalation reason, and conversation id. Channel selection
    (email, Slack, push) belongs to the Knock workflow configuration, not
    to this process. This class is not constructed by the demo.
    """

    def notify(self, notice: OperatorHandoffNotice) -> None:
        raise NotifierNotConfigured(
            "KnockOperatorNotifier requires a Knock workflow key"
        )


def notify_once(
    connection: sqlite3.Connection,
    notifier: OperatorNotifier,
    clock,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> tuple[dict[str, int], list[str]]:
    """Claim and deliver up to ``batch_size`` notices, then return."""
    stats = {"claimed": 0, "delivered": 0, "retry": 0, "timed_out": 0}
    lines: list[str] = []
    for _ in range(batch_size):
        claimed = claim_notification(connection, clock, lease_seconds)
        if claimed is None:
            break
        stats["claimed"] += 1
        kind, error = _notify_with_timeout(notifier, claimed.notice, timeout_seconds)
        if kind == "timeout":
            stats["timed_out"] += 1
            LOGGER.error(
                "operator notify timed out conversation=%s error_type=timeout",
                claimed.notice.conversation_id,
            )
            continue
        if kind == "ok":
            wrote = finish_notification(
                connection, claimed, "delivered", clock, retry_delay_seconds, None
            )
            if wrote:
                stats["delivered"] += 1
                lines.append(claimed.notice.render())
            else:
                stats["timed_out"] += 1
            continue
        assert error is not None
        error_type = type(error).__name__
        LOGGER.error(
            "operator notify failed conversation=%s error_type=%s",
            claimed.notice.conversation_id,
            error_type,
        )
        wrote = finish_notification(
            connection, claimed, "queued", clock, retry_delay_seconds, error_type
        )
        stats["retry" if wrote else "timed_out"] += 1
    return stats, lines


def claim_notification(
    connection: sqlite3.Connection, clock, lease_seconds: float
) -> ClaimedNotification | None:
    now = clock.now()
    from comment_to_inbox.db import transaction

    with transaction(connection):
        row = connection.execute(
            """
            SELECT n.id AS notification_id, h.id AS handoff_id, h.account_id, h.reason
            FROM operator_notifications AS n
            JOIN operator_handoffs AS h ON h.id = n.handoff_id
            WHERE (
                n.state = 'queued'
                AND (n.next_attempt_at IS NULL OR n.next_attempt_at <= ?)
            ) OR (
                n.state = 'claimed' AND n.claim_expires_at <= ?
            )
            ORDER BY n.id
            LIMIT 1
            """,
            (now, now),
        ).fetchone()
        if row is None:
            return None
        token = str(uuid.uuid4())
        expires = add_seconds(now, lease_seconds)
        updated = connection.execute(
            """
            UPDATE operator_notifications
            SET state = 'claimed',
                claim_token = ?,
                claim_expires_at = ?,
                attempt_count = attempt_count + 1,
                updated_at = ?
            WHERE id = ?
              AND (
                (state = 'queued' AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                OR (state = 'claimed' AND claim_expires_at <= ?)
              )
            """,
            (token, expires, now, row["notification_id"], now, now),
        )
        if updated.rowcount != 1:
            return None
        notice = OperatorHandoffNotice(
            project=row["account_id"],
            reason=row["reason"],
            conversation_id=row["handoff_id"],
        )
        return ClaimedNotification(
            notification_id=row["notification_id"], token=token, notice=notice
        )


def finish_notification(
    connection: sqlite3.Connection,
    claimed: ClaimedNotification,
    state: str,
    clock,
    retry_delay_seconds: float,
    error_type: str | None,
) -> bool:
    """Record this attempt only while its claim token is current.

    Delivery failure leaves the handoff ``active``.
    """
    now = clock.now()
    from comment_to_inbox.db import transaction

    next_attempt = None
    if state == "queued":
        next_attempt = add_seconds(now, retry_delay_seconds)
    with transaction(connection):
        updated = connection.execute(
            """
            UPDATE operator_notifications
            SET state = ?,
                claim_token = NULL,
                claim_expires_at = NULL,
                next_attempt_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ? AND claim_token = ? AND state = 'claimed'
            """,
            (
                state,
                next_attempt,
                error_type,
                now,
                claimed.notification_id,
                claimed.token,
            ),
        )
        if updated.rowcount != 1:
            return False
    return True


def _notify_with_timeout(notifier: OperatorNotifier, notice: OperatorHandoffNotice, timeout_seconds: float):
    box: dict = {}

    def run() -> None:
        try:
            notifier.notify(notice)
        except Exception as exc:
            box["error"] = exc
        else:
            box["ok"] = True

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        return "timeout", None
    if "error" in box:
        return "error", box["error"]
    if box.get("ok"):
        return "ok", None
    return "error", NotifyTemporaryError("notifier ended without a result")
