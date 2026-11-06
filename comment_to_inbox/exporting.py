"""Lead export queue.

The Instagram worker never calls this module. A slow or failing exporter
cannot hold the send transaction, because ``upsert`` runs only after the
claim transaction has committed.
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


class ExportTemporaryError(Exception):
    """The destination did not accept the lead, and a later attempt may."""


class ExportPermanentError(Exception):
    """The destination rejected the lead and a retry would not help."""


@dataclass(frozen=True)
class Lead:
    """Payload passed to every upsert attempt. ``lead_id`` is stable."""

    lead_id: str
    account_id: str
    person_id: str
    email: str
    post_id: str
    accepted_at: str


@dataclass(frozen=True)
class ClaimedExport:
    export_id: int
    token: str
    lead: Lead


class LeadExporter:
    """Narrow destination port. Implementations upsert by ``lead.lead_id``."""

    def upsert(self, lead: Lead) -> None:
        raise NotImplementedError


class FakeLeadExporter(LeadExporter):
    """Local stand-in with reproducible success and failure.

    ``script`` items are ``success``, ``temporary``, or ``permanent``.
    An empty script repeats success. The same ``lead_id`` may be presented
    again after a lost confirmation; this exporter has no other key.
    """

    def __init__(self, script: list[str] | None = None) -> None:
        self.script = list(script or [])
        self.calls: list[Lead] = []

    def upsert(self, lead: Lead) -> None:
        self.calls.append(lead)
        outcome = self.script.pop(0) if self.script else "success"
        if outcome == "success":
            return
        if outcome == "permanent":
            raise ExportPermanentError("permanent failure")
        raise ExportTemporaryError("temporary failure")


def export_once(
    connection: sqlite3.Connection,
    exporter: LeadExporter,
    clock,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> dict[str, int]:
    """Claim and export up to ``batch_size`` ready rows, then return."""
    stats = {"claimed": 0, "exported": 0, "retry": 0, "permanent": 0, "timed_out": 0}
    for _ in range(batch_size):
        claimed = claim_export(connection, clock, lease_seconds)
        if claimed is None:
            break
        stats["claimed"] += 1
        kind, error = _upsert_with_timeout(exporter, claimed.lead, timeout_seconds)
        if kind == "timeout":
            stats["timed_out"] += 1
            LOGGER.error(
                "lead export timed out lead_id=%s error_type=timeout",
                claimed.lead.lead_id,
            )
            continue
        if kind == "ok":
            wrote = finish_export(connection, claimed, "exported", clock, retry_delay_seconds, None)
            stats["exported" if wrote else "timed_out"] += 1
            continue
        assert error is not None
        error_type = type(error).__name__
        LOGGER.error(
            "lead export failed lead_id=%s error_type=%s",
            claimed.lead.lead_id,
            error_type,
        )
        if isinstance(error, ExportPermanentError):
            wrote = finish_export(
                connection, claimed, "permanent_failure", clock, retry_delay_seconds, error_type
            )
            stats["permanent" if wrote else "timed_out"] += 1
            continue
        wrote = finish_export(
            connection, claimed, "queued", clock, retry_delay_seconds, error_type
        )
        stats["retry" if wrote else "timed_out"] += 1
    return stats


def claim_export(
    connection: sqlite3.Connection, clock, lease_seconds: float
) -> ClaimedExport | None:
    """Take one ready or expired row. The lease token is the attempt id."""
    now = clock.now()
    from comment_to_inbox.db import transaction
    from comment_to_inbox.flow import opted_out

    with transaction(connection):
        while True:
            row = connection.execute(
                """
                SELECT e.id AS export_id, l.id AS lead_id, l.account_id, l.person_id,
                       l.email, l.post_id, l.accepted_at
                FROM lead_exports AS e
                JOIN leads AS l ON l.id = e.lead_id
                WHERE (
                    e.state = 'queued'
                    AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= ?)
                ) OR (
                    e.state = 'claimed' AND e.claim_expires_at <= ?
                )
                ORDER BY e.id
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            if not opted_out(connection, row["account_id"], row["person_id"]):
                break
            connection.execute(
                """
                UPDATE lead_exports
                SET state = 'cancelled',
                    claim_token = NULL,
                    claim_expires_at = NULL,
                    next_attempt_at = NULL,
                    updated_at = ?
                WHERE id = ? AND state IN ('queued', 'claimed')
                """,
                (now, row["export_id"]),
            )
        token = str(uuid.uuid4())
        expires = add_seconds(now, lease_seconds)
        updated = connection.execute(
            """
            UPDATE lead_exports
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
            (token, expires, now, row["export_id"], now, now),
        )
        if updated.rowcount != 1:
            return None
        lead = Lead(
            lead_id=row["lead_id"],
            account_id=row["account_id"],
            person_id=row["person_id"],
            email=row["email"],
            post_id=row["post_id"],
            accepted_at=row["accepted_at"],
        )
        return ClaimedExport(export_id=row["export_id"], token=token, lead=lead)


def finish_export(
    connection: sqlite3.Connection,
    claimed: ClaimedExport,
    state: str,
    clock,
    retry_delay_seconds: float,
    error_type: str | None,
) -> bool:
    """Write the outcome only while this attempt still owns the lease."""
    now = clock.now()
    from comment_to_inbox.db import transaction

    next_attempt = None
    if state == "queued":
        from comment_to_inbox.flow import opted_out

        if opted_out(connection, claimed.lead.account_id, claimed.lead.person_id):
            state = "cancelled"
        else:
            next_attempt = add_seconds(now, retry_delay_seconds)
    with transaction(connection):
        updated = connection.execute(
            """
            UPDATE lead_exports
            SET state = ?,
                claim_token = NULL,
                claim_expires_at = NULL,
                next_attempt_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ? AND claim_token = ? AND state = 'claimed'
            """,
            (state, next_attempt, error_type, now, claimed.export_id, claimed.token),
        )
        if updated.rowcount == 1 and state == "exported":
            from comment_to_inbox.stats import record_event

            record_event(connection, claimed.lead.account_id, "lead_exported", now)
    if updated.rowcount != 1:
        LOGGER.error(
            "lead export stale attempt ignored lead_id=%s error_type=stale_claim",
            claimed.lead.lead_id,
        )
        return False
    return True


def _upsert_with_timeout(exporter: LeadExporter, lead: Lead, timeout_seconds: float):
    box: dict = {}

    def run() -> None:
        try:
            exporter.upsert(lead)
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
    return "error", ExportTemporaryError("exporter ended without a result")
