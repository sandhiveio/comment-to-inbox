import asyncio
import logging
import sqlite3
from dataclasses import dataclass

from comment_to_inbox.adapter import (
    OUTCOME_EXPLICIT_FAILURE,
    OUTCOME_SUCCESS,
    OUTCOME_UNKNOWN,
    PERMANENT_FAILURE_DETAIL,
    ExplicitFailure,
    OutboundRequest,
    SendResult,
)
from comment_to_inbox.clock import add_seconds, seconds_between
from comment_to_inbox.clock import normalize_timestamp
from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.inbox_poll import INBOX_POLL_SECONDS, apply_events
from comment_to_inbox.db import transaction
from comment_to_inbox.stats import record_event
from comment_to_inbox.flow import (
    cancel_queued,
    complete_link,
    enqueue_public_reply,
    mark_private_reply_unknown,
    expire_email_waits,
    expire_run,
    open_email_wait,
    opted_out,
)

COMMENT_WINDOW_SECONDS = 7 * 24 * 3600
DM_WINDOW_SECONDS = 24 * 3600

LOGGER = logging.getLogger(__name__)
RATE_WINDOW_SECONDS = 60
POLL_SECONDS = 0.25


@dataclass(frozen=True)
class ClaimedSend:
    request: OutboundRequest
    run_id: int


class Worker:
    """One async sender. The adapter is called outside any database transaction.

    The asyncio lock is process-local. It is enough only because one active
    container owns the account. A second container has its own lock, its own
    worker, and its own database.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        config: ProjectConfig,
        adapter,
        clock,
    ) -> None:
        self.connection = connection
        self.config = config
        self.adapter = adapter
        self.clock = clock
        self._lock: asyncio.Lock | None = None
        self._lock_loop = None
        self.inflight = 0
        self.max_inflight = 0
        self.inbox = None

    def recover(self) -> None:
        """Treat an interrupted send as unknown. Do not call the adapter again."""
        now = self.clock.now()
        with transaction(self.connection):
            interrupted = self.connection.execute(
                """
                SELECT * FROM outbound_actions WHERE state = 'sending'
                """
            ).fetchall()
            self.connection.execute(
                """
                UPDATE action_results
                SET outcome = 'unknown',
                    detail = 'unconfirmed after restart',
                    finished_at = ?
                WHERE outcome = 'pending'
                """,
                (now,),
            )
            self.connection.execute(
                """
                UPDATE outbound_actions
                SET state = 'unknown', updated_at = ?
                WHERE state = 'sending'
                """,
                (now,),
            )
            for action in interrupted:
                if action["kind"] == "email_request" and action["slot"] == 0:
                    mark_private_reply_unknown(self.connection, action["run_id"], now)

    async def serve(self) -> None:
        import signal

        self.recover()
        LOGGER.info("worker started account_id=%s", self.config.account_id)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_signum, _frame: stop.set())
        next_poll = 0.0
        while not stop.is_set():
            if self.inbox is not None and loop.time() >= next_poll:
                await self._poll_inbox()
                next_poll = loop.time() + INBOX_POLL_SECONDS
            outcome = await self.step()
            if stop.is_set() or outcome not in ("idle", "deferred"):
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.idle_sleep_seconds())
            except TimeoutError:
                pass
        LOGGER.info("worker stopped account_id=%s", self.config.account_id)

    async def _poll_inbox(self) -> None:
        try:
            events = await asyncio.to_thread(self.inbox.collect)
        except Exception as exc:
            LOGGER.error("inbox poll failed: %s", exc)
            return
        accepted = apply_events(self.connection, self.config, events, self.clock.now())
        if accepted:
            LOGGER.info("inbox poll accepted=%s", accepted)

    async def run_until_idle(self, max_steps: int = 30) -> str:
        outcome = "idle"
        for _ in range(max_steps):
            outcome = await self.step()
            if outcome in ("idle", "deferred"):
                return outcome
        raise RuntimeError("worker did not become idle")

    def idle_sleep_seconds(self) -> float:
        row = self.connection.execute(
            """
            SELECT MIN(not_before) AS due
            FROM outbound_actions
            WHERE state = 'queued' AND not_before IS NOT NULL
            """
        ).fetchone()
        if row["due"] is None:
            return POLL_SECONDS
        delay = seconds_between(self.clock.now(), row["due"])
        if delay <= 0:
            return 0.01
        return min(delay, POLL_SECONDS)

    def _sender_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._lock is None or self._lock_loop is not loop:
            self._lock = asyncio.Lock()
            self._lock_loop = loop
        return self._lock

    async def step(self) -> str:
        async with self._sender_lock():
            claimed = self.claim()
            if not isinstance(claimed, ClaimedSend):
                return claimed
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            try:
                result = await self._call_adapter(claimed.request)
            finally:
                self.inflight -= 1
            self.record(claimed, result)
            LOGGER.info(
                "send action_id=%s kind=%s channel=%s outcome=%s detail=%s",
                claimed.request.action_id,
                claimed.request.kind,
                claimed.request.channel,
                result.outcome,
                result.detail or "",
            )
            return "sent"

    async def _call_adapter(self, request: OutboundRequest) -> SendResult:
        try:
            result = await asyncio.wait_for(
                self.adapter.send(request),
                timeout=self.config.send_timeout_seconds,
            )
        except TimeoutError:
            return SendResult.unknown("timeout")
        except ExplicitFailure as exc:
            return SendResult.explicit_failure(str(exc))
        except Exception as exc:
            LOGGER.error(
                "adapter error action_id=%s; marking unknown: %s",
                request.action_id,
                exc,
            )
            return SendResult.unknown(f"{type(exc).__name__}: {exc}")
        if result.outcome not in (OUTCOME_SUCCESS, OUTCOME_EXPLICIT_FAILURE, OUTCOME_UNKNOWN):
            return SendResult.unknown(f"unrecognized outcome {result.outcome}")
        return result

    def claim(self) -> ClaimedSend | str:
        now = self.clock.now()
        with transaction(self.connection):
            expire_email_waits(self.connection, now)
            row = self.connection.execute(
                """
                SELECT a.*, r.comment_id, r.post_id, r.state AS run_state
                FROM outbound_actions AS a
                JOIN runs AS r ON r.id = a.run_id
                WHERE a.state = 'queued'
                  AND (a.not_before IS NULL OR a.not_before <= ?)
                ORDER BY a.id
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return "idle"
            if row["deadline_at"] <= now:
                self._expire_locked(row, now)
                return "expired"
            if opted_out(self.connection, row["account_id"], row["person_id"]):
                cancel_queued(self.connection, row["account_id"], row["person_id"], now)
                self.connection.execute(
                    """
                    UPDATE runs
                    SET state = 'stopped', updated_at = ?
                    WHERE id = ? AND state IN (
                        'public_reply_pending', 'email_request_pending',
                        'awaiting_email', 'link_dm_pending', 'handed_off'
                    )
                    """,
                    (now, row["run_id"]),
                )
                return "cancelled"
            if row["run_state"] == "handed_off":
                cancel_queued(self.connection, row["account_id"], row["person_id"], now)
                return "cancelled"
            if self._messaging_window_closed(row, now):
                self._expire_for_window(row, now)
                return "expired"
            blocked_until = self._rate_limit_until(row["account_id"], now)
            if blocked_until is not None:
                self.connection.execute(
                    """
                    UPDATE outbound_actions
                    SET not_before = ?, updated_at = ?
                    WHERE id = ? AND state = 'queued'
                    """,
                    (blocked_until, now, row["id"]),
                )
                return "deferred"
            attempt_no = self.connection.execute(
                """
                SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next_attempt
                FROM action_results
                WHERE action_id = ?
                """,
                (row["id"],),
            ).fetchone()["next_attempt"]
            claimed = self.connection.execute(
                """
                UPDATE outbound_actions
                SET state = 'sending', updated_at = ?
                WHERE id = ? AND state = 'queued'
                """,
                (now, row["id"]),
            )
            if claimed.rowcount != 1:
                return "idle"
            self.connection.execute(
                """
                INSERT INTO action_results (
                    action_id, attempt_no, outcome, detail, started_at, finished_at
                ) VALUES (?, ?, 'pending', NULL, ?, NULL)
                """,
                (row["id"], attempt_no, now),
            )
            self.connection.execute(
                """
                INSERT INTO budget_spend (account_id, action_id, attempt_no, spent_at)
                VALUES (?, ?, ?, ?)
                """,
                (row["account_id"], row["id"], attempt_no, now),
            )
            request = OutboundRequest(
                action_id=row["id"],
                attempt_no=attempt_no,
                kind=row["kind"],
                channel=row["channel"],
                account_id=row["account_id"],
                person_id=row["person_id"],
                comment_id=row["comment_id"],
                post_id=row["post_id"],
                body=row["body"],
                attempted_at=now,
                conversation_id=self._conversation_id(row),
            )
            return ClaimedSend(request=request, run_id=row["run_id"])

    def record(self, claimed: ClaimedSend, result: SendResult) -> None:
        now = self.clock.now()
        request = claimed.request
        with transaction(self.connection):
            action = self.connection.execute(
                "SELECT * FROM outbound_actions WHERE id = ?",
                (request.action_id,),
            ).fetchone()
            if action is None or action["state"] != "sending":
                self.connection.execute(
                    """
                    UPDATE action_results
                    SET outcome = 'unknown',
                        detail = 'result arrived after the attempt was closed',
                        finished_at = ?
                    WHERE action_id = ? AND attempt_no = ? AND outcome = 'pending'
                    """,
                    (now, request.action_id, request.attempt_no),
                )
                return
            self.connection.execute(
                """
                UPDATE action_results
                SET outcome = ?, detail = ?, finished_at = ?
                WHERE action_id = ? AND attempt_no = ? AND outcome = 'pending'
                """,
                (
                    result.outcome,
                    result.detail or None,
                    now,
                    request.action_id,
                    request.attempt_no,
                ),
            )
            if result.outcome == OUTCOME_SUCCESS:
                sent_at = result.sent_at or request.attempted_at
                sent_at = normalize_timestamp(sent_at)
                confirmed = self.connection.execute(
                    """
                    UPDATE outbound_actions
                    SET state = 'succeeded',
                        provider_message_id = ?,
                        sent_at = ?,
                        updated_at = ?
                    WHERE id = ? AND state = 'sending'
                    """,
                    (result.provider_message_id, sent_at, now, request.action_id),
                )
                run_state = self.connection.execute(
                    "SELECT state FROM runs WHERE id = ?", (action["run_id"],)
                ).fetchone()
                self._advance(action, sent_at, now)
                if confirmed.rowcount == 1:
                    self._count_confirmed_send(action, run_state, now)
                return
            if result.outcome == OUTCOME_EXPLICIT_FAILURE:
                if (
                    result.detail == PERMANENT_FAILURE_DETAIL
                    and action["kind"] == "email_request"
                    and action["slot"] == 0
                ):
                    self._permanent_private_reply(action, now)
                    return
                self._requeue_or_expire(action, now)
                return
            self.connection.execute(
                """
                UPDATE outbound_actions
                SET state = 'unknown', updated_at = ?
                WHERE id = ? AND state = 'sending'
                """,
                (now, request.action_id),
            )
            if action["kind"] == "email_request" and action["slot"] == 0:
                mark_private_reply_unknown(self.connection, action["run_id"], now)

    def _count_confirmed_send(self, action: sqlite3.Row, run_state, now: str) -> None:
        """Count one successful send. A later retry of a failed attempt is a new success."""
        if action["kind"] == "email_request" and action["slot"] == 0:
            record_event(self.connection, action["account_id"], "private_reply_confirmed", now)
            return
        if action["kind"] != "public_reply":
            return
        state = "" if run_state is None else run_state["state"]
        event = (
            "public_fallback_confirmed"
            if state == "private_reply_failed"
            else "public_reply_confirmed"
        )
        record_event(self.connection, action["account_id"], event, now)

    def _advance(self, action: sqlite3.Row, sent_at: str, now: str) -> None:
        if opted_out(self.connection, action["account_id"], action["person_id"]):
            cancel_queued(self.connection, action["account_id"], action["person_id"], now)
            self.connection.execute(
                """
                UPDATE runs
                SET state = 'stopped', updated_at = ?
                WHERE id = ? AND state IN (
                    'public_reply_pending', 'email_request_pending',
                    'awaiting_email', 'link_dm_pending', 'handed_off'
                )
                """,
                (now, action["run_id"]),
            )
            return
        if action["kind"] == "public_reply":
            return
        if action["kind"] == "email_request":
            open_email_wait(self.connection, action["run_id"], sent_at, now, self.config)
            if action["slot"] == 0:
                enqueue_public_reply(
                    self.connection, action["run_id"], now, self.config, fallback=False
                )
            return
        if action["kind"] == "link_dm":
            complete_link(self.connection, action["run_id"], now)

    def _permanent_private_reply(self, action: sqlite3.Row, now: str) -> None:
        self.connection.execute(
            """
            UPDATE outbound_actions
            SET state = 'cancelled', updated_at = ?
            WHERE id = ? AND state = 'sending'
            """,
            (now, action["id"]),
        )
        if opted_out(self.connection, action["account_id"], action["person_id"]):
            return
        run = self.connection.execute(
            "SELECT state FROM runs WHERE id = ?", (action["run_id"],)
        ).fetchone()
        if run is None or run["state"] in ("stopped", "handed_off"):
            return
        enqueue_public_reply(
            self.connection, action["run_id"], now, self.config, fallback=True
        )

    def _conversation_id(self, row: sqlite3.Row) -> str:
        """Latest inbound conversation id for this person. Empty when none was supplied."""
        if row["channel"] != "direct_message":
            return ""
        found = self.connection.execute(
            """
            SELECT conversation_id
            FROM inbound_events
            WHERE account_id = ? AND person_id = ? AND kind = 'message'
              AND conversation_id IS NOT NULL AND conversation_id != ''
            ORDER BY occurred_at DESC, id DESC
            LIMIT 1
            """,
            (row["account_id"], row["person_id"]),
        ).fetchone()
        if found is None:
            return ""
        return str(found["conversation_id"])

    def _messaging_window_closed(self, row: sqlite3.Row, now: str) -> bool:
        """Instagram's send window, separate from the product email wait."""
        if row["channel"] == "direct_message":
            latest = self.connection.execute(
                """
                SELECT MAX(occurred_at) AS occurred_at
                FROM inbound_events
                WHERE account_id = ? AND person_id = ? AND kind = 'message'
                """,
                (row["account_id"], row["person_id"]),
            ).fetchone()
            if latest is None or not latest["occurred_at"]:
                return True
            return now > add_seconds(latest["occurred_at"], DM_WINDOW_SECONDS)
        comment = self.connection.execute(
            """
            SELECT occurred_at FROM inbound_events
            WHERE id = (SELECT source_event_id FROM runs WHERE id = ?)
            """,
            (row["run_id"],),
        ).fetchone()
        if comment is None:
            return False
        return now > add_seconds(comment["occurred_at"], COMMENT_WINDOW_SECONDS)

    def _expire_for_window(self, row: sqlite3.Row, now: str) -> None:
        self.connection.execute(
            """
            UPDATE outbound_actions
            SET state = 'expired', updated_at = ?
            WHERE id = ? AND state = 'queued'
            """,
            (now, row["id"]),
        )
        if row["kind"] == "public_reply" and row["run_state"] == "awaiting_email":
            return
        expire_run(self.connection, row["run_id"], now)

    def _requeue_or_expire(self, action: sqlite3.Row, now: str) -> None:
        run = self.connection.execute(
            "SELECT state FROM runs WHERE id = ?", (action["run_id"],)
        ).fetchone()
        if opted_out(self.connection, action["account_id"], action["person_id"]) or (
            run is not None and run["state"] in ("stopped", "handed_off")
        ):
            self.connection.execute(
                """
                UPDATE outbound_actions
                SET state = 'cancelled', updated_at = ?
                WHERE id = ? AND state = 'sending'
                """,
                (now, action["id"]),
            )
            return
        if action["deadline_at"] <= now:
            self._expire_locked(action, now)
            return
        not_before = add_seconds(now, self.config.retry_delay_seconds)
        self.connection.execute(
            """
            UPDATE outbound_actions
            SET state = 'queued', not_before = ?, updated_at = ?
            WHERE id = ? AND state = 'sending'
            """,
            (not_before, now, action["id"]),
        )

    def _expire_locked(self, action: sqlite3.Row, now: str) -> None:
        self.connection.execute(
            """
            UPDATE outbound_actions
            SET state = 'expired', updated_at = ?
            WHERE id = ? AND state IN ('queued', 'sending')
            """,
            (now, action["id"]),
        )
        expire_run(self.connection, action["run_id"], now)

    def _rate_limit_until(self, account_id: str, now: str) -> str | None:
        window_start = add_seconds(now, -RATE_WINDOW_SECONDS)
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS spent, MIN(spent_at) AS oldest
            FROM budget_spend
            WHERE account_id = ? AND spent_at > ?
            """,
            (account_id, window_start),
        ).fetchone()
        if row["spent"] < self.config.rate_limit_per_minute or row["oldest"] is None:
            return None
        return add_seconds(row["oldest"], RATE_WINDOW_SECONDS)
