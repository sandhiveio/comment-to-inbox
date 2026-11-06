import logging
import sqlite3
import threading

from comment_to_inbox.db import connect, init_db
from comment_to_inbox.exporting import (
    ExportTemporaryError,
    FakeLeadExporter,
    Lead,
    claim_export,
    export_once,
    finish_export,
)
from comment_to_inbox.report import render_status

from tests.conftest import (
    accept_comment,
    accept_message,
    action_rows,
    after,
    prime_awaiting_email,
)


def _capture_email(world, external_id="email-1", body="a@example.com"):
    waiting = prime_awaiting_email(world)
    accepted = accept_message(
        world,
        external_id=external_id,
        body=body,
        occurred_at=after(waiting["boundary_at"]),
    )
    assert accepted["disposition"] == "applied_email"
    return accepted


def _lead(world):
    return world.conn.execute("SELECT * FROM leads").fetchone()


def _export(world, connection: sqlite3.Connection | None = None):
    conn = world.conn if connection is None else connection
    return conn.execute("SELECT * FROM lead_exports").fetchone()


def test_duplicate_inbound_dm_does_not_create_another_lead(world):
    waiting = prime_awaiting_email(world)
    occurred_at = after(waiting["boundary_at"])
    first = accept_message(
        world, external_id="email-1", body="a@example.com", occurred_at=occurred_at
    )
    second = accept_message(
        world, external_id="email-1", body="other@example.com", occurred_at=occurred_at
    )
    later = accept_message(
        world,
        external_id="email-2",
        body="other@example.com",
        occurred_at=after(waiting["boundary_at"], 2),
    )
    assert first["disposition"] == "applied_email"
    assert second["status"] == "duplicate"
    assert later["disposition"] == "ignored_after_close"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM leads").fetchone()["n"] == 1
    assert world.conn.execute("SELECT COUNT(*) AS n FROM lead_exports").fetchone()["n"] == 1
    assert len([row for row in action_rows(world) if row["kind"] == "link_dm"]) == 1
    assert _lead(world)["email"] == "a@example.com"


def test_existing_volume_does_not_queue_historical_addresses(tmp_path):
    path = tmp_path / "old.sqlite"
    connection = connect(path)
    init_db(connection)
    now = "2026-01-01T00:00:00.000000Z"
    connection.execute("BEGIN IMMEDIATE")
    cursor = connection.execute(
        """
        INSERT INTO inbound_events (
            account_id, external_id, kind, person_id, post_id, comment_id,
            body, occurred_at, received_at, disposition
        ) VALUES ('ig-test', 'old-comment', 'comment', 'person-old', 'post-1',
                  'c-old', 'guide', ?, ?, 'accepted_comment')
        """,
        (now, now),
    )
    connection.execute(
        """
        INSERT INTO runs (
            account_id, person_id, source_event_id, post_id, comment_id, state,
            email, email_event_id, boundary_at, invalid_replies, created_at, updated_at
        ) VALUES ('ig-test', 'person-old', ?, 'post-1', 'c-old', 'completed',
                  'old@example.com', NULL, NULL, 0, ?, ?)
        """,
        (cursor.lastrowid, now, now),
    )
    connection.execute("COMMIT")
    init_db(connection)
    assert connection.execute("SELECT COUNT(*) AS n FROM leads").fetchone()["n"] == 0
    assert connection.execute("SELECT COUNT(*) AS n FROM lead_exports").fetchone()["n"] == 0
    assert connection.execute("SELECT email FROM runs").fetchone()["email"] == "old@example.com"
    connection.close()


def test_unavailable_exporter_does_not_block_the_link_dm(world):
    _capture_email(world)
    exporter = _BlockingExporter()

    def run_export() -> None:
        export_conn = connect(world.path)
        try:
            export_once(
                export_conn,
                exporter,
                world.clock,
                batch_size=1,
                timeout_seconds=5,
                retry_delay_seconds=30,
            )
        finally:
            export_conn.close()

    thread = threading.Thread(target=run_export)
    thread.start()
    assert exporter.started.wait(2)
    assert _export(world)["state"] == "claimed"
    import asyncio

    assert asyncio.run(world.worker.step()) == "sent"
    link = [row for row in action_rows(world) if row["kind"] == "link_dm"][0]
    assert link["state"] == "succeeded"
    other = accept_comment(
        world,
        person_id="person-2",
        external_id="during-export",
        comment_id="c-during",
    )
    assert other["disposition"] == "accepted_comment"
    exporter.release.set()
    thread.join(2)
    assert not thread.is_alive()
    queued = _export(world)
    assert queued["state"] == "queued"
    assert queued["attempt_count"] == 1
    assert queued["next_attempt_at"] == "2026-01-01T00:00:30.000000Z"
    assert "a@example.com" not in (queued["last_error"] or "")


def test_lost_confirmation_retries_the_same_lead_id(world):
    _capture_email(world, body="winner@example.com")
    lead_id = _lead(world)["id"]
    exporter = FakeLeadExporter()
    export_conn = connect(world.path)
    try:
        claimed = claim_export(export_conn, world.clock, lease_seconds=30)
        assert claimed is not None
        exporter.upsert(claimed.lead)
        assert _export(world)["state"] == "claimed"
        world.clock.advance(31)
        stats = export_once(export_conn, exporter, world.clock, batch_size=1, lease_seconds=30)
    finally:
        export_conn.close()
    assert stats["exported"] == 1
    assert [call.lead_id for call in exporter.calls] == [lead_id, lead_id]
    assert _export(world)["state"] == "exported"


def test_expired_claim_can_be_taken_and_a_stale_attempt_cannot_finish(world):
    _capture_email(world)
    export_conn = connect(world.path)
    try:
        first = claim_export(export_conn, world.clock, lease_seconds=30)
        world.clock.advance(30)
        second = claim_export(export_conn, world.clock, lease_seconds=30)
        assert first is not None and second is not None
        assert first.lead.lead_id == second.lead.lead_id
        assert first.token != second.token
        assert finish_export(export_conn, first, "exported", world.clock, 30, None) is False
        current = _export(world)
        assert current["state"] == "claimed"
        assert current["claim_token"] == second.token
        assert finish_export(export_conn, second, "exported", world.clock, 30, None) is True
    finally:
        export_conn.close()
    assert _export(world)["state"] == "exported"


def test_permanent_failure_stays_visible_and_is_not_retried(world):
    _capture_email(world, body="keep@example.com")
    exporter = FakeLeadExporter(script=["permanent"])
    export_conn = connect(world.path)
    try:
        first = export_once(export_conn, exporter, world.clock, batch_size=5)
        second = export_once(export_conn, exporter, world.clock, batch_size=5)
    finally:
        export_conn.close()
    assert first["permanent"] == 1
    assert second["claimed"] == 0
    assert len(exporter.calls) == 1
    status = render_status(world.conn, world.config, "test.sqlite")
    assert "export state=permanent_failure count=1" in status
    assert "keep@example.com" not in status
    assert _export(world)["last_error"] == "ExportPermanentError"


def test_temporary_failure_waits_until_next_attempt(world):
    _capture_email(world)
    lead_id = _lead(world)["id"]
    exporter = FakeLeadExporter(script=["temporary", "success"])
    export_conn = connect(world.path)
    try:
        first = export_once(
            export_conn, exporter, world.clock, batch_size=5, retry_delay_seconds=30
        )
        blocked = export_once(export_conn, exporter, world.clock, batch_size=5)
        world.clock.advance(30)
        second = export_once(export_conn, exporter, world.clock, batch_size=5)
    finally:
        export_conn.close()
    assert first["retry"] == 1
    assert blocked["claimed"] == 0
    assert second["exported"] == 1
    assert [call.lead_id for call in exporter.calls] == [lead_id, lead_id]
    assert _export(world)["attempt_count"] == 2


def test_exporter_errors_do_not_include_the_address(world, caplog):
    _capture_email(world, body="secret@example.com")
    caplog.set_level(logging.ERROR)

    class LeakyExporter:
        def upsert(self, lead: Lead) -> None:
            raise ExportTemporaryError(f"payload {lead.email} {lead}")

    export_conn = connect(world.path)
    try:
        stats = export_once(export_conn, LeakyExporter(), world.clock, batch_size=1)
    finally:
        export_conn.close()
    assert stats["retry"] == 1
    assert "secret@example.com" not in caplog.text
    assert "payload" not in caplog.text
    stored = _export(world)["last_error"]
    assert stored == "ExportTemporaryError"
    assert "secret@example.com" not in render_status(world.conn, world.config, "test.sqlite")


def test_timeout_leaves_the_claim_for_a_later_attempt(world):
    _capture_email(world)
    lead_id = _lead(world)["id"]
    hanger = _BlockingExporter()
    export_conn = connect(world.path)
    try:
        stats = export_once(
            export_conn,
            hanger,
            world.clock,
            batch_size=1,
            lease_seconds=30,
            timeout_seconds=0.4,
        )
        assert stats["timed_out"] == 1
        assert _export(world)["state"] == "claimed"
        assert hanger.calls == [lead_id]
        hanger.release.set()
        world.clock.advance(30)
        follow = FakeLeadExporter()
        again = export_once(export_conn, follow, world.clock, batch_size=1, lease_seconds=30)
    finally:
        hanger.release.set()
        export_conn.close()
    assert again["exported"] == 1
    assert follow.calls[0].lead_id == lead_id


def test_overlapping_export_commands_upsert_once(world):
    _capture_email(world)
    lead_id = _lead(world)["id"]
    entered: list[str] = []
    release = threading.Event()

    class Gate:
        def upsert(self, lead: Lead) -> None:
            entered.append(lead.lead_id)
            release.wait(3)

    results: list[dict[str, int]] = []

    def run() -> None:
        connection = connect(world.path)
        try:
            results.append(
                export_once(
                    connection,
                    Gate(),
                    world.clock,
                    batch_size=1,
                    timeout_seconds=3,
                )
            )
        finally:
            connection.close()

    threads = [threading.Thread(target=run), threading.Thread(target=run)]
    for thread in threads:
        thread.start()
    for _ in range(50):
        if len(entered) == 1 and any(item["claimed"] == 0 for item in results):
            break
        threading.Event().wait(0.02)
    assert entered == [lead_id]
    assert any(item["claimed"] == 0 for item in results)
    release.set()
    for thread in threads:
        thread.join(3)
        assert not thread.is_alive()
    assert sum(item["exported"] for item in results) == 1
    assert _export(world)["state"] == "exported"


class _BlockingExporter:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.calls: list[str] = []

    def upsert(self, lead: Lead) -> None:
        self.calls.append(lead.lead_id)
        self.started.set()
        self.release.wait(2)
        raise ExportTemporaryError("destination unavailable")
