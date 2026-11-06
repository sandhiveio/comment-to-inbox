import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbound_events (
    id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('comment', 'message')),
    person_id TEXT NOT NULL,
    post_id TEXT,
    comment_id TEXT,
    body TEXT NOT NULL,
    conversation_id TEXT,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    disposition TEXT NOT NULL,
    UNIQUE (account_id, external_id)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    source_event_id INTEGER NOT NULL UNIQUE REFERENCES inbound_events(id),
    post_id TEXT NOT NULL,
    comment_id TEXT NOT NULL,
    state TEXT NOT NULL,
    email TEXT,
    email_event_id INTEGER REFERENCES inbound_events(id),
    boundary_at TEXT,
    invalid_replies INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (account_id, person_id)
);

CREATE TABLE IF NOT EXISTS outbound_actions (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    account_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('public_reply', 'email_request', 'link_dm')),
    slot INTEGER NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('comment_reply', 'private_reply', 'direct_message')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'sending', 'succeeded', 'unknown', 'cancelled', 'expired')),
    not_before TEXT,
    deadline_at TEXT NOT NULL,
    body TEXT NOT NULL,
    provider_message_id TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (run_id, kind, slot),
    CHECK (
        (kind = 'email_request' AND slot BETWEEN 0 AND 3)
        OR (kind != 'email_request' AND slot = 0)
    )
);

CREATE TABLE IF NOT EXISTS action_results (
    id INTEGER PRIMARY KEY,
    action_id INTEGER NOT NULL REFERENCES outbound_actions(id),
    attempt_no INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('pending', 'success', 'explicit_failure', 'unknown')),
    detail TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (action_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS opt_outs (
    account_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    command TEXT NOT NULL,
    source_event_id INTEGER NOT NULL REFERENCES inbound_events(id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (account_id, person_id)
);

CREATE TABLE IF NOT EXISTS budget_spend (
    id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    action_id INTEGER NOT NULL REFERENCES outbound_actions(id),
    attempt_no INTEGER NOT NULL,
    spent_at TEXT NOT NULL,
    UNIQUE (action_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS leads (
    id TEXT PRIMARY KEY,
    run_id INTEGER NOT NULL UNIQUE REFERENCES runs(id),
    account_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    email TEXT NOT NULL,
    post_id TEXT NOT NULL,
    accepted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lead_exports (
    id INTEGER PRIMARY KEY,
    lead_id TEXT NOT NULL UNIQUE REFERENCES leads(id),
    state TEXT NOT NULL CHECK (state IN ('queued', 'claimed', 'exported', 'permanent_failure', 'cancelled')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    claim_expires_at TEXT,
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inbound_person
    ON inbound_events (account_id, person_id, kind, disposition, occurred_at);
CREATE INDEX IF NOT EXISTS idx_outbound_queue
    ON outbound_actions (state, not_before, id);
CREATE INDEX IF NOT EXISTS idx_budget_window
    ON budget_spend (account_id, spent_at);
CREATE INDEX IF NOT EXISTS idx_lead_exports_ready
    ON lead_exports (state, next_attempt_at, claim_expires_at, id);

CREATE TABLE IF NOT EXISTS operator_handoffs (
    id TEXT PRIMARY KEY,
    run_id INTEGER REFERENCES runs(id),
    account_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'delivered', 'cleared')),
    created_at TEXT NOT NULL,
    cleared_at TEXT
);

CREATE TABLE IF NOT EXISTS operator_notifications (
    id INTEGER PRIMARY KEY,
    handoff_id TEXT NOT NULL UNIQUE REFERENCES operator_handoffs(id),
    state TEXT NOT NULL CHECK (state IN ('queued', 'claimed', 'delivered')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    claim_expires_at TEXT,
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_notifications_ready
    ON operator_notifications (state, next_attempt_at, claim_expires_at, id);

CREATE TABLE IF NOT EXISTS daily_counts (
    day TEXT NOT NULL,
    account_id TEXT NOT NULL,
    event TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (day, account_id, event)
);
"""

BUSY_TIMEOUT_MS = 5000

ACTIVE_RUN_STATES = (
    "public_reply_pending",
    "email_request_pending",
    "awaiting_email",
    "link_dm_pending",
)

TERMINAL_RUN_STATES = ("completed", "stopped", "exhausted", "expired")


def connect(path: str | Path) -> sqlite3.Connection:
    database = Path(path)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def init_db(connection: sqlite3.Connection) -> None:
    """Create any missing tables and widen handoffs on an older volume.

    Safe on a volume created by an older build. Rows already stored in
    ``runs``, ``leads``, ``lead_exports``, and ``operator_handoffs`` are kept.
    Addresses already stored on old runs are not copied and are not queued.
    """
    connection.executescript(SCHEMA)
    _ensure_inbound_columns(connection)
    _migrate_handoffs(connection)
    _ensure_handoff_indexes(connection)
    _ensure_run_columns(connection)
    _migrate_lead_exports(connection)


def _ensure_inbound_columns(connection: sqlite3.Connection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(inbound_events)")}
    if "conversation_id" not in columns:
        connection.execute("ALTER TABLE inbound_events ADD COLUMN conversation_id TEXT")


def _ensure_run_columns(connection: sqlite3.Connection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    if "email_deadline_at" not in columns:
        connection.execute("ALTER TABLE runs ADD COLUMN email_deadline_at TEXT")


def _migrate_lead_exports(connection: sqlite3.Connection) -> None:
    master = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'lead_exports'"
    ).fetchone()
    if master is None or "cancelled" in (master[0] or ""):
        return
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN")
        connection.execute(
            """
            CREATE TABLE lead_exports_next (
                id INTEGER PRIMARY KEY,
                lead_id TEXT NOT NULL UNIQUE REFERENCES leads(id),
                state TEXT NOT NULL CHECK (state IN ('queued', 'claimed', 'exported', 'permanent_failure', 'cancelled')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                claim_token TEXT,
                claim_expires_at TEXT,
                next_attempt_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO lead_exports_next (
                id, lead_id, state, attempt_count, claim_token, claim_expires_at,
                next_attempt_at, last_error, created_at, updated_at
            )
            SELECT
                id, lead_id, state, attempt_count, claim_token, claim_expires_at,
                next_attempt_at, last_error, created_at, updated_at
            FROM lead_exports
            """
        )
        connection.execute("DROP TABLE lead_exports")
        connection.execute("ALTER TABLE lead_exports_next RENAME TO lead_exports")
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_exports_ready
                ON lead_exports (state, next_attempt_at, claim_expires_at, id)
            """
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


def _ensure_handoff_indexes(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_handoff_run
            ON operator_handoffs (run_id) WHERE run_id IS NOT NULL
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_handoff_active_person
            ON operator_handoffs (account_id, person_id) WHERE state = 'active'
        """
    )


def _migrate_handoffs(connection: sqlite3.Connection) -> None:
    """Add person-level handoffs to a database created before escalation.

    Older rows required a run and used states ``active`` and ``delivered``.
    Those rows stay. ``delivered`` is left as delivered so an old notification
    is not treated as still waiting for an operator to clear it.
    """
    master = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'operator_handoffs'"
    ).fetchone()
    if master is None:
        return
    columns = {row[1] for row in connection.execute("PRAGMA table_info(operator_handoffs)")}
    if "person_id" in columns and "cleared" in (master[0] or ""):
        return
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN")
        connection.execute(
            """
            CREATE TABLE operator_handoffs_next (
                id TEXT PRIMARY KEY,
                run_id INTEGER REFERENCES runs(id),
                account_id TEXT NOT NULL,
                person_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('active', 'delivered', 'cleared')),
                created_at TEXT NOT NULL,
                cleared_at TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO operator_handoffs_next (
                id, run_id, account_id, person_id, reason, state, created_at, cleared_at
            )
            SELECT
                h.id,
                h.run_id,
                h.account_id,
                COALESCE(r.person_id, ''),
                h.reason,
                h.state,
                h.created_at,
                NULL
            FROM operator_handoffs AS h
            LEFT JOIN runs AS r ON r.id = h.run_id
            """
        )
        connection.execute("DROP TABLE operator_handoffs")
        connection.execute("ALTER TABLE operator_handoffs_next RENAME TO operator_handoffs")
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_handoff_run
                ON operator_handoffs (run_id) WHERE run_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_handoff_active_person
                ON operator_handoffs (account_id, person_id) WHERE state = 'active'
            """
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


@contextmanager
def transaction(connection: sqlite3.Connection):
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")
