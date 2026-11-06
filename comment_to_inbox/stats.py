"""Daily counters for the events an operator reviews.

One row is one UTC day, one account, and one event name. The count is
incremented in the same transaction as the event it describes.
"""

import csv
import io
import sqlite3
from datetime import datetime, timedelta, timezone

TIMESTAMP_FORMAT_PREFIX = 10

COUNT_COLUMNS = (
    "accepted_comment",
    "opt_out",
    "email_accepted",
    "private_reply_confirmed",
    "public_reply_queued",
    "public_reply_confirmed",
    "public_fallback_queued",
    "public_fallback_confirmed",
    "link_confirmed",
    "lead_exported",
    "run_expired",
    "escalation",
    "email_exhausted",
    "private_reply_failed",
    "private_reply_unknown",
)

# Open waits still belong to the launch day. Their conversion is not final.
OPEN_RUN_STATES = (
    "public_reply_pending",
    "email_request_pending",
    "awaiting_email",
    "link_dm_pending",
)

CSV_COLUMNS = (
    "account_id",
    "day",
    *COUNT_COLUMNS,
    "runs_started",
    "leads",
    "conversion",
    "preliminary",
)


def record_event(
    connection: sqlite3.Connection, account_id: str, event: str, now: str
) -> None:
    day = now[:TIMESTAMP_FORMAT_PREFIX]
    connection.execute(
        """
        INSERT INTO daily_counts (day, account_id, event, count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT (day, account_id, event)
        DO UPDATE SET count = count + 1
        """,
        (day, account_id, event),
    )


def previous_closed_utc_day(now: datetime | None = None) -> str:
    """The last UTC date that has already finished."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()


def parse_day(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("day must be YYYY-MM-DD") from exc
    return parsed.date().isoformat()


def render_stats_csv(connection: sqlite3.Connection, account_id: str, day: str) -> str:
    """One header and one project row. Missing events are zero."""
    counts = {
        row["event"]: int(row["count"])
        for row in connection.execute(
            """
            SELECT event, count FROM daily_counts
            WHERE account_id = ? AND day = ?
            """,
            (account_id, day),
        )
    }
    started = connection.execute(
        """
        SELECT COUNT(*) AS n FROM runs
        WHERE account_id = ? AND substr(created_at, 1, 10) = ?
        """,
        (account_id, day),
    ).fetchone()["n"]
    leads = connection.execute(
        """
        SELECT COUNT(*) AS n
        FROM runs
        JOIN leads ON leads.run_id = runs.id
        WHERE runs.account_id = ? AND substr(runs.created_at, 1, 10) = ?
        """,
        (account_id, day),
    ).fetchone()["n"]
    open_runs = connection.execute(
        f"""
        SELECT COUNT(*) AS n FROM runs
        WHERE account_id = ? AND substr(created_at, 1, 10) = ?
          AND state IN ({",".join("?" for _ in OPEN_RUN_STATES)})
        """,
        (account_id, day, *OPEN_RUN_STATES),
    ).fetchone()["n"]
    conversion = "0.0000" if started == 0 else f"{leads / started:.4f}"
    row = {
        "account_id": account_id,
        "day": day,
        **{name: counts.get(name, 0) for name in COUNT_COLUMNS},
        "runs_started": started,
        "leads": leads,
        "conversion": conversion,
        "preliminary": "yes" if open_runs else "no",
    }
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerow(row)
    return buffer.getvalue()
