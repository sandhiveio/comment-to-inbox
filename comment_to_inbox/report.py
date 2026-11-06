import sqlite3

from comment_to_inbox.config import ProjectConfig


def render_status(
    connection: sqlite3.Connection, config: ProjectConfig, database: str
) -> str:
    lines = [
        f"account_id={config.account_id}",
        f"database={database}",
    ]
    runs = connection.execute("SELECT * FROM runs ORDER BY id").fetchall()
    if not runs:
        lines.append("runs=0")
    for run in runs:
        has_email = "yes" if run["email"] else "no"
        boundary = run["boundary_at"] or "-"
        lines.append(
            "run {id} person_id={person} state={state} email={has_email} invalid_replies={invalid} boundary={boundary}".format(
                id=run["id"],
                person=run["person_id"],
                state=run["state"],
                has_email=has_email,
                invalid=run["invalid_replies"],
                boundary=boundary,
            )
        )
    actions = connection.execute(
        "SELECT * FROM outbound_actions ORDER BY id"
    ).fetchall()
    for action in actions:
        lines.append(
            "action {id} run={run_id} {kind} slot={slot} channel={channel} state={state}".format(
                id=action["id"],
                run_id=action["run_id"],
                kind=action["kind"],
                slot=action["slot"],
                channel=action["channel"],
                state=action["state"],
            )
        )
    results = connection.execute("SELECT * FROM action_results ORDER BY id").fetchall()
    for result in results:
        lines.append(
            f"result action={result['action_id']} attempt={result['attempt_no']} outcome={result['outcome']}"
        )
    spend = connection.execute("SELECT COUNT(*) AS n FROM budget_spend").fetchone()["n"]
    opt_outs = connection.execute("SELECT COUNT(*) AS n FROM opt_outs").fetchone()["n"]
    lines.append(f"budget_spend={spend}")
    lines.append(f"opt_outs={opt_outs}")
    lines.extend(_export_lines(connection))
    lines.extend(_handoff_lines(connection))
    lines.extend(_stat_lines(connection, config.account_id))
    return "\n".join(lines)


def _export_lines(connection: sqlite3.Connection) -> list[str]:
    counts = {
        row["state"]: row["n"]
        for row in connection.execute(
            "SELECT state, COUNT(*) AS n FROM lead_exports GROUP BY state"
        )
    }
    lines = []
    for state in ("queued", "claimed", "exported", "permanent_failure", "cancelled"):
        lines.append(f"export state={state} count={counts.get(state, 0)}")
    pending = connection.execute(
        """
        SELECT lead_id, state, attempt_count, next_attempt_at
        FROM lead_exports
        WHERE state != 'exported'
        ORDER BY id
        """
    ).fetchall()
    for row in pending:
        nxt = row["next_attempt_at"] or "-"
        lines.append(
            "export lead_id={lead_id} state={state} attempts={attempts} next_attempt_at={nxt}".format(
                lead_id=row["lead_id"],
                state=row["state"],
                attempts=row["attempt_count"],
                nxt=nxt,
            )
        )
    return lines


def _handoff_lines(connection: sqlite3.Connection) -> list[str]:
    counts = {
        row["state"]: row["n"]
        for row in connection.execute(
            "SELECT state, COUNT(*) AS n FROM operator_handoffs GROUP BY state"
        )
    }
    lines = [
        f"handoff state=active count={counts.get('active', 0)}",
        f"handoff state=delivered count={counts.get('delivered', 0)}",
    ]
    pending = connection.execute(
        """
        SELECT h.id AS conversation_id, h.reason, n.state, n.attempt_count
        FROM operator_notifications AS n
        JOIN operator_handoffs AS h ON h.id = n.handoff_id
        WHERE n.state != 'delivered'
        ORDER BY n.id
        """
    ).fetchall()
    for row in pending:
        lines.append(
            "notify conversation={conversation} reason={reason} state={state} attempts={attempts}".format(
                conversation=row["conversation_id"],
                reason=row["reason"],
                state=row["state"],
                attempts=row["attempt_count"],
            )
        )
    return lines


def _stat_lines(connection: sqlite3.Connection, account_id: str) -> list[str]:
    rows = connection.execute(
        """
        SELECT day, event, count
        FROM daily_counts
        WHERE account_id = ?
        ORDER BY day, event
        """,
        (account_id,),
    ).fetchall()
    if not rows:
        return ["stats=0"]
    return [
        f"stat day={row['day']} event={row['event']} count={row['count']}"
        for row in rows
    ]
