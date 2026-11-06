import csv
import io
from datetime import datetime, timezone

from comment_to_inbox.cli import main
from comment_to_inbox.stats import CSV_COLUMNS, previous_closed_utc_day, render_stats_csv

from tests.conftest import accept_comment


def test_previous_closed_day_is_yesterday_utc():
    now = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)
    assert previous_closed_utc_day(now) == "2026-11-05"


def test_stats_csv_has_fixed_columns_and_launch_conversion(world, capsys):
    accept_comment(world)
    text = render_stats_csv(world.conn, world.config.account_id, "2026-01-01")
    rows = list(csv.DictReader(io.StringIO(text)))
    assert list(rows[0]) == list(CSV_COLUMNS)
    assert rows[0]["account_id"] == "ig-test"
    assert rows[0]["day"] == "2026-01-01"
    assert rows[0]["accepted_comment"] == "1"
    assert rows[0]["public_reply_confirmed"] == "0"
    assert rows[0]["runs_started"] == "1"
    assert rows[0]["leads"] == "0"
    assert rows[0]["conversion"] == "0.0000"
    assert rows[0]["preliminary"] == "yes"

    code = main(
        [
            "--config",
            "config/project.json",
            "--database",
            str(world.path),
            "stats",
            "--day",
            "2026-01-02",
            "--format",
            "csv",
        ]
    )
    assert code == 0
    printed = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert printed[0]["account_id"] == "ig-demo-alpha"
    assert printed[0]["accepted_comment"] == "0"
    assert printed[0]["runs_started"] == "0"
    assert printed[0]["preliminary"] == "no"
