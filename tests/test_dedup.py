from tests.conftest import accept_comment, action_rows, drain, run_row


def test_repeat_comment_on_another_post_does_not_start_another_run(world):
    missed = accept_comment(
        world,
        external_id="not-a-keyword",
        body="guidance please",
        post_id="post-1",
    )
    assert missed["disposition"] == "ignored_keyword"
    assert run_row(world) is None

    other_post = accept_comment(
        world,
        external_id="wrong-post",
        body="guide",
        post_id="post-9",
        comment_id="ig-comment-9",
    )
    assert other_post["disposition"] == "ignored_post"
    assert run_row(world) is None

    first = accept_comment(world, external_id="first", post_id="post-1", comment_id="c-1")
    assert first["disposition"] == "accepted_comment"
    assert drain(world) == "idle"

    duplicate = accept_comment(
        world,
        external_id="first",
        post_id="post-2",
        comment_id="c-2",
        body="guide again",
    )
    assert duplicate["status"] == "duplicate"

    second_post = accept_comment(
        world,
        external_id="second",
        post_id="post-2",
        comment_id="c-2",
        body="guide on the other post",
    )
    assert second_post["disposition"] == "ignored_existing_run"
    assert drain(world) == "idle"

    finished_comment = accept_comment(
        world,
        person_id="person-1",
        external_id="after-done",
        post_id="post-2",
        comment_id="c-3",
        body="guide after the run",
    )
    # The run is waiting, not completed, and still blocks another launch.
    assert finished_comment["disposition"] == "ignored_existing_run"

    runs = world.conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"]
    assert runs == 1
    assert len(action_rows(world)) == 2

    other_person = accept_comment(
        world,
        person_id="person-2",
        external_id="other-person",
        post_id="post-2",
        comment_id="c-4",
    )
    assert other_person["disposition"] == "accepted_comment"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 2


def test_same_person_is_not_restarted_after_completion(world):
    from tests.conftest import accept_message, after, drain as drain_again

    accept_comment(world)
    assert drain(world) == "idle"
    waiting = run_row(world)
    accept_message(
        world,
        external_id="email",
        body="reader@example.com",
        occurred_at=after(waiting["boundary_at"]),
    )
    assert drain_again(world) == "idle"
    assert run_row(world)["state"] == "completed"

    again = accept_comment(
        world,
        external_id="later",
        post_id="post-2",
        comment_id="later-comment",
        body="guide please",
    )
    assert again["disposition"] == "ignored_existing_run"
    assert world.conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 1
    assert len(world.adapter.calls) == 3
