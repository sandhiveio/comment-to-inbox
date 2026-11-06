from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.inbox_poll import ZernioInbox


def test_poll_ingests_a_matching_comment_and_an_incoming_message(world):
    config = ProjectConfig(
        **{
            **world.config.__dict__,
            "zernio_base_url": "https://zernio.example/api/v1",
            "zernio_api_key": "test-key",
        }
    )
    pages = {
        "/inbox/comments": {
            "comments": [
                {"id": "zernio-post-1", "permalink": "https://www.instagram.com/p/post-1/"}
            ]
        },
        "/inbox/comments/zernio-post-1": {
            "comments": [
                {
                    "id": "comment-9",
                    "message": "guide",
                    "createdTime": "2026-11-06T12:00:00Z",
                    "from": {"id": "person-9", "username": "reader"},
                }
            ]
        },
        "/inbox/conversations": {"conversations": [{"id": "conv-9"}]},
        "/inbox/conversations/conv-9/messages": {
            "messages": [
                {
                    "id": "msg-out",
                    "direction": "outgoing",
                    "senderId": "ig-test",
                    "message": "Reply with your email.",
                    "createdAt": "2026-11-06T12:01:00Z",
                },
                {
                    "id": "msg-in",
                    "direction": "incoming",
                    "senderId": "person-9",
                    "message": "reader@example.com",
                    "createdAt": "2026-11-06T12:02:00Z",
                    "conversationId": "conv-9",
                },
            ]
        },
    }

    def transport(_base, _key, path, _query):
        if path in pages:
            return pages[path]
        if path.startswith("/inbox/comments/"):
            return {"comments": []}
        raise AssertionError(path)

    inbox = ZernioInbox(config, transport=transport)
    assert inbox.poll(world.conn, "2026-11-06T12:05:00Z") == 2
    assert inbox.poll(world.conn, "2026-11-06T12:06:00Z") == 0
    run = world.conn.execute("SELECT post_id, person_id, state FROM runs").fetchone()
    assert run["post_id"] == "post-1"
    assert run["person_id"] == "person-9"
    message = world.conn.execute(
        "SELECT conversation_id FROM inbound_events WHERE kind = 'message'"
    ).fetchone()
    assert message["conversation_id"] == "conv-9"
