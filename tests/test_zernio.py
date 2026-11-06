import asyncio
from pathlib import Path

from comment_to_inbox.adapter import (
    OUTCOME_EXPLICIT_FAILURE,
    OUTCOME_SUCCESS,
    OUTCOME_UNKNOWN,
    AdapterNotConfigured,
    OutboundRequest,
    ZernioAdapter,
)
from comment_to_inbox.cli import main
from tests.conftest import accept_comment, accept_message, after, drain, make_config, run_row

BASE = "https://zernio.test/api/v1"


class Stub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "body": body, "timeout": timeout}
        )
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _config():
    return make_config(
        zernio_base_url=BASE,
        zernio_api_key="test-key",
        send_timeout_seconds=3,
    )


def _request(**overrides) -> OutboundRequest:
    values = dict(
        action_id=1,
        attempt_no=1,
        kind="email_request",
        channel="private_reply",
        account_id="ig-test",
        person_id="igsid-author",
        comment_id="comment-9",
        post_id="post-9",
        body="Reply with your email.",
        attempted_at="2026-01-01T00:00:00.000000Z",
        conversation_id="",
    )
    values.update(overrides)
    return OutboundRequest(**values)


def test_zernio_requires_live_settings():
    try:
        ZernioAdapter(make_config())
    except AdapterNotConfigured as exc:
        assert "zernio_api_key" in str(exc)
        assert "test-key" not in str(exc)
    else:
        raise AssertionError("missing Zernio settings must fail at construction")


def test_serve_zernio_fails_when_the_key_is_empty(tmp_path):
    code = main(
        [
            "--config",
            "config/project.json",
            "--database",
            str(tmp_path / "inbox.sqlite"),
            "serve",
            "--adapter",
            "zernio",
        ]
    )
    assert code == 2


def test_private_public_and_direct_message_calls():
    stub = Stub(
        [
            (200, {"status": "success", "messageId": "mid-private", "commentId": "comment-9"}),
            (200, {"success": True, "data": {"commentId": "reply-1", "isReply": True}}),
            (
                200,
                {
                    "data": {
                        "id": "conv-1",
                        "accountId": "ig-test",
                        "participantId": "igsid-author",
                    }
                },
            ),
            (200, {"success": True, "data": {"messageId": "mid-link", "conversationId": "conv-1"}}),
        ]
    )
    adapter = ZernioAdapter(_config(), transport=stub)
    private = asyncio.run(adapter.send(_request()))
    public = asyncio.run(
        adapter.send(
            _request(kind="public_reply", channel="comment_reply", body="Thanks. I sent you a message.")
        )
    )
    direct = asyncio.run(
        adapter.send(
            _request(
                kind="link_dm",
                channel="direct_message",
                conversation_id="conv-1",
                body="Here is the link: https://example.com/guide",
            )
        )
    )
    assert private.outcome == OUTCOME_SUCCESS and private.provider_message_id == "mid-private"
    assert public.outcome == OUTCOME_SUCCESS and public.provider_message_id == "reply-1"
    assert direct.outcome == OUTCOME_SUCCESS and direct.provider_message_id == "mid-link"
    assert [call["method"] for call in stub.calls] == ["POST", "POST", "GET", "POST"]
    assert stub.calls[0]["url"] == f"{BASE}/inbox/comments/post-9/comment-9/private-reply"
    assert stub.calls[0]["body"] == {"accountId": "ig-test", "message": "Reply with your email."}
    assert stub.calls[1]["url"] == f"{BASE}/inbox/comments/post-9"
    assert stub.calls[1]["body"]["commentId"] == "comment-9"
    assert stub.calls[2]["url"] == f"{BASE}/inbox/conversations/conv-1?accountId=ig-test"
    assert stub.calls[3]["url"] == f"{BASE}/inbox/conversations/conv-1/messages"
    assert stub.calls[3]["body"]["accountId"] == "ig-test"
    assert all(call["headers"]["Authorization"] == "Bearer test-key" for call in stub.calls)


def test_direct_message_fails_closed_without_a_matching_conversation():
    missing = ZernioAdapter(_config(), transport=Stub([]))
    result = missing._direct_message(_request(channel="direct_message", conversation_id=""))
    assert result.outcome == OUTCOME_EXPLICIT_FAILURE
    assert missing._transport.calls == []

    mismatch = Stub(
        [
            (
                200,
                {"data": {"accountId": "ig-test", "participantId": "someone-else"}},
            )
        ]
    )
    adapter = ZernioAdapter(_config(), transport=mismatch)
    result = adapter._direct_message(
        _request(channel="direct_message", conversation_id="conv-1")
    )
    assert result.outcome == OUTCOME_EXPLICIT_FAILURE
    assert [call["method"] for call in mismatch.calls] == ["GET"]

    blank = Stub([(200, {"data": {"accountId": "ig-test"}})])
    adapter = ZernioAdapter(_config(), transport=blank)
    result = adapter._direct_message(
        _request(channel="direct_message", conversation_id="conv-1")
    )
    assert result.outcome == OUTCOME_EXPLICIT_FAILURE
    assert [call["method"] for call in blank.calls] == ["GET"]


def test_private_reply_rewrites_an_instagram_shortcode():
    stub = Stub(
        [
            (
                200,
                {
                    "comments": [
                        {
                            "id": "18450000000000001",
                            "permalink": "https://www.instagram.com/p/DfYsTp8hiNh/",
                        }
                    ]
                },
            ),
            (200, {"messageId": "mid-private"}),
        ]
    )
    adapter = ZernioAdapter(_config(), transport=stub)
    result = adapter._private_reply(_request(post_id="DfYsTp8hiNh"))
    assert result.outcome == OUTCOME_SUCCESS
    assert stub.calls[0]["url"].endswith("/inbox/comments?accountId=ig-test&platform=instagram&limit=50")
    assert stub.calls[1]["url"] == f"{BASE}/inbox/comments/18450000000000001/comment-9/private-reply"


def test_private_reply_failure_classes():
    cases = [
        (
            (400, {"details": {"privateReplyConsumed": True}}),
            OUTCOME_UNKNOWN,
        ),
        (
            (400, {"error": "validation", "details": {}}),
            OUTCOME_EXPLICIT_FAILURE,
        ),
        (
            (502, {"details": {"privateReplyOutcome": "unknown"}}),
            OUTCOME_UNKNOWN,
        ),
        (
            (502, {"error": "upstream"}),
            OUTCOME_UNKNOWN,
        ),
        (TimeoutError("timed out"), OUTCOME_UNKNOWN),
        (ConnectionRefusedError("refused"), OUTCOME_EXPLICIT_FAILURE),
        ((200, {"status": "success"}), OUTCOME_UNKNOWN),
    ]
    for response, outcome in cases:
        stub = Stub([response])
        result = ZernioAdapter(_config(), transport=stub)._private_reply(_request())
        assert result.outcome == outcome
        assert result.provider_message_id is None or outcome == OUTCOME_SUCCESS


def test_worker_copies_the_inbound_conversation_id(world):
    accept_comment(world)
    assert drain(world) == "idle"
    waiting = run_row(world)
    accept_message(
        world,
        external_id="email-1",
        body="lead@example.com",
        occurred_at=after(waiting["boundary_at"]),
        conversation_id="conv-live",
    )
    assert drain(world) == "idle"
    assert world.adapter.calls[0].conversation_id == ""
    assert world.adapter.calls[-1].channel == "direct_message"
    assert world.adapter.calls[-1].conversation_id == "conv-live"


def test_compose_local_profile_is_one_zernio_service():
    text = Path("compose.yaml").read_text(encoding="utf-8")
    local = text.split("  local:", 1)[1]
    assert 'profiles: ["local"]' in local
    assert '"serve", "--adapter", "zernio"' in local
    assert "./config/local.json:/config/local.json:ro" in local
    assert "local-data:/data" in local
    assert "local-data:" in text
    assert "webhook" not in local
