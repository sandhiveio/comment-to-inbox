import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

OUTCOME_SUCCESS = "success"
OUTCOME_EXPLICIT_FAILURE = "explicit_failure"
OUTCOME_UNKNOWN = "unknown"
PERMANENT_FAILURE_DETAIL = "permanent"

# How a real adapter classifies provider responses:
# - confirmed provider message id -> success
# - the provider states the message was not accepted and no side effect
#   occurred (validation 4xx, connection failure before the request was
#   written) -> explicit_failure, safe to retry later
# - timeout, connection drop after the request was written, HTTP 5xx, or a
#   body that says the outcome is unknown -> unknown, never retried here


class AdapterNotConfigured(RuntimeError):
    pass


class ExplicitFailure(Exception):
    """The provider definitely did not accept the request."""


@dataclass(frozen=True)
class OutboundRequest:
    action_id: int
    attempt_no: int
    kind: str
    channel: str
    account_id: str
    person_id: str
    comment_id: str
    post_id: str
    body: str
    attempted_at: str
    conversation_id: str = ""


@dataclass(frozen=True)
class SendResult:
    outcome: str
    provider_message_id: str | None = None
    sent_at: str | None = None
    detail: str = ""

    @staticmethod
    def success(provider_message_id: str, sent_at: str, detail: str = "") -> "SendResult":
        return SendResult(OUTCOME_SUCCESS, provider_message_id, sent_at, detail)

    @staticmethod
    def explicit_failure(detail: str) -> "SendResult":
        return SendResult(OUTCOME_EXPLICIT_FAILURE, None, None, detail)

    @staticmethod
    def unknown(detail: str) -> "SendResult":
        return SendResult(OUTCOME_UNKNOWN, None, None, detail)


class FakeInstagram:
    """In-process stand-in used by the demo and the tests.

    Outcomes are ``success``, ``explicit_failure``, and ``unknown``.
    An empty script repeats success. ``fixed_sent_at`` forces the provider
    timestamp so tests can separate that clock from local receive time.
    """

    def __init__(self) -> None:
        self.script: list[str] = []
        self.calls: list[OutboundRequest] = []
        self.fixed_sent_at: str | None = None
        self.on_send = None

    async def send(self, request: OutboundRequest) -> SendResult:
        self.calls.append(request)
        if self.on_send is not None:
            await _maybe_await(self.on_send(request))
        outcome = self.script.pop(0) if self.script else OUTCOME_SUCCESS
        if outcome == OUTCOME_EXPLICIT_FAILURE:
            return SendResult.explicit_failure("fake explicit failure")
        if outcome == "permanent_failure":
            return SendResult.explicit_failure(PERMANENT_FAILURE_DETAIL)
        if outcome == OUTCOME_UNKNOWN:
            return SendResult.unknown("fake unknown result")
        if outcome != OUTCOME_SUCCESS:
            raise ExplicitFailure(f"unsupported fake outcome {outcome}")
        sent_at = self.fixed_sent_at or request.attempted_at
        message_id = f"fake-{request.action_id}-{request.attempt_no}"
        return SendResult.success(message_id, sent_at)


class OfficialInstagramAdapter:
    """Primary adapter. Not constructed by the demo.

    Public comment reply:
        POST /{comment-id}/replies
        body field ``message``

    Private reply to the comment, one per comment, within 7 days.
    This does not open the standard messaging window:
        POST /{ig-user-id}/messages
        recipient.comment_id = comment id
        message.text = text

    Direct message after the person has written, inside the messaging window
    (normally 24 hours from their latest message):
        POST /{ig-user-id}/messages
        recipient.id = Instagram-scoped person id
        message.text = text

    Host is graph.instagram.com for Instagram Login, or graph.facebook.com
    for a Facebook Page token. The demo does not call either host.
    """

    async def send(self, request: OutboundRequest) -> SendResult:
        raise AdapterNotConfigured("official Instagram credentials are not configured")


class ZernioAdapter:
    """Live sender for the same three channels. Product rules stay in the worker.

    ``base_url`` is the configured origin plus ``/api/v1``. Paths below are
    appended to that base, which already contains the ``/v1`` prefix from the
    Zernio docs.

    Private reply (one per comment, within 7 days):
        POST /inbox/comments/{postId}/{commentId}/private-reply
        body ``accountId``, ``message``
        200 ``messageId`` is success.
        400 ``details.privateReplyConsumed: true`` is not a safe retry.
        502 ``details.privateReplyOutcome: "unknown"`` is ``unknown``.

    Public comment reply:
        POST /inbox/comments/{postId}
        body ``accountId``, ``message``, ``commentId``
        200 ``data.commentId`` is success.

    Direct message, only after the person has written:
        POST /inbox/conversations/{conversationId}/messages
        body ``accountId``, ``message``
        200 ``data.messageId`` is success.

    A direct message is addressed by the conversation id carried on the
    inbound message. Before that POST, the adapter reads
    ``GET /inbox/conversations/{conversationId}`` and sends only when
    ``participantId`` equals the run's person id (the comment ``from.id`` and
    the message ``senderId``, the same Instagram-scoped id) and ``accountId``
    matches this project. A first message from someone who has never written
    before still has to carry that conversation id; the comment author id is
    not a conversation id. Zero or several candidates, or a participant that
    does not match, fails closed and does not POST.
    """

    def __init__(self, config, transport=None) -> None:
        missing = [
            name
            for name, value in (
                ("account_id", config.account_id),
                ("zernio_base_url", config.zernio_base_url),
                ("zernio_api_key", config.zernio_api_key),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise AdapterNotConfigured(
                "Zernio adapter requires " + ", ".join(missing)
            )
        self.base_url = str(config.zernio_base_url).rstrip("/")
        self.api_key = str(config.zernio_api_key).strip()
        self.account_id = str(config.account_id).strip()
        self.timeout = float(config.send_timeout_seconds)
        self._transport = transport or _urllib_transport

    async def send(self, request: OutboundRequest) -> SendResult:
        if request.account_id != self.account_id:
            return SendResult.explicit_failure("account does not match the configured Zernio account")
        if request.channel == "private_reply":
            return self._private_reply(request)
        if request.channel == "comment_reply":
            return self._public_reply(request)
        if request.channel == "direct_message":
            return self._direct_message(request)
        return SendResult.explicit_failure("unsupported channel")

    def _private_reply(self, request: OutboundRequest) -> SendResult:
        if not request.post_id or not request.comment_id:
            return SendResult.explicit_failure("private reply requires post id and comment id")
        post_id = self._provider_post_id(request.post_id)
        path = (
            f"/inbox/comments/{_quote(post_id)}/"
            f"{_quote(request.comment_id)}/private-reply"
        )
        status, payload, failed = self._post(
            path, {"accountId": self.account_id, "message": request.body}
        )
        if failed is not None:
            return failed
        if _private_reply_not_retryable(status, payload):
            return SendResult.unknown("private reply outcome is not safe to retry")
        if status >= 500:
            return SendResult.unknown(f"zernio {status}")
        if status >= 400:
            return SendResult.explicit_failure(_http_failure(status, payload))
        message_id = _string(payload.get("messageId"))
        if not message_id:
            return SendResult.unknown("zernio success response missing messageId")
        return SendResult.success(message_id, _sent_at(payload))

    def _public_reply(self, request: OutboundRequest) -> SendResult:
        if not request.post_id or not request.comment_id:
            return SendResult.explicit_failure("public reply requires post id and comment id")
        path = f"/inbox/comments/{_quote(self._provider_post_id(request.post_id))}"
        status, payload, failed = self._post(
            path,
            {
                "accountId": self.account_id,
                "message": request.body,
                "commentId": request.comment_id,
            },
        )
        if failed is not None:
            return failed
        if status >= 500:
            return SendResult.unknown(f"zernio {status}")
        if status >= 400:
            return SendResult.explicit_failure(_http_failure(status, payload))
        message_id = _string(_data(payload).get("commentId"))
        if not message_id:
            return SendResult.unknown("zernio success response missing commentId")
        return SendResult.success(message_id, _sent_at(payload))

    def _direct_message(self, request: OutboundRequest) -> SendResult:
        conversation_id = str(request.conversation_id or "").strip()
        if not conversation_id:
            return SendResult.explicit_failure("direct message requires a conversation id")
        check = self._get(f"/inbox/conversations/{_quote(conversation_id)}")
        if isinstance(check, SendResult):
            return check
        status, payload = check
        if status >= 500:
            return SendResult.explicit_failure(f"zernio {status} before send")
        if status >= 400:
            return SendResult.explicit_failure(f"zernio {status}")
        record = _data(payload)
        participant = _string(record.get("participantId"))
        account = _string(record.get("accountId"))
        if not participant or participant != request.person_id or (account and account != self.account_id):
            return SendResult.explicit_failure("conversation does not match the comment author")
        post = self._post(
            f"/inbox/conversations/{_quote(conversation_id)}/messages",
            {"accountId": self.account_id, "message": request.body},
        )
        status, payload, failed = post
        if failed is not None:
            return failed
        if status >= 500:
            return SendResult.unknown(f"zernio {status}")
        if status >= 400:
            return SendResult.explicit_failure(f"zernio {status}")
        message_id = _string(_data(payload).get("messageId"))
        if not message_id:
            return SendResult.unknown("zernio success response missing messageId")
        return SendResult.success(message_id, _sent_at(payload))

    def _provider_post_id(self, post_id: str) -> str:
        """Instagram shortcodes are not the ids Zernio uses in comment routes."""
        if not _SHORTCODE.match(post_id) or post_id.isdigit():
            return post_id
        check = self._get("/inbox/comments", {"platform": "instagram", "limit": "50"})
        if isinstance(check, SendResult):
            return post_id
        status, payload = check
        if status >= 400:
            return post_id
        for row in _rows(payload):
            permalink = str(row.get("permalink") or "")
            if post_id in permalink:
                found = _string(row.get("id"))
                if found:
                    return found
        return post_id

    def _get(self, path: str, extra: dict | None = None):
        query = {"accountId": self.account_id}
        if extra:
            query.update(extra)
        query = urllib.parse.urlencode(query)
        try:
            status, payload = self._transport(
                "GET",
                f"{self.base_url}{path}?{query}",
                self._headers(),
                None,
                self.timeout,
            )
        except (TimeoutError, OSError):
            return SendResult.explicit_failure("connection failed before the send was written")
        return status, payload if isinstance(payload, dict) else {}

    def _post(self, path: str, body: dict):
        try:
            status, payload = self._transport(
                "POST",
                f"{self.base_url}{path}",
                self._headers(),
                body,
                self.timeout,
            )
        except TimeoutError:
            return 0, {}, SendResult.unknown("timeout")
        except ConnectionRefusedError:
            return 0, {}, SendResult.explicit_failure("connection failed before the send was written")
        except OSError:
            return 0, {}, SendResult.unknown("response lost after the request was written")
        return status, payload if isinstance(payload, dict) else {}, None

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }


def _urllib_transport(method: str, url: str, headers: dict, body: dict | None, timeout: float):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, _read_json(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, _read_json(exc.read())
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, TimeoutError):
            raise TimeoutError(str(reason)) from exc
        if isinstance(reason, ConnectionRefusedError):
            raise ConnectionRefusedError(str(reason)) from exc
        raise OSError(str(reason)) from exc


def _read_json(raw: bytes) -> dict:
    if not raw:
        return {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


_SHORTCODE = re.compile(r"^[A-Za-z0-9_-]{11,12}$")


def _rows(payload: dict) -> list[dict]:
    for key in ("comments", "posts", "data", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _http_failure(status: int, payload: dict) -> str:
    message = _string(payload.get("message") or payload.get("error") or payload.get("detail"))
    if message:
        return f"zernio {status}: {message[:180]}"
    return f"zernio {status}"


def _quote(value: str) -> str:
    return urllib.parse.quote(str(value), safe="")


def _data(payload: dict) -> dict:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _string(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _sent_at(payload: dict) -> str | None:
    for source in (payload, _data(payload)):
        stamp = _string(source.get("sentAt") or source.get("createdAt"))
        if stamp:
            return stamp
    return None


def _private_reply_not_retryable(status: int, payload: dict) -> bool:
    details = payload.get("details")
    if not isinstance(details, dict):
        return False
    if details.get("privateReplyConsumed") is True:
        return True
    return status >= 500 and str(details.get("privateReplyOutcome") or "") == "unknown"


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        await value
