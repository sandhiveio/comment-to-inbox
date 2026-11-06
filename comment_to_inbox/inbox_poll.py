"""Pull Zernio comments and incoming messages into the same ingest path the worker sends from."""

import json
import logging
import sqlite3
import urllib.error
import urllib.parse
import urllib.request

from comment_to_inbox.config import ProjectConfig
from comment_to_inbox.ingest import accept_event

LOGGER = logging.getLogger(__name__)
INBOX_POLL_SECONDS = 20


class ZernioInbox:
    def __init__(self, config: ProjectConfig, transport=None) -> None:
        self.config = config
        self.base = str(config.zernio_base_url).rstrip("/")
        self.key = str(config.zernio_api_key).strip()
        self.account_id = str(config.account_id).strip()
        self._transport = transport or _urllib_get

    def poll(self, connection: sqlite3.Connection, now: str) -> int:
        return apply_events(connection, self.config, self.collect(), now)

    def collect(self) -> list[dict]:
        events: list[dict] = []
        for post_id in self.config.post_ids:
            events.extend(self._comments_for(post_id))
        events.extend(self._incoming_messages())
        return events

    def _comments_for(self, shortcode: str) -> list[dict]:
        listing = self._get(
            "/inbox/comments",
            {"accountId": self.account_id, "platform": "instagram", "limit": "50"},
        )
        fetch_id = shortcode
        if listing is not None:
            resolved = _post_id_from_listing(listing, shortcode)
            if resolved:
                fetch_id = resolved
        payload = self._get(
            f"/inbox/comments/{urllib.parse.quote(fetch_id)}",
            {"accountId": self.account_id},
        )
        if payload is None and fetch_id != shortcode:
            payload = self._get(
                f"/inbox/comments/{urllib.parse.quote(shortcode)}",
                {"accountId": self.account_id},
            )
        if payload is None:
            return []
        events = []
        for comment in _comments(payload):
            event = _comment_event(comment, self.account_id, shortcode)
            if event is not None:
                events.append(event)
        return events

    def _incoming_messages(self) -> list[dict]:
        listing = self._get("/inbox/conversations", {"accountId": self.account_id, "limit": "50"})
        if listing is None:
            return []
        events = []
        for conversation in _rows(listing, "conversations"):
            conversation_id = str(conversation.get("id") or "").strip()
            if not conversation_id:
                continue
            payload = self._get(
                f"/inbox/conversations/{urllib.parse.quote(conversation_id)}/messages",
                {"accountId": self.account_id},
            )
            if payload is None:
                continue
            for message in _rows(payload, "messages"):
                event = _message_event(message, self.account_id, conversation_id)
                if event is not None:
                    events.append(event)
        return events

    def _get(self, path: str, query: dict) -> dict | None:
        try:
            return self._transport(self.base, self.key, path, query)
        except Exception as exc:
            LOGGER.error("inbox read failed path=%s error=%s", path, exc)
            return None


def apply_events(connection: sqlite3.Connection, config: ProjectConfig, events: list[dict], now: str) -> int:
    accepted = 0
    for event in events:
        if _accept(connection, config, event, now):
            accepted += 1
    return accepted


def _accept(connection, config, event, now) -> bool:
    result = accept_event(connection, config, event, now)
    disposition = result.get("status")
    if disposition == "duplicate":
        return False
    LOGGER.info(
        "inbox event kind=%s external_id=%s disposition=%s",
        event["kind"],
        event["external_id"],
        disposition,
    )
    return True


def _comment_event(comment: dict, account_id: str, post_id: str) -> dict | None:
    author = comment.get("from") if isinstance(comment.get("from"), dict) else {}
    comment_id = str(comment.get("id") or "").strip()
    person_id = str(author.get("id") or "").strip()
    occurred_at = str(comment.get("createdTime") or "").strip()
    body = str(comment.get("message") or "")
    if not comment_id or not person_id or not occurred_at:
        return None
    return {
        "account_id": account_id,
        "external_id": comment_id,
        "kind": "comment",
        "person_id": person_id,
        "post_id": post_id,
        "comment_id": comment_id,
        "body": body,
        "occurred_at": occurred_at,
    }


def _message_event(message: dict, account_id: str, conversation_id: str) -> dict | None:
    direction = str(message.get("direction") or "").strip().casefold()
    if direction not in ("incoming", "inbound"):
        return None
    message_id = str(message.get("id") or "").strip()
    person_id = str(message.get("senderId") or "").strip()
    occurred_at = str(message.get("createdAt") or message.get("createdTime") or "").strip()
    body = str(message.get("message") or message.get("text") or "")
    if not message_id or not person_id or not occurred_at:
        return None
    return {
        "account_id": account_id,
        "external_id": message_id,
        "kind": "message",
        "person_id": person_id,
        "body": body,
        "occurred_at": occurred_at,
        "conversation_id": str(message.get("conversationId") or conversation_id),
    }


def _post_id_from_listing(payload: dict, shortcode: str) -> str | None:
    for row in _rows(payload, "comments"):
        permalink = str(row.get("permalink") or "")
        if shortcode and shortcode in permalink:
            found = str(row.get("id") or "").strip()
            if found:
                return found
    return None


def _comments(payload: dict) -> list[dict]:
    comments = [item for item in (payload.get("comments") or []) if isinstance(item, dict)]
    comments.sort(key=lambda item: str(item.get("createdTime") or ""))
    return comments


def _rows(payload: dict, preferred: str) -> list[dict]:
    for key in (preferred, "data", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _urllib_get(base: str, key: str, path: str, query: dict) -> dict:
    url = base + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"zernio {exc.code} on {path}: {detail}") from exc
