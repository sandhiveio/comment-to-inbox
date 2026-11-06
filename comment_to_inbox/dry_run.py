"""Read Instagram comments through Zernio and describe the prototype outcome.

This command never replies, sends a direct message, exports a lead, or
notifies an operator. It only lists comments and says what ingest would do.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from comment_to_inbox.config import load_config
from comment_to_inbox.parsing import (
    DECISION_ALLOW,
    DECISION_BLOCK,
    DECISION_ECHO,
    DECISION_ESCALATE,
    DECISION_OWN,
    comment_decision,
)


def run_dry(config_path: str) -> int:
    raw = json.loads(Path(config_path).read_text(encoding="utf-8"))
    base = str(raw.get("zernio_base_url", "https://zernio.com/api/v1")).rstrip("/")
    key = str(raw.get("zernio_api_key", "")).strip()
    username = str(raw["account_username"]).strip().lstrip("@")
    try:
        config = load_config(config_path)
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}")
        return 2
    posts = list(raw["posts"])
    if not key:
        print("error: zernio_api_key is empty")
        return 2
    if not config.comment_allow or not posts:
        print("error: comment allowlist and posts are required")
        return 2

    accounts = _get(base, key, "/accounts", {"platform": "instagram"})
    account = _match_account(accounts, username)
    if account is None:
        names = ", ".join(_account_label(item) for item in _rows(accounts)) or "(none)"
        print(f"No connected Instagram account matches @{username}. Connected: {names}")
        return 1
    account_id = account["id"]
    print(
        f"Dry run for @{account.get('username') or username} "
        f"(account {account_id}). Allowlist: {', '.join(config.comment_allow)}. "
        "Any letter case matches. No messages will be sent."
    )

    seen_people: set[str] = set()
    blocked_people: set[str] = set()
    escalated_people: set[str] = set()
    for post in posts:
        shortcode = str(post.get("id") or post.get("shortcode"))
        url = str(post.get("url") or f"https://www.instagram.com/p/{shortcode}/")
        print(f"\nPost {shortcode} {url}")
        if isinstance(post, dict) and post.get("enabled") is False:
            print("  Disabled. Existing comments are not turned into outbound actions.")
            continue
        payload = _comments_for_post(base, key, account_id, shortcode, url)
        if payload is None:
            print("  Could not read comments for this post.")
            continue
        comments = _flatten(payload)
        if not comments:
            print("  No comments returned.")
            continue
        for comment in comments:
            line = _describe(
                comment,
                config,
                account_username=username,
                seen_people=seen_people,
                blocked_people=blocked_people,
                escalated_people=escalated_people,
            )
            print("  " + line)
    return 0


def _comments_for_post(base: str, key: str, account_id: str, shortcode: str, url: str):
    listing = _get_ok(
        base,
        key,
        "/inbox/comments",
        {"accountId": account_id, "platform": "instagram", "limit": "50"},
    )
    post_id = None if listing is None else _post_id_from_listing(listing, shortcode, url)
    if post_id:
        found = _get_ok(
            base, key, f"/inbox/comments/{urllib.parse.quote(post_id)}", {"accountId": account_id}
        )
        if found is not None:
            return found
    return _get_ok(
        base, key, f"/inbox/comments/{urllib.parse.quote(shortcode)}", {"accountId": account_id}
    )


def _post_id_from_listing(payload: dict, shortcode: str, url: str) -> str | None:
    for row in _rows(payload):
        permalink = str(row.get("permalink") or "")
        if shortcode in permalink or url.rstrip("/") in permalink.rstrip("/"):
            return str(row.get("id") or "")
    return None


def _describe(
    comment: dict,
    config,
    *,
    account_username: str,
    seen_people: set[str],
    blocked_people: set[str],
    escalated_people: set[str],
) -> str:
    author = comment.get("from") or {}
    name = author.get("username") or author.get("name") or author.get("id") or "unknown"
    text = str(comment.get("message") or "").replace("\n", " ")
    when = comment.get("createdTime") or "unknown time"
    person = str(author.get("id") or name)
    preview = text if len(text) <= 140 else text[:137] + "..."
    prefix = f"@{name} at {when}: \"{preview}\"."
    author_name = str(author.get("username") or "").lstrip("@")
    is_own = bool(author.get("isOwner")) or _same_account(author_name, account_username)
    decision, _phrase = comment_decision(
        text,
        is_own=is_own,
        echo_texts=config.public_reply_variants,
        blocklist=config.blocklist,
        escalation=config.escalation,
        allowlist=config.comment_allow,
        keyword_typo_escalation=config.keyword_typo_escalation,
    )
    if decision == DECISION_OWN:
        return prefix + " Skipped: this is the account's own comment."
    if decision == DECISION_ECHO:
        return prefix + " Skipped: this repeats an outbound reply."
    if decision == DECISION_BLOCK:
        blocked_people.add(person)
        return (
            prefix
            + " Would block. The opt-out is stored, the run stops, and no automatic reply is sent."
        )
    if decision == DECISION_ESCALATE:
        if person not in escalated_people:
            escalated_people.add(person)
            return (
                prefix
                + " Would open one operator handoff and send no automatic reply. "
                + "The operator handles the conversation manually."
            )
        return prefix + " Ignored: an operator handoff is already active for this person."
    if decision != DECISION_ALLOW:
        shown = ", ".join(repr(phrase) for phrase in config.comment_allow)
        return prefix + f" Ignored: none of {shown} is a whole word or phrase."
    if person in blocked_people:
        return prefix + " Ignored: this person already opted out."
    if person in escalated_people:
        return prefix + " Ignored: an operator handoff is already active for this person."
    if person in seen_people:
        return prefix + " Ignored: this person already started a run with an earlier matching comment."
    seen_people.add(person)
    return (
        prefix
        + " Would accept. One run would start: a private reply asking for an email, "
        + "then a public reply under the comment after that private reply is confirmed. "
        + "Nothing is sent in this dry run."
    )


def _same_account(author_name: str, account_username: str) -> bool:
    if not author_name or not account_username:
        return False
    return author_name.casefold() == account_username.casefold()


def _flatten(payload: dict) -> list[dict]:
    comments = list(payload.get("comments") or [])
    comments.sort(key=lambda item: str(item.get("createdTime") or ""))
    return comments


def _match_account(payload: dict, username: str) -> dict | None:
    wanted = username.casefold()
    for item in _rows(payload):
        label = str(item.get("username") or item.get("displayName") or "").lstrip("@").casefold()
        if label == wanted:
            account_id = item.get("id") or item.get("_id")
            if account_id:
                return {"id": str(account_id), "username": item.get("username") or username}
    return None


def _account_label(item: dict) -> str:
    return "@" + str(item.get("username") or item.get("displayName") or item.get("id") or item.get("_id") or "?")


def _rows(payload: dict) -> list[dict]:
    for key in ("accounts", "data", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _get_ok(base: str, key: str, path: str, query: dict) -> dict | None:
    try:
        return _get(base, key, path, query)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        print(f"  Zernio {exc.code} on {path}: {detail}")
        return None
    except urllib.error.URLError as exc:
        print(f"  Zernio could not be reached on {path}: {exc.reason}")
        return None


def _get(base: str, key: str, path: str, query: dict) -> dict:
    url = base + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
