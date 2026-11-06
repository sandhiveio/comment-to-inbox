"""Discover posts and add them to the project config as disabled.

Discovery is not scheduled. It does not read comments and does not enqueue
outbound actions. An operator enables a post in the config before this
process monitors it.
"""

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DiscoveredPost:
    id: str
    url: str


class FakePostSource:
    """In-memory pages. ``pages`` is a list of post lists, one page at a time."""

    def __init__(self, pages: list[list[DiscoveredPost]]) -> None:
        self.pages = list(pages)

    def list_page(self, account_id: str, cursor: str | None) -> tuple[list[DiscoveredPost], str | None]:
        index = 0 if cursor is None else int(cursor)
        if index >= len(self.pages):
            return [], None
        nxt = None if index + 1 >= len(self.pages) else str(index + 1)
        return list(self.pages[index]), nxt


def sync_posts(config_path: str | Path, source, *, apply: bool) -> tuple[list[DiscoveredPost], list[str]]:
    """Return ``(new posts, lines to print)``. ``apply`` writes disabled entries."""
    path = Path(config_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    account_id = str(raw["account_id"])
    existing = _existing_ids(raw)
    found: list[DiscoveredPost] = []
    seen: set[str] = set()
    cursor = None
    while True:
        page, cursor = source.list_page(account_id, cursor)
        for post in page:
            if not post.id or post.id in seen:
                continue
            seen.add(post.id)
            if post.id in existing:
                continue
            found.append(post)
        if not cursor:
            break
    lines = []
    if not found:
        lines.append("sync_posts new=0")
        return found, lines
    for post in found:
        lines.append(f"sync_posts post={post.id} enabled=false url={post.url}")
    if not apply:
        lines.append(f"sync_posts preview new={len(found)}")
        return found, lines
    posts = list(raw.get("posts") or [])
    for post in found:
        posts.append({"id": post.id, "url": post.url, "enabled": False})
    raw["posts"] = posts
    _write_atomic(path, raw)
    lines.append(f"sync_posts applied new={len(found)}")
    return found, lines


def _existing_ids(raw: dict) -> set[str]:
    found = set()
    for item in raw.get("posts") or []:
        if isinstance(item, str):
            found.add(item)
        else:
            found.add(str(item.get("id") or ""))
    for item in raw.get("post_ids") or []:
        found.add(str(item))
    found.discard("")
    return found


def _write_atomic(path: Path, raw: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
