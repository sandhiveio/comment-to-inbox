import json
import zlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectConfig:
    """Project configuration and the one account binding for this process.

    The task statement does not require one Instagram account to own exactly
    one project. This prototype makes that assumption anyway: a project file
    is also the account binding. ``account_id``, the phrase lists, the post
    list, the reply variants, and the link all live here, and one database
    serves that account only.
    """

    account_id: str
    post_ids: tuple[str, ...]
    link_url: str
    rate_limit_per_minute: int
    send_timeout_seconds: float
    retry_delay_seconds: float
    action_deadline_seconds: float
    comment_allow: tuple[str, ...]
    blocklist: tuple[str, ...]
    escalation: tuple[str, ...]
    public_reply_variants: tuple[str, ...]
    email_request_variants: tuple[str, ...]
    email_retry_variants: tuple[str, ...]
    link_dm_variants: tuple[str, ...]
    email_wait_seconds: float = 604800
    public_fallback_variants: tuple[str, ...] = ()
    enabled_at_by_post: tuple[tuple[str, str], ...] = ()
    account_username: str = ""
    notification_workflow: str = ""
    zernio_base_url: str = ""
    zernio_api_key: str = ""
    keyword_typo_escalation: bool = False

    @property
    def keyword(self) -> str:
        return self.comment_allow[0]

    @property
    def keywords(self) -> tuple[str, ...]:
        return self.comment_allow

    @property
    def public_reply_text(self) -> str:
        return self.public_reply_variants[0]

    @property
    def email_request_text(self) -> str:
        return self.email_request_variants[0]

    @property
    def email_retry_text(self) -> str:
        return self.email_retry_variants[0]

    @property
    def link_dm_text(self) -> str:
        return self.link_dm_variants[0]

    def pick_variant(self, kind: str, slot: int, run_id: int) -> str:
        """Choose one configured variant. The chosen text is stored on the action."""
        if kind == "public_reply":
            pool = self.public_reply_variants
        elif kind == "link_dm":
            pool = self.link_dm_variants
        elif kind == "email_request" and slot == 0:
            pool = self.email_request_variants
        elif kind == "email_request":
            pool = self.email_retry_variants
        elif kind == "public_fallback":
            pool = self.public_fallback_variants
        else:
            raise ValueError(f"unknown action kind {kind}")
        index = zlib.crc32(f"{run_id}:{kind}:{slot}".encode()) % len(pool)
        return pool[index]


def load_config(path: str | Path) -> ProjectConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    allow = _phrases(raw, "comment_allowlist", "comment_allow")
    if not allow:
        allow = _phrases(raw, "keywords", "keyword")
    blocklist = _required_phrases(raw, "blocklist", "comment_blocklist")
    escalation = _required_phrases(raw, "escalation_list", "escalation")
    notifications = raw.get("notifications") or {}
    config = ProjectConfig(
        account_id=str(raw["account_id"]).strip(),
        post_ids=_post_ids(raw),
        link_url=str(raw["link_url"]).strip(),
        rate_limit_per_minute=int(raw["rate_limit_per_minute"]),
        send_timeout_seconds=float(raw["send_timeout_seconds"]),
        retry_delay_seconds=float(raw["retry_delay_seconds"]),
        action_deadline_seconds=float(raw["action_deadline_seconds"]),
        comment_allow=allow,
        blocklist=blocklist,
        escalation=escalation,
        public_reply_variants=_variants(raw, "public_reply_variants", "public_reply_text"),
        email_request_variants=_variants(raw, "email_request_variants", "email_request_text"),
        email_retry_variants=_variants(raw, "email_retry_variants", "email_retry_text"),
        link_dm_variants=_variants(raw, "link_dm_variants", "link_dm_text"),
        email_wait_seconds=float(raw.get("email_wait_seconds", 604800)),
        public_fallback_variants=_optional_variants(raw, "public_fallback_variants"),
        enabled_at_by_post=_enabled_at(raw),
        account_username=str(raw.get("account_username") or "").strip().lstrip("@"),
        notification_workflow=str((notifications.get("workflow") or "")).strip(),
        zernio_base_url=str(raw.get("zernio_base_url") or "").strip().rstrip("/"),
        zernio_api_key=str(raw.get("zernio_api_key") or "").strip(),
        keyword_typo_escalation=_optional_bool(raw, "keyword_typo_escalation"),
    )
    _validate(config)
    return config


def _validate(config: ProjectConfig) -> None:
    if not config.account_id:
        raise ValueError("account_id is required")
    if not config.comment_allow:
        raise ValueError("comment_allowlist is required")
    if not config.post_ids or any(not post for post in config.post_ids):
        raise ValueError("post_ids must list at least one post")
    for name in (
        "public_reply_variants",
        "email_request_variants",
        "email_retry_variants",
        "link_dm_variants",
    ):
        pool = getattr(config, name)
        if not pool or any(not str(item).strip() for item in pool):
            raise ValueError(f"{name} must be a non-empty list")
    if not config.link_url:
        raise ValueError("link_url is required")
    for variant in config.link_dm_variants:
        if config.link_url not in variant:
            raise ValueError("every link_dm variant must contain link_url")
    if config.rate_limit_per_minute < 1:
        raise ValueError("rate_limit_per_minute must be at least 1")
    if config.send_timeout_seconds <= 0:
        raise ValueError("send_timeout_seconds must be positive")
    if config.retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be zero or positive")
    if config.action_deadline_seconds < 0:
        raise ValueError("action_deadline_seconds must be zero or positive")
    if config.email_wait_seconds <= 0:
        raise ValueError("email_wait_seconds must be positive")


def _required_phrases(raw: dict, *keys: str) -> tuple[str, ...]:
    for key in keys:
        if key in raw and raw[key] not in (None, ""):
            return _as_list(raw[key])
    raise ValueError(f"{keys[0]} is required")


def _phrases(raw: dict, *keys: str) -> tuple[str, ...]:
    for key in keys:
        if key not in raw or raw[key] in (None, ""):
            continue
        return _as_list(raw[key])
    return ()


def _variants(raw: dict, plural_key: str, singular_key: str) -> tuple[str, ...]:
    if plural_key in raw:
        return _as_list(raw[plural_key])
    if singular_key in raw:
        return _as_list(raw[singular_key])
    raise ValueError(f"{plural_key} is required")


def _as_list(value) -> tuple[str, ...]:
    if isinstance(value, str):
        value = [value]
    return tuple(str(item).strip() for item in value if str(item).strip())


def _optional_bool(raw: dict, key: str) -> bool:
    if key not in raw or raw[key] in (None, ""):
        return False
    value = raw[key]
    if isinstance(value, bool):
        return value
    raise ValueError(f"{key} must be true or false")


def _optional_variants(raw: dict, key: str) -> tuple[str, ...]:
    if key not in raw or raw[key] in (None, ""):
        return ()
    return _as_list(raw[key])


def _enabled_at(raw: dict) -> tuple[tuple[str, str], ...]:
    found = []
    for item in raw.get("posts") or []:
        if isinstance(item, str) or item.get("enabled") is False:
            continue
        stamp = str(item.get("enabled_at") or "").strip()
        post_id = str(item.get("id") or "").strip()
        if post_id and stamp:
            found.append((post_id, stamp))
    return tuple(found)


def _post_ids(raw: dict) -> tuple[str, ...]:
    if "posts" in raw:
        ids = []
        for item in raw["posts"]:
            if isinstance(item, str):
                ids.append(item.strip())
                continue
            if item.get("enabled") is False:
                continue
            ids.append(str(item["id"]).strip())
        return tuple(item for item in ids if item)
    return _as_list(raw["post_ids"])
