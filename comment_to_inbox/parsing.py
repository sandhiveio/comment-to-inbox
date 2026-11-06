import re
import unicodedata

EMAIL_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._%+\-]*@[A-Za-z0-9][A-Za-z0-9.\-]*\.[A-Za-z]{2,}"
)
URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)

# Block, then escalation, then allow. Ordinary text is "ignore" for a comment
# and "ordinary" for a direct message.
DECISION_OWN = "own"
DECISION_ECHO = "echo"
DECISION_BLOCK = "block"
DECISION_ESCALATE = "escalate"
DECISION_ALLOW = "allow"
DECISION_IGNORE = "ignore"
DECISION_ORDINARY = "ordinary"


def normalize_text(text: str) -> str:
    """NFKC, straight apostrophes, collapsed whitespace, then casefold."""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = (
        normalized.replace("\u00a0", " ")
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("`", "'")
    )
    normalized = re.sub(r"\s+", " ", normalized.strip())
    return normalized.casefold()


def normalize_command(text: str) -> str:
    """Same normalization used before phrase matching."""
    return normalize_text(text)


def first_email(text: str) -> str | None:
    """Return the first syntactically valid email, lowercased."""
    match = EMAIL_RE.search(text)
    if match is None:
        return None
    return match.group(0).casefold()


def _mask_emails_and_urls(text: str) -> str:
    """Blank email addresses and URLs so a phrase inside them cannot match."""

    def blank(match: re.Match) -> str:
        return " " * (match.end() - match.start())

    return URL_RE.sub(blank, EMAIL_RE.sub(blank, text))


def phrase_matches(text: str, phrase: str) -> bool:
    """Whole-word or contiguous-phrase match after normalization.

    The phrase may sit anywhere in the text. A match inside an email address
    or a URL does not count. ``stop`` matches ``Pricing, STOP!`` and does not
    match ``stopwatch`` or ``stop@example.com``.
    """
    haystack = _mask_emails_and_urls(normalize_text(text))
    needle = normalize_text(phrase)
    if not needle:
        return False
    return re.search(rf"(?<!\w){re.escape(needle)}(?!\w)", haystack) is not None


def keyword_matches(text: str, keyword: str) -> bool:
    """Case-insensitive token match. ``guide`` matches ``the GUIDE!``."""
    return phrase_matches(text, keyword)


def standalone_command(text: str, commands: tuple[str, ...] | list[str]) -> str | None:
    """Match a command that is the whole message.

    Case, whitespace, and trailing punctuation are normalized. A command
    inside a longer sentence, a URL, or an email address does not match.
    ``STOP!`` matches ``stop``. ``Is this spam?`` does not.
    """
    normalized = normalize_text(text)
    normalized = re.sub(r"[\s.!,?;:\u2026]+$", "", normalized).strip()
    if not normalized:
        return None
    for command in commands:
        needle = normalize_text(command)
        if needle and needle == normalized:
            return needle
    return None


def one_edit_keyword(text: str, phrases: tuple[str, ...] | list[str]) -> str | None:
    """A whole word one edit away from an allowlist phrase.

    One edit is an insertion, a deletion, a substitution, or a swap of two
    adjacent characters. Phrases shorter than four characters are skipped:
    a one-character miss on a short word matches too much ordinary text.
    Text inside an email address or a URL is ignored. An exact allowlist
    match is not reported here.
    """
    haystack = _mask_emails_and_urls(normalize_text(text))
    tokens = re.findall(r"\w+", haystack, flags=re.UNICODE)
    if not tokens:
        return None
    needles = []
    for phrase in phrases:
        needle = normalize_text(phrase)
        if len(needle) < 4 or not needle:
            continue
        needles.append(needle)
    for needle in sorted(needles, key=len, reverse=True):
        parts = needle.split(" ")
        width = len(parts)
        for start in range(0, len(tokens) - width + 1):
            window = " ".join(tokens[start : start + width])
            if window == needle:
                continue
            if _one_edit(window, needle):
                return needle
    return None


def _one_edit(left: str, right: str) -> bool:
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        diffs = [index for index, (a, b) in enumerate(zip(left, right)) if a != b]
        if len(diffs) == 1:
            return True
        if len(diffs) == 2 and diffs[1] == diffs[0] + 1:
            first, second = diffs
            return left[first] == right[second] and left[second] == right[first]
        return False
    longer, shorter = (left, right) if len(left) > len(right) else (right, left)
    index = 0
    skipped = False
    for character in longer:
        if index < len(shorter) and character == shorter[index]:
            index += 1
        elif skipped:
            return False
        else:
            skipped = True
    return True


def first_phrase(text: str, phrases: tuple[str, ...] | list[str]) -> str | None:
    ordered = sorted((phrase for phrase in phrases if phrase.strip()), key=len, reverse=True)
    for phrase in ordered:
        if phrase_matches(text, phrase):
            return normalize_text(phrase)
    return None


def comment_decision(
    text: str,
    *,
    is_own: bool,
    echo_texts: tuple[str, ...] | list[str],
    blocklist: tuple[str, ...] | list[str],
    escalation: tuple[str, ...] | list[str],
    allowlist: tuple[str, ...] | list[str],
    keyword_typo_escalation: bool = False,
) -> tuple[str, str | None]:
    """Classify one comment. The same function is used by ingest and dry run."""
    if is_own:
        return DECISION_OWN, None
    normalized = normalize_text(text)
    for echo in echo_texts:
        if echo.strip() and normalize_text(echo) == normalized:
            return DECISION_ECHO, None
    blocked = standalone_command(text, blocklist)
    if blocked is not None:
        return DECISION_BLOCK, blocked
    escalated = first_phrase(text, escalation)
    if escalated is not None:
        return DECISION_ESCALATE, escalated
    allowed = first_phrase(text, allowlist)
    if allowed is not None:
        return DECISION_ALLOW, allowed
    if keyword_typo_escalation:
        typo = one_edit_keyword(text, allowlist)
        if typo is not None:
            return DECISION_ESCALATE, typo
    return DECISION_IGNORE, None


def message_decision(
    text: str,
    *,
    blocklist: tuple[str, ...] | list[str],
    escalation: tuple[str, ...] | list[str],
) -> tuple[str, str | None]:
    """Classify one direct message. Allowlist phrases do not start a run."""
    blocked = standalone_command(text, blocklist)
    if blocked is not None:
        return DECISION_BLOCK, blocked
    escalated = first_phrase(text, escalation)
    if escalated is not None:
        return DECISION_ESCALATE, escalated
    return DECISION_ORDINARY, None
