from datetime import datetime, timedelta, timezone

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def normalize_timestamp(value: str) -> str:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value}")
    utc = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return utc.strftime(TIMESTAMP_FORMAT)


def add_seconds(stamp: str, seconds: float) -> str:
    parsed = datetime.strptime(stamp, TIMESTAMP_FORMAT)
    return (parsed + timedelta(seconds=seconds)).strftime(TIMESTAMP_FORMAT)


def seconds_between(earlier: str, later: str) -> float:
    start = datetime.strptime(earlier, TIMESTAMP_FORMAT)
    end = datetime.strptime(later, TIMESTAMP_FORMAT)
    return (end - start).total_seconds()


class SystemClock:
    def now(self) -> str:
        return datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)


class ManualClock:
    def __init__(self, start: str = "2026-01-01T00:00:00.000000Z") -> None:
        self.current = normalize_timestamp(start)

    def now(self) -> str:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current = add_seconds(self.current, seconds)
