"""A small, self-contained 5-field cron parser. stdlib-only.

Fields (in order): minute, hour, day-of-month, month, day-of-week. Each
supports ``*``, a single value, a comma-separated list (``1,2,3``), a
range (``1-5``), or a step (``*/5``). Day-of-week follows the POSIX
convention: 0 and 7 both mean Sunday, 1 means Monday, ... 6 means
Saturday.

No third-party cron library. ``CronSchedule.next_after(now)`` finds the
next match by stepping forward one minute at a time, bounded to 5 years
out to avoid spinning forever on an expression that can never match
(e.g. ``0 0 30 2 *`` — February 30th never exists). Simple and
well-tested rather than clever. Known limitation from the fixed 5-year
bound: a schedule that only matches on February 29th could in rare
cases (century years not divisible by 400, e.g. 1900) go up to 8 years
between occurrences, which exceeds the bound. Not worth the added
complexity to handle for a v1 background-job scheduler.

Deviation from POSIX: when both day-of-month and day-of-week are
restricted (neither is ``*``), most cron implementations treat them as
OR'd (fire if *either* matches). This parser always ANDs all five
fields — simpler, and the common case (only one of the two restricted)
behaves identically either way.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from .errors import CronParseError

_FIELD_BOUNDS = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day-of-month": (1, 31),
    "month": (1, 12),
    "day-of-week": (0, 7),
}

_MAX_LOOKAHEAD = timedelta(days=366 * 5)


@dataclass(frozen=True)
class CronSchedule:
    """A parsed 5-field cron expression.

    Example::

        schedule = CronSchedule.parse("0 * * * *")  # top of every hour
        schedule.next_after(datetime(2024, 1, 1, 0, 30))
        # datetime(2024, 1, 1, 1, 0)
    """

    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]

    @classmethod
    def parse(cls, expression: str) -> CronSchedule:
        """Parse a 5-field cron expression. Raises ``CronParseError`` on bad input."""
        fields = expression.split()
        if len(fields) != 5:
            raise CronParseError(
                "Expected 5 space-separated fields (minute hour day-of-month "
                f"month day-of-week), got {len(fields)}: {expression!r}"
            )
        minute_f, hour_f, dom_f, month_f, dow_f = fields
        raw_days_of_week = _parse_field(dow_f, "day-of-week")
        days_of_week = frozenset(0 if d == 7 else d for d in raw_days_of_week)
        return cls(
            minutes=frozenset(_parse_field(minute_f, "minute")),
            hours=frozenset(_parse_field(hour_f, "hour")),
            days_of_month=frozenset(_parse_field(dom_f, "day-of-month")),
            months=frozenset(_parse_field(month_f, "month")),
            days_of_week=days_of_week,
        )

    def next_after(self, now: datetime) -> datetime:
        """The next minute-aligned moment strictly after ``now`` that matches."""
        candidate = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        deadline = now + _MAX_LOOKAHEAD
        while candidate <= deadline:
            if self._matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise CronParseError(
            f"No matching time found within 5 years of {now} for this "
            "cron expression -- it likely can never match (e.g. Feb 30th)"
        )

    def _matches(self, moment: datetime) -> bool:
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.day in self.days_of_month
            and moment.month in self.months
            and (moment.isoweekday() % 7) in self.days_of_week
        )


def _parse_field(field: str, field_name: str) -> set[int]:
    low, high = _FIELD_BOUNDS[field_name]
    values: set[int] = set()
    for token in field.split(","):
        values |= _parse_token(token, low, high, field_name)
    return values


def _parse_token(token: str, low: int, high: int, field_name: str) -> set[int]:
    if token == "*":
        return set(range(low, high + 1))

    if token.startswith("*/"):
        step = _parse_int(token[2:], token, field_name)
        if step <= 0:
            raise CronParseError(f"Invalid step {token!r} in {field_name} field")
        return set(range(low, high + 1, step))

    if "-" in token:
        start_str, _, end_str = token.partition("-")
        start = _parse_int(start_str, token, field_name)
        end = _parse_int(end_str, token, field_name)
        if start > end:
            raise CronParseError(f"Invalid range {token!r} in {field_name} field")
        _check_bounds(start, token, low, high, field_name)
        _check_bounds(end, token, low, high, field_name)
        return set(range(start, end + 1))

    value = _parse_int(token, token, field_name)
    _check_bounds(value, token, low, high, field_name)
    return {value}


def _parse_int(text: str, token: str, field_name: str) -> int:
    try:
        return int(text)
    except ValueError as exc:
        raise CronParseError(f"Invalid value {token!r} in {field_name} field") from exc


def _check_bounds(value: int, token: str, low: int, high: int, field_name: str) -> None:
    if not (low <= value <= high):
        raise CronParseError(
            f"Value {value} in {token!r} is out of range for {field_name} "
            f"field ({low}-{high})"
        )
