from datetime import datetime

import pytest

from errand_jobs import CronParseError
from errand_jobs.cron import CronSchedule


def test_wildcard_every_minute() -> None:
    schedule = CronSchedule.parse("* * * * *")
    now = datetime(2024, 1, 1, 12, 30, 15)
    assert schedule.next_after(now) == datetime(2024, 1, 1, 12, 31)


def test_single_value_hourly() -> None:
    schedule = CronSchedule.parse("0 * * * *")
    assert schedule.next_after(datetime(2024, 1, 1, 12, 30)) == datetime(
        2024, 1, 1, 13, 0
    )
    assert schedule.next_after(datetime(2024, 1, 1, 12, 0)) == datetime(
        2024, 1, 1, 13, 0
    )


def test_list_of_values() -> None:
    schedule = CronSchedule.parse("0,15,30,45 * * * *")
    assert schedule.next_after(datetime(2024, 1, 1, 12, 1)) == datetime(
        2024, 1, 1, 12, 15
    )
    assert schedule.next_after(datetime(2024, 1, 1, 12, 46)) == datetime(
        2024, 1, 1, 13, 0
    )


def test_range_of_values() -> None:
    schedule = CronSchedule.parse("0 9-17 * * *")
    assert schedule.next_after(datetime(2024, 1, 1, 8, 0)) == datetime(2024, 1, 1, 9, 0)
    assert schedule.next_after(datetime(2024, 1, 1, 17, 30)) == datetime(
        2024, 1, 2, 9, 0
    )


def test_step_values() -> None:
    schedule = CronSchedule.parse("*/15 * * * *")
    assert schedule.next_after(datetime(2024, 1, 1, 12, 1)) == datetime(
        2024, 1, 1, 12, 15
    )
    assert schedule.next_after(datetime(2024, 1, 1, 12, 44)) == datetime(
        2024, 1, 1, 12, 45
    )


def test_day_of_month() -> None:
    schedule = CronSchedule.parse("0 0 1 * *")
    assert schedule.next_after(datetime(2024, 1, 5, 0, 0)) == datetime(2024, 2, 1, 0, 0)


def test_month() -> None:
    schedule = CronSchedule.parse("0 0 1 6 *")
    assert schedule.next_after(datetime(2024, 1, 1, 0, 0)) == datetime(2024, 6, 1, 0, 0)


def test_day_of_week_monday() -> None:
    # 2024-01-01 is a Monday.
    schedule = CronSchedule.parse("0 9 * * 1")
    assert schedule.next_after(datetime(2024, 1, 1, 9, 0)) == datetime(2024, 1, 8, 9, 0)
    assert schedule.next_after(datetime(2024, 1, 1, 0, 0)) == datetime(2024, 1, 1, 9, 0)


def test_day_of_week_sunday_zero_and_seven_are_equivalent() -> None:
    # 2024-01-07 is a Sunday.
    zero = CronSchedule.parse("0 9 * * 0")
    seven = CronSchedule.parse("0 9 * * 7")
    now = datetime(2024, 1, 1, 0, 0)
    assert zero.next_after(now) == datetime(2024, 1, 7, 9, 0)
    assert seven.next_after(now) == datetime(2024, 1, 7, 9, 0)


def test_combined_fields_hourly_on_weekdays() -> None:
    schedule = CronSchedule.parse("0 9 * * 1-5")
    # 2024-01-05 is a Friday, 2024-01-06 is a Saturday.
    assert schedule.next_after(datetime(2024, 1, 5, 9, 0)) == datetime(2024, 1, 8, 9, 0)


@pytest.mark.parametrize(
    "expression",
    [
        "* * * *",  # too few fields
        "* * * * * *",  # too many fields
        "60 * * * *",  # minute out of range
        "* 24 * * *",  # hour out of range
        "* * 32 * *",  # day-of-month out of range
        "* * * 13 *",  # month out of range
        "* * * * 8",  # day-of-week out of range
        "abc * * * *",  # not a number
        "5-2 * * * *",  # inverted range
        "*/0 * * * *",  # zero step
    ],
)
def test_invalid_expressions_raise(expression: str) -> None:
    with pytest.raises(CronParseError):
        CronSchedule.parse(expression)


def test_expression_that_can_never_match_raises_on_next_after() -> None:
    schedule = CronSchedule.parse("0 0 30 2 *")  # February 30th never exists
    with pytest.raises(CronParseError):
        schedule.next_after(datetime(2024, 1, 1))
