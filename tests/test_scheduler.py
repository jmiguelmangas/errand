import asyncio
from datetime import datetime, timedelta

from errand_jobs.scheduler import Scheduler


def test_interval_schedule_fires_expected_number_of_times_over_window() -> None:
    start = datetime(2024, 1, 1, 0, 0, 0)
    scheduler = Scheduler(now=lambda: start)
    fire_count = 0

    def record() -> None:
        nonlocal fire_count
        fire_count += 1

    scheduler.add_interval("heartbeat", 10.0, record)

    # First fire is one interval out (t=10s), so a 55s window covers
    # fires at t=10,20,30,40,50 -> 5 fires.
    now = start
    for i in range(1, 56):
        now = start + timedelta(seconds=i)
        scheduler.tick(now)

    assert fire_count == 5


def test_interval_schedule_does_not_fire_before_first_interval() -> None:
    scheduler = Scheduler(now=lambda: datetime(2024, 1, 1))
    fire_count = 0

    def record() -> None:
        nonlocal fire_count
        fire_count += 1

    start = datetime(2024, 1, 1)
    scheduler.add_interval("heartbeat", 10.0, record)

    scheduler.tick(start + timedelta(seconds=5))
    assert fire_count == 0

    scheduler.tick(start + timedelta(seconds=10))
    assert fire_count == 1


def test_cron_schedule_fires_at_expected_minute() -> None:
    start = datetime(2024, 1, 1, 12, 30)
    scheduler = Scheduler(now=lambda: start)
    fire_count = 0

    def record() -> None:
        nonlocal fire_count
        fire_count += 1

    scheduler.add_cron("hourly", "0 * * * *", record)

    scheduler.tick(datetime(2024, 1, 1, 12, 59))
    assert fire_count == 0

    scheduler.tick(datetime(2024, 1, 1, 13, 0))
    assert fire_count == 1

    scheduler.tick(datetime(2024, 1, 1, 13, 30))
    assert fire_count == 1

    scheduler.tick(datetime(2024, 1, 1, 14, 0))
    assert fire_count == 2


def test_at_schedule_fires_exactly_once() -> None:
    start = datetime(2024, 1, 1, 0, 0)
    scheduler = Scheduler(now=lambda: start)
    fire_count = 0

    def record() -> None:
        nonlocal fire_count
        fire_count += 1

    target = datetime(2024, 1, 1, 0, 5)
    scheduler.add_at("one-shot", target, record)

    scheduler.tick(datetime(2024, 1, 1, 0, 4))
    assert fire_count == 0

    scheduler.tick(datetime(2024, 1, 1, 0, 5))
    assert fire_count == 1

    scheduler.tick(datetime(2024, 1, 1, 0, 6))
    scheduler.tick(datetime(2024, 1, 1, 1, 0))
    assert fire_count == 1


def test_seconds_until_next_wake_reflects_earliest_schedule() -> None:
    start = datetime(2024, 1, 1)
    scheduler = Scheduler(now=lambda: start)
    scheduler.add_interval("slow", 100.0, lambda: None)
    scheduler.add_interval("fast", 10.0, lambda: None)

    assert scheduler.seconds_until_next_wake(start) == 10.0


def test_seconds_until_next_wake_capped_at_max_tick_with_no_schedules() -> None:
    scheduler = Scheduler()
    now = datetime(2024, 1, 1)
    assert scheduler.seconds_until_next_wake(now) == 60.0


def test_seconds_until_next_wake_never_negative() -> None:
    start = datetime(2024, 1, 1)
    scheduler = Scheduler(now=lambda: start)
    scheduler.add_interval("overdue", 5.0, lambda: None)

    # Ask well past the next fire time -- must not return a negative sleep.
    assert scheduler.seconds_until_next_wake(start + timedelta(seconds=100)) == 0.0


async def test_start_and_stop_lifecycle_ticks_and_fires() -> None:
    fire_count = 0

    def record() -> None:
        nonlocal fire_count
        fire_count += 1

    scheduler = Scheduler()
    scheduler.add_interval("fast", 0.02, record)

    await scheduler.start()
    try:
        await asyncio.sleep(0.15)
    finally:
        await scheduler.stop()

    assert fire_count >= 2


async def test_start_is_idempotent() -> None:
    scheduler = Scheduler()
    await scheduler.start()
    first_task = scheduler._task
    await scheduler.start()
    assert scheduler._task is first_task
    await scheduler.stop()


async def test_stop_without_start_is_a_noop() -> None:
    scheduler = Scheduler()
    await scheduler.stop()  # must not raise
