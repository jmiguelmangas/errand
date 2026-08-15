import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from errand_jobs import Errand, JobStatus


async def _wait_until(
    predicate, *, timeout: float = 2.0, interval: float = 0.01
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met in time")


async def test_interval_schedule_produces_tracked_jobs() -> None:
    tasks = Errand()
    runs = 0

    @tasks.schedule(interval_seconds=0.03)
    def heartbeat() -> str:
        nonlocal runs
        runs += 1
        return "beat"

    await tasks.startup()
    try:
        await _wait_until(lambda: runs >= 3, timeout=2.0)
    finally:
        await tasks.shutdown()

    jobs = await tasks.list_jobs()
    heartbeat_jobs = [job for job in jobs if job.name == "heartbeat"]
    assert len(heartbeat_jobs) >= 3
    assert all(job.status == JobStatus.SUCCEEDED for job in heartbeat_jobs)
    assert all(job.result_repr == "beat" for job in heartbeat_jobs)


async def test_cron_schedule_produces_tracked_job() -> None:
    # Cron granularity is a minute, so waiting for a real tick would make
    # this test slow/flaky. Instead, drive the scheduler's deterministic
    # tick() seam directly -- the enqueue plumbing it exercises is the
    # same regardless of schedule kind (cron/interval/at), and cron's own
    # firing logic is already covered in test_cron.py and test_scheduler.py.
    tasks = Errand()

    @tasks.schedule(cron="* * * * *")
    def minutely() -> str:
        return "tick"

    await tasks.startup()
    try:
        next_fire = tasks._scheduler._schedules[0].next_fire
        assert next_fire is not None
        tasks._scheduler.tick(next_fire)

        async def _has_tracked_job() -> bool:
            jobs = await tasks.list_jobs()
            return any(job.name == "minutely" for job in jobs)

        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            if await _has_tracked_job():
                break
            await asyncio.sleep(0.01)
    finally:
        await tasks.shutdown()

    jobs = await tasks.list_jobs()
    minutely_jobs = [job for job in jobs if job.name == "minutely"]
    assert len(minutely_jobs) == 1
    assert minutely_jobs[0].status == JobStatus.SUCCEEDED


async def test_at_schedule_fires_once_as_tracked_job() -> None:
    tasks = Errand()
    calls = 0

    @tasks.schedule(at=datetime.now(timezone.utc) + timedelta(milliseconds=20))
    def one_shot() -> str:
        nonlocal calls
        calls += 1
        return "done"

    await tasks.startup()
    try:
        await _wait_until(lambda: calls >= 1, timeout=2.0)
        await asyncio.sleep(0.1)  # give a would-be second fire a chance to (not) happen
    finally:
        await tasks.shutdown()

    assert calls == 1
    jobs = await tasks.list_jobs()
    one_shot_jobs = [job for job in jobs if job.name == "one_shot"]
    assert len(one_shot_jobs) == 1
    assert one_shot_jobs[0].status == JobStatus.SUCCEEDED


def test_schedule_requires_exactly_one_of_cron_interval_at() -> None:
    tasks = Errand()

    with pytest.raises(ValueError, match="exactly one of"):

        @tasks.schedule()
        def no_trigger() -> None:
            pass

    with pytest.raises(ValueError, match="exactly one of"):

        @tasks.schedule(cron="* * * * *", interval_seconds=1.0)
        def two_triggers() -> None:
            pass


def test_schedule_bare_call_form() -> None:
    tasks = Errand()

    def direct() -> None:
        pass

    registered = tasks.schedule(direct, interval_seconds=60.0)

    assert registered is direct
    assert "direct" in tasks._registry
