import asyncio
from datetime import datetime, timedelta, timezone

from errand_jobs import Errand, JobStatus


async def _wait_for_terminal(tasks: Errand, job_id: str, *, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await tasks.get_job(job_id)
        if job is not None and job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


async def test_prune_after_none_registers_no_schedule() -> None:
    tasks = Errand()
    assert tasks._scheduler._schedules == []


def test_prune_after_registers_a_schedule() -> None:
    tasks = Errand(prune_after=3600.0)
    assert len(tasks._scheduler._schedules) == 1
    schedule = tasks._scheduler._schedules[0]
    assert schedule.interval_seconds == 60.0  # capped at the max check interval


def test_prune_after_shorter_than_max_check_interval_uses_it_directly() -> None:
    tasks = Errand(prune_after=10.0)
    schedule = tasks._scheduler._schedules[0]
    assert schedule.interval_seconds == 10.0


async def test_prune_after_removes_old_terminal_jobs_only() -> None:
    tasks = Errand(prune_after=60.0)

    @tasks.task
    def noop() -> None:
        pass

    await tasks.startup()
    try:
        old_job = tasks.enqueue(noop)
        await _wait_for_terminal(tasks, old_job.id)
        recent_job = tasks.enqueue(noop)
        await _wait_for_terminal(tasks, recent_job.id)

        # Backdate only the first job's finish time to simulate it being old.
        stale = await tasks.get_job(old_job.id)
        assert stale is not None
        stale.finished_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        await tasks._store.update(stale)

        # Trigger the prune schedule deterministically instead of waiting
        # for the real check interval, then await the triggered prune
        # task directly rather than polling for it to finish.
        next_fire = tasks._scheduler._schedules[0].next_fire
        assert next_fire is not None
        tasks._scheduler.tick(next_fire)
        assert len(tasks._background) == 1
        await next(iter(tasks._background))
    finally:
        await tasks.shutdown()

    assert await tasks.get_job(old_job.id) is None
    assert await tasks.get_job(recent_job.id) is not None
