import asyncio

import pytest

from errand import Errand, InMemoryJobStore, JobStatus, UnknownTaskError
from errand.models import Job
from errand.runner import Runner


async def _wait_for_terminal(tasks: Errand, job_id: str, *, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await tasks.get_job(job_id)
        if job is not None and job.status in (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
        ):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


async def test_enqueue_sync_task_succeeds() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        @tasks.task
        def add(a: int, b: int) -> int:
            return a + b

        job = tasks.enqueue(add, 2, 3)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "5"
        assert finished.attempts == 1
        assert finished.started_at is not None
        assert finished.finished_at is not None
    finally:
        await tasks.shutdown()


async def test_enqueue_async_task_succeeds() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        @tasks.task
        async def greet(name: str) -> str:
            await asyncio.sleep(0)
            return f"hello {name}"

        job = tasks.enqueue(greet, "world")
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "hello world"
    finally:
        await tasks.shutdown()


async def test_failing_task_captures_error_and_traceback() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        @tasks.task
        def boom() -> None:
            raise ValueError("kaboom")

        job = tasks.enqueue(boom)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.FAILED
        assert finished.error == "ValueError: kaboom"
        assert finished.traceback is not None
        assert "ValueError: kaboom" in finished.traceback
        assert "boom" in finished.traceback
    finally:
        await tasks.shutdown()


async def test_enqueue_by_registered_name() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        @tasks.task(name="custom-name")
        def noop() -> str:
            return "ok"

        job = tasks.enqueue("custom-name")
        finished = await _wait_for_terminal(tasks, job.id)

        assert job.name == "custom-name"
        assert finished.status == JobStatus.SUCCEEDED
    finally:
        await tasks.shutdown()


async def test_enqueue_unregistered_task_raises() -> None:
    tasks = Errand()

    def not_registered() -> None:
        pass

    with pytest.raises(UnknownTaskError):
        tasks.enqueue(not_registered)

    with pytest.raises(UnknownTaskError):
        tasks.enqueue("nope")


async def test_get_job_unknown_returns_none() -> None:
    tasks = Errand()
    assert await tasks.get_job("does-not-exist") is None


async def test_shutdown_drains_in_flight_job() -> None:
    tasks = Errand(shutdown_timeout=2.0)
    await tasks.startup()

    started = asyncio.Event()

    @tasks.task
    async def slow() -> str:
        started.set()
        await asyncio.sleep(0.1)
        return "done"

    job = tasks.enqueue(slow)
    await asyncio.wait_for(started.wait(), timeout=1.0)

    await tasks.shutdown()

    finished = await tasks.get_job(job.id)
    assert finished is not None
    assert finished.status == JobStatus.SUCCEEDED
    assert finished.result_repr == "done"


async def test_shutdown_timeout_fails_stuck_job() -> None:
    tasks = Errand(shutdown_timeout=0.05)
    await tasks.startup()

    started = asyncio.Event()

    @tasks.task
    async def stuck() -> None:
        started.set()
        await asyncio.sleep(10)

    job = tasks.enqueue(stuck)
    await asyncio.wait_for(started.wait(), timeout=1.0)

    await tasks.shutdown()

    finished = await tasks.get_job(job.id)
    assert finished is not None
    assert finished.status == JobStatus.FAILED
    assert finished.error == "Cancelled during shutdown"


async def test_runner_start_is_idempotent() -> None:
    store = InMemoryJobStore()
    runner = Runner(store, max_workers=2)

    await runner.start()
    first_workers = list(runner._workers)
    await runner.start()

    assert runner._workers == first_workers
    await runner.stop()


async def test_runner_stop_without_start_is_a_noop() -> None:
    store = InMemoryJobStore()
    runner = Runner(store)

    await runner.stop()  # must not raise


async def test_runner_fails_jobs_still_queued_on_shutdown() -> None:
    store = InMemoryJobStore()
    runner = Runner(store, max_workers=1)
    await runner.start()

    started = asyncio.Event()

    async def hog() -> None:
        started.set()
        await asyncio.sleep(10)

    hogging_job = Job(name="hog")
    queued_job = Job(name="queued")
    await runner.submit(hogging_job, hog, (), {})
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await runner.submit(queued_job, hog, (), {})

    await runner.stop(drain=True, timeout=0.05)

    hogging_result = await store.get(hogging_job.id)
    queued_result = await store.get(queued_job.id)
    assert hogging_result is not None
    assert queued_result is not None
    assert hogging_result.status == JobStatus.FAILED
    assert queued_result.status == JobStatus.FAILED
    assert queued_result.error == "Cancelled during shutdown"


async def test_custom_store_is_used() -> None:
    store = InMemoryJobStore()
    tasks = Errand(store=store)
    await tasks.startup()
    try:

        @tasks.task
        def ping() -> str:
            return "pong"

        job = tasks.enqueue(ping)
        await _wait_for_terminal(tasks, job.id)

        assert await store.get(job.id) is not None
    finally:
        await tasks.shutdown()
