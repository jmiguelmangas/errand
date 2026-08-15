import asyncio

from errand_jobs import Errand, InMemoryJobStore, Job, JobStatus, RetryPolicy
from errand_jobs.runner import HookRegistry, Runner


async def _wait_for_terminal(
    tasks: Errand, job_id: str, *, timeout: float = 2.0
) -> Job:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await tasks.get_job(job_id)
        if job is not None and job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


async def test_on_success_fires_with_the_finished_job() -> None:
    tasks = Errand()
    seen: list[Job] = []

    @tasks.on_success
    def record(job: Job) -> None:
        seen.append(job)

    @tasks.task
    def add(a: int, b: int) -> int:
        return a + b

    await tasks.startup()
    try:
        job = tasks.enqueue(add, 2, 3)
        await _wait_for_terminal(tasks, job.id)
    finally:
        await tasks.shutdown()

    assert len(seen) == 1
    assert seen[0].id == job.id
    assert seen[0].status == JobStatus.SUCCEEDED
    assert seen[0].result_repr == "5"


async def test_on_failure_fires_after_retries_exhausted() -> None:
    tasks = Errand()
    seen: list[Job] = []

    @tasks.on_failure
    def record(job: Job) -> None:
        seen.append(job)

    @tasks.task(max_retries=1, retry_backoff="fixed", base_delay=0.01)
    def always_fails() -> None:
        raise RuntimeError("nope")

    await tasks.startup()
    try:
        job = tasks.enqueue(always_fails)
        finished = await _wait_for_terminal(tasks, job.id, timeout=5.0)
        assert finished.status == JobStatus.FAILED
    finally:
        await tasks.shutdown()

    assert len(seen) == 1
    assert seen[0].id == job.id
    assert seen[0].status == JobStatus.FAILED
    assert seen[0].error == "RuntimeError: nope"


async def test_on_retry_fires_once_per_retry() -> None:
    tasks = Errand()
    seen: list[Job] = []

    @tasks.on_retry
    def record(job: Job) -> None:
        seen.append(job)

    attempts_made = 0

    @tasks.task(max_retries=2, retry_backoff="fixed", base_delay=0.01)
    def flaky() -> str:
        nonlocal attempts_made
        attempts_made += 1
        if attempts_made < 3:
            raise RuntimeError("not yet")
        return "ok"

    await tasks.startup()
    try:
        job = tasks.enqueue(flaky)
        finished = await _wait_for_terminal(tasks, job.id, timeout=5.0)
        assert finished.status == JobStatus.SUCCEEDED
    finally:
        await tasks.shutdown()

    assert len(seen) == 2
    assert all(j.id == job.id for j in seen)
    assert all(j.status == JobStatus.PENDING for j in seen)


async def test_multiple_hooks_fire_in_registration_order() -> None:
    tasks = Errand()
    order: list[str] = []

    @tasks.on_success
    def first(job: Job) -> None:
        order.append("first")

    @tasks.on_success
    def second(job: Job) -> None:
        order.append("second")

    @tasks.task
    def noop() -> None:
        pass

    await tasks.startup()
    try:
        job = tasks.enqueue(noop)
        await _wait_for_terminal(tasks, job.id)
    finally:
        await tasks.shutdown()

    assert order == ["first", "second"]


async def test_async_hook_is_awaited() -> None:
    tasks = Errand()
    seen: list[Job] = []

    @tasks.on_success
    async def record(job: Job) -> None:
        await asyncio.sleep(0)
        seen.append(job)

    @tasks.task
    def noop() -> None:
        pass

    await tasks.startup()
    try:
        job = tasks.enqueue(noop)
        await _wait_for_terminal(tasks, job.id)
    finally:
        await tasks.shutdown()

    assert len(seen) == 1
    assert seen[0].id == job.id


async def test_hook_that_raises_does_not_break_job_or_other_hooks() -> None:
    tasks = Errand()
    order: list[str] = []

    @tasks.on_success
    def broken(job: Job) -> None:
        order.append("broken")
        raise RuntimeError("hook bug")

    @tasks.on_success
    def fine(job: Job) -> None:
        order.append("fine")

    @tasks.task
    def noop() -> None:
        pass

    await tasks.startup()
    try:
        job = tasks.enqueue(noop)
        finished = await _wait_for_terminal(tasks, job.id)
    finally:
        await tasks.shutdown()

    assert finished.status == JobStatus.SUCCEEDED
    assert order == ["broken", "fine"]


async def test_slow_hook_does_not_block_worker_or_next_job() -> None:
    # A single worker, and an on_success hook that hangs well past this
    # test's patience. If hooks were awaited inline in the worker path,
    # the second job would never get picked up.
    tasks = Errand(max_workers=1, shutdown_timeout=0.1)
    hook_started = asyncio.Event()

    @tasks.on_success
    async def slow_hook(job: Job) -> None:
        hook_started.set()
        await asyncio.sleep(10)

    @tasks.task
    def noop() -> None:
        pass

    await tasks.startup()
    try:
        first = tasks.enqueue(noop)
        second = tasks.enqueue(noop)

        finished_first = await _wait_for_terminal(tasks, first.id, timeout=2.0)
        finished_second = await _wait_for_terminal(tasks, second.id, timeout=2.0)

        assert finished_first.status == JobStatus.SUCCEEDED
        assert finished_second.status == JobStatus.SUCCEEDED
        assert hook_started.is_set()
    finally:
        # Also proves shutdown() doesn't hang waiting on the permanently
        # stuck hook -- it's cancelled once shutdown_timeout is up.
        await tasks.shutdown()


async def test_on_failure_fires_for_job_cancelled_during_shutdown() -> None:
    tasks = Errand(shutdown_timeout=0.05)
    seen: list[Job] = []

    @tasks.on_failure
    def record(job: Job) -> None:
        seen.append(job)

    started = asyncio.Event()

    @tasks.task
    async def stuck() -> None:
        started.set()
        await asyncio.sleep(10)

    await tasks.startup()
    job = tasks.enqueue(stuck)
    await asyncio.wait_for(started.wait(), timeout=1.0)

    await tasks.shutdown()

    assert len(seen) == 1
    assert seen[0].id == job.id
    assert seen[0].error == "Cancelled during shutdown"


def test_hook_decorators_return_the_original_function() -> None:
    tasks = Errand()

    def my_hook(job: Job) -> None:
        pass

    assert tasks.on_success(my_hook) is my_hook
    assert tasks.on_failure(my_hook) is my_hook
    assert tasks.on_retry(my_hook) is my_hook


async def test_hooks_registered_directly_on_runner_also_fire() -> None:
    seen: list[Job] = []
    fired = asyncio.Event()

    def record(job: Job) -> None:
        seen.append(job)
        fired.set()

    hooks = HookRegistry(on_success=[record])
    store = InMemoryJobStore()
    runner = Runner(store, hooks=hooks)
    await runner.start()
    try:

        def noop() -> None:
            pass

        job = Job(name="noop")
        runner.submit_sync(job, noop, (), {}, RetryPolicy())

        await asyncio.wait_for(fired.wait(), timeout=2.0)
    finally:
        await runner.stop()

    assert len(seen) == 1
    assert seen[0].id == job.id
