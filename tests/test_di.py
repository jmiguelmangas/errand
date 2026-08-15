import asyncio

from errand_jobs import Depends, Errand, JobStatus, UnsupportedDependencyError


async def _wait_for_terminal(tasks: Errand, job_id: str, *, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await tasks.get_job(job_id)
        if job is not None and job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


async def test_plain_dependency_is_injected() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        def get_greeting() -> str:
            return "hello"

        @tasks.task
        def greet(greeting: str = Depends(get_greeting)) -> str:
            return greeting

        job = tasks.enqueue(greet)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "hello"
    finally:
        await tasks.shutdown()


async def test_async_dependency_is_injected() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        async def get_value() -> int:
            await asyncio.sleep(0)
            return 42

        @tasks.task
        async def use_value(value: int = Depends(get_value)) -> int:
            return value

        job = tasks.enqueue(use_value)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "42"
    finally:
        await tasks.shutdown()


async def test_sync_yield_dependency_torn_down_after_success() -> None:
    tasks = Errand()
    await tasks.startup()
    events: list[str] = []

    def get_resource():
        events.append("open")
        try:
            yield "resource"
        finally:
            events.append("close")

    @tasks.task
    def use_resource(resource: str = Depends(get_resource)) -> str:
        events.append("use")
        return resource

    try:
        job = tasks.enqueue(use_resource)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert events == ["open", "use", "close"]
    finally:
        await tasks.shutdown()


async def test_async_yield_dependency_torn_down_after_success() -> None:
    tasks = Errand()
    await tasks.startup()
    events: list[str] = []

    async def get_resource():
        events.append("open")
        try:
            yield "resource"
        finally:
            events.append("close")

    @tasks.task
    async def use_resource(resource: str = Depends(get_resource)) -> str:
        events.append("use")
        return resource

    try:
        job = tasks.enqueue(use_resource)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert events == ["open", "use", "close"]
    finally:
        await tasks.shutdown()


async def test_yield_dependency_torn_down_after_failure() -> None:
    tasks = Errand()
    await tasks.startup()
    events: list[str] = []

    def get_resource():
        events.append("open")
        try:
            yield "resource"
        finally:
            events.append("close")

    @tasks.task
    def boom(resource: str = Depends(get_resource)) -> None:
        events.append("use")
        raise RuntimeError("nope")

    try:
        job = tasks.enqueue(boom)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.FAILED
        assert finished.error == "RuntimeError: nope"
        assert events == ["open", "use", "close"]
    finally:
        await tasks.shutdown()


async def test_nested_dependencies_are_resolved() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        def get_base() -> int:
            return 10

        def get_doubled(base: int = Depends(get_base)) -> int:
            return base * 2

        @tasks.task
        def use_doubled(doubled: int = Depends(get_doubled)) -> int:
            return doubled

        job = tasks.enqueue(use_doubled)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "20"
    finally:
        await tasks.shutdown()


async def test_dependency_resolved_once_per_job_when_cached() -> None:
    tasks = Errand()
    await tasks.startup()
    calls = 0
    try:

        def get_shared() -> int:
            nonlocal calls
            calls += 1
            return calls

        def get_a(shared: int = Depends(get_shared)) -> int:
            return shared

        def get_b(shared: int = Depends(get_shared)) -> int:
            return shared

        @tasks.task
        def combine(a: int = Depends(get_a), b: int = Depends(get_b)) -> str:
            return f"{a}-{b}"

        job = tasks.enqueue(combine)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "1-1"
        assert calls == 1
    finally:
        await tasks.shutdown()


async def test_dependency_resolved_per_reference_when_not_cached() -> None:
    tasks = Errand()
    await tasks.startup()
    calls = 0
    try:

        def get_shared() -> int:
            nonlocal calls
            calls += 1
            return calls

        @tasks.task
        def combine(
            a: int = Depends(get_shared, use_cache=False),
            b: int = Depends(get_shared, use_cache=False),
        ) -> str:
            return f"{a}-{b}"

        job = tasks.enqueue(combine)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "1-2"
        assert calls == 2
    finally:
        await tasks.shutdown()


async def test_unannotated_plain_argument_is_left_alone() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        @tasks.task
        def add(a, b):
            return a + b

        job = tasks.enqueue(add, 2, 3)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "5"
    finally:
        await tasks.shutdown()


async def test_enqueued_kwarg_overrides_dependency() -> None:
    tasks = Errand()
    await tasks.startup()
    resolved_calls = 0
    try:

        def get_value() -> int:
            nonlocal resolved_calls
            resolved_calls += 1
            return 1

        @tasks.task
        def use_value(value: int = Depends(get_value)) -> int:
            return value

        job = tasks.enqueue(use_value, value=99)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "99"
        assert resolved_calls == 0
    finally:
        await tasks.shutdown()


async def test_security_scopes_marker_raises_unsupported() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        class _FakeSecurity:
            def __init__(self, dependency):
                self.dependency = dependency
                self.scopes: list[str] = []

        def get_user() -> str:
            return "user"

        @tasks.task
        def secure_task(user: str = _FakeSecurity(get_user)) -> str:
            return user

        job = tasks.enqueue(secure_task)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.FAILED
        assert finished.error is not None
        assert "UnsupportedDependencyError" in finished.error
    finally:
        await tasks.shutdown()


async def test_request_annotation_raises_unsupported() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        class Request:
            pass

        @tasks.task
        def handler(request: Request) -> None:
            pass

        job = tasks.enqueue(handler)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.FAILED
        assert finished.error is not None
        assert "UnsupportedDependencyError" in finished.error
    finally:
        await tasks.shutdown()


def test_unsupported_dependency_error_is_public() -> None:
    assert issubclass(UnsupportedDependencyError, Exception)


async def test_depends_dataclass_defaults() -> None:
    def noop() -> None:
        return None

    marker = Depends(noop)
    assert marker.dependency is noop
    assert marker.use_cache is True
