"""fastapi.Depends(...) must resolve identically to errand.Depends(...)."""

import asyncio

from fastapi import Depends as FastAPIDepends

from errand import Depends as ErrandDepends
from errand import Errand, JobStatus


async def _wait_for_terminal(tasks: Errand, job_id: str, *, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await tasks.get_job(job_id)
        if job is not None and job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


async def test_fastapi_depends_resolves_like_errand_depends() -> None:
    tasks = Errand()
    await tasks.startup()
    events: list[str] = []
    try:

        def get_resource():
            events.append("open")
            try:
                yield "resource"
            finally:
                events.append("close")

        @tasks.task
        def use_fastapi_depends(resource: str = FastAPIDepends(get_resource)) -> str:
            events.append("use")
            return resource

        job = tasks.enqueue(use_fastapi_depends)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "resource"
        assert events == ["open", "use", "close"]
    finally:
        await tasks.shutdown()


async def test_fastapi_and_errand_depends_are_interchangeable_side_by_side() -> None:
    tasks = Errand()
    await tasks.startup()
    try:

        def get_a() -> str:
            return "a"

        def get_b() -> str:
            return "b"

        @tasks.task
        def combine(
            a: str = ErrandDepends(get_a), b: str = FastAPIDepends(get_b)
        ) -> str:
            return a + b

        job = tasks.enqueue(combine)
        finished = await _wait_for_terminal(tasks, job.id)

        assert finished.status == JobStatus.SUCCEEDED
        assert finished.result_repr == "ab"
    finally:
        await tasks.shutdown()
