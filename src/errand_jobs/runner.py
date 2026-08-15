"""Async worker pool: runs enqueued jobs and tracks their state.

Everything here is stdlib-only. A :class:`Runner` owns an
:class:`asyncio.Queue` of pending work and a fixed pool of worker
coroutines; each worker pulls one envelope at a time, transitions the
job through ``RUNNING`` to a terminal status, and persists every
transition to the :class:`~errand_jobs.store.JobStore`. On failure, the
configured :class:`~errand_jobs.retry.RetryPolicy` decides whether to
re-enqueue after a delay or mark the job ``FAILED``; the delay is a
separate timer task so a retry wait never blocks a worker.
"""

from __future__ import annotations

import asyncio
import traceback as traceback_module
from collections.abc import Callable
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .di import resolve_dependencies
from .models import Job, JobStatus
from .retry import RetryPolicy
from .store import JobStore

TaskFunc = Callable[..., Any]

_TRACEBACK_MAX_CHARS = 4000
_SHUTDOWN_ERROR = "Cancelled during shutdown"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _Envelope:
    """A job paired with everything needed to run and, on failure, retry it.

    Kept in memory only — never persisted, per the store's contract.
    """

    job: Job
    fn: TaskFunc
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    retry: RetryPolicy


class Runner:
    """In-process worker pool that executes jobs on the event loop.

    Example::

        runner = Runner(InMemoryJobStore(), max_workers=4)
        await runner.start()
        await runner.submit(Job(name="ping"), ping, (), {}, RetryPolicy())
        await runner.stop()
    """

    def __init__(
        self,
        store: JobStore,
        *,
        max_workers: int = 4,
        result_repr_max: int = 500,
    ) -> None:
        self._store = store
        self._max_workers = max_workers
        self._result_repr_max = result_repr_max
        self._queue: asyncio.Queue[_Envelope] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._retry_timers: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Spawn the worker coroutines. Safe to call at most once."""
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker_loop()) for _ in range(self._max_workers)
        ]

    async def stop(self, *, drain: bool = True, timeout: float | None = None) -> None:
        """Stop the pool.

        If ``drain`` is true, wait up to ``timeout`` seconds for pending,
        in-flight, and retry-pending jobs to finish. Anything still
        unfinished after that is cancelled and marked ``FAILED`` with a
        shutdown note.
        """
        if not self._workers:
            return

        if drain:
            await self._drain(timeout)

        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

        await self._cancel_retry_timers()
        await self._fail_remaining_queue_items()

    async def submit(
        self,
        job: Job,
        fn: TaskFunc,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        retry: RetryPolicy,
    ) -> None:
        """Persist ``job`` as ``PENDING`` and queue it for a worker."""
        job.max_retries = retry.max_retries
        await self._store.create(job)
        envelope = _Envelope(job=job, fn=fn, args=args, kwargs=kwargs, retry=retry)
        await self._queue.put(envelope)

    async def _drain(self, timeout: float | None) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._drain_fully(), timeout=timeout)

    async def _drain_fully(self) -> None:
        while True:
            await self._queue.join()
            if not self._retry_timers:
                return
            await asyncio.gather(*self._retry_timers)

    async def _cancel_retry_timers(self) -> None:
        timers = list(self._retry_timers)
        for timer in timers:
            timer.cancel()
        if timers:
            await asyncio.gather(*timers, return_exceptions=True)

    async def _fail_remaining_queue_items(self) -> None:
        while not self._queue.empty():
            envelope = self._queue.get_nowait()
            await self._mark_shutdown_failed(envelope.job)
            self._queue.task_done()

    async def _mark_shutdown_failed(self, job: Job) -> None:
        job.status = JobStatus.FAILED
        job.error = _SHUTDOWN_ERROR
        job.finished_at = _utcnow()
        await self._store.update(job)

    async def _worker_loop(self) -> None:
        while True:
            envelope = await self._queue.get()
            try:
                await self._run_one(envelope)
            except asyncio.CancelledError:
                await self._mark_shutdown_failed(envelope.job)
                raise
            finally:
                self._queue.task_done()

    async def _run_one(self, envelope: _Envelope) -> None:
        job = envelope.job
        job.status = JobStatus.RUNNING
        job.started_at = _utcnow()
        job.attempts += 1
        await self._store.update(job)

        try:
            result = await self._execute(envelope)
        except Exception as exc:
            await self._handle_failure(envelope, exc)
            return

        job.status = JobStatus.SUCCEEDED
        job.result_repr = str(result)[: self._result_repr_max]
        job.finished_at = _utcnow()
        await self._store.update(job)

    @staticmethod
    async def _execute(envelope: _Envelope) -> Any:
        async with AsyncExitStack() as stack:
            resolved = await resolve_dependencies(
                envelope.fn, envelope.kwargs, stack, {}
            )
            call_kwargs = {**resolved, **envelope.kwargs}
            return await Runner._call(envelope.fn, envelope.args, call_kwargs)

    @staticmethod
    async def _call(fn: TaskFunc, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if asyncio.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _handle_failure(self, envelope: _Envelope, exc: Exception) -> None:
        job = envelope.job
        job.error = f"{type(exc).__name__}: {exc}"
        formatted = "".join(traceback_module.format_exception(exc))
        job.traceback = formatted[:_TRACEBACK_MAX_CHARS]

        if job.attempts <= envelope.retry.max_retries:
            job.status = JobStatus.PENDING
            await self._store.update(job)
            self._schedule_retry(envelope, envelope.retry.delay_for(job.attempts))
            return

        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        await self._store.update(job)

    def _schedule_retry(self, envelope: _Envelope, delay: float) -> None:
        timer = asyncio.create_task(self._retry_after_delay(envelope, delay))
        self._retry_timers.add(timer)
        timer.add_done_callback(self._retry_timers.discard)

    async def _retry_after_delay(self, envelope: _Envelope, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            await self._mark_shutdown_failed(envelope.job)
            raise
        await self._queue.put(envelope)
