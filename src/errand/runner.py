"""Async worker pool: runs enqueued jobs and tracks their state.

Everything here is stdlib-only. A :class:`Runner` owns an
:class:`asyncio.Queue` of pending work and a fixed pool of worker
coroutines; each worker pulls one envelope at a time, transitions the
job through ``RUNNING`` to a terminal status, and persists every
transition to the :class:`~errand.store.JobStore`.
"""

from __future__ import annotations

import asyncio
import traceback as traceback_module
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .models import Job, JobStatus
from .store import JobStore

TaskFunc = Callable[..., Any]

_TRACEBACK_MAX_CHARS = 4000
_SHUTDOWN_ERROR = "Cancelled during shutdown"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _Envelope:
    """A job paired with the callable and arguments needed to run it.

    Kept in memory only — never persisted, per the store's contract.
    """

    job: Job
    fn: TaskFunc
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class Runner:
    """In-process worker pool that executes jobs on the event loop.

    Example::

        runner = Runner(InMemoryJobStore(), max_workers=4)
        await runner.start()
        await runner.submit(Job(name="ping"), ping, (), {})
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

    async def start(self) -> None:
        """Spawn the worker coroutines. Safe to call at most once."""
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker_loop()) for _ in range(self._max_workers)
        ]

    async def stop(self, *, drain: bool = True, timeout: float | None = None) -> None:
        """Stop the pool.

        If ``drain`` is true, wait up to ``timeout`` seconds for pending and
        in-flight jobs to finish. Anything still unfinished after that is
        cancelled and marked ``FAILED`` with a shutdown note.
        """
        if not self._workers:
            return

        if drain:
            await self._drain(timeout)

        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

        await self._fail_remaining_queue_items()

    async def submit(
        self,
        job: Job,
        fn: TaskFunc,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """Persist ``job`` as ``PENDING`` and queue it for a worker."""
        await self._store.create(job)
        await self._queue.put(_Envelope(job=job, fn=fn, args=args, kwargs=kwargs))

    async def _drain(self, timeout: float | None) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._queue.join(), timeout=timeout)

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
            result = await self._call(envelope.fn, envelope.args, envelope.kwargs)
        except Exception as exc:
            await self._mark_failed(job, exc)
            return

        job.status = JobStatus.SUCCEEDED
        job.result_repr = str(result)[: self._result_repr_max]
        job.finished_at = _utcnow()
        await self._store.update(job)

    @staticmethod
    async def _call(fn: TaskFunc, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if asyncio.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _mark_failed(self, job: Job, exc: Exception) -> None:
        job.status = JobStatus.FAILED
        job.error = f"{type(exc).__name__}: {exc}"
        formatted = "".join(traceback_module.format_exception(exc))
        job.traceback = formatted[:_TRACEBACK_MAX_CHARS]
        job.finished_at = _utcnow()
        await self._store.update(job)
