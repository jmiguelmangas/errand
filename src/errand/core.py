"""The Errand engine: task registration, enqueueing, and lifecycle.

stdlib-only. FastAPI integration (:attr:`Errand.router`) is added by
:mod:`errand._fastapi` and imported lazily from there — this module never
imports FastAPI.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, TypeVar, overload

from .errors import UnknownTaskError
from .models import Job, JobStatus
from .retry import BackoffKind, RetryPolicy
from .runner import Runner
from .store import InMemoryJobStore, JobStore

F = TypeVar("F", bound=Callable[..., Any])

_NAME_ATTR = "__errand_name__"
_DEFAULT_SHUTDOWN_TIMEOUT = 5.0


@dataclass
class _Registration:
    """A registered task: the callable plus its resolved retry policy."""

    fn: Callable[..., Any]
    retry: RetryPolicy


class Errand:
    """The background-job engine: register tasks, enqueue work, track state.

    Example::

        tasks = Errand()

        @tasks.task
        async def send_email(user_id: int) -> None: ...

        job = tasks.enqueue(send_email, user_id=1)
        job.status
        # JobStatus.PENDING
    """

    def __init__(
        self,
        *,
        store: JobStore | None = None,
        max_workers: int = 4,
        result_repr_max: int = 500,
        shutdown_timeout: float = _DEFAULT_SHUTDOWN_TIMEOUT,
        default_retry: RetryPolicy | None = None,
    ) -> None:
        self._store = store if store is not None else InMemoryJobStore()
        self._registry: dict[str, _Registration] = {}
        self._default_retry = (
            default_retry if default_retry is not None else RetryPolicy()
        )
        self._runner = Runner(
            self._store, max_workers=max_workers, result_repr_max=result_repr_max
        )
        self._shutdown_timeout = shutdown_timeout
        self._background: set[asyncio.Task[None]] = set()
        self._router: Any = None

    @overload
    def task(self, fn: F) -> F: ...

    @overload
    def task(
        self,
        fn: None = None,
        *,
        name: str | None = None,
        max_retries: int | None = None,
        retry_backoff: BackoffKind | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
    ) -> Callable[[F], F]: ...

    def task(
        self,
        fn: F | None = None,
        *,
        name: str | None = None,
        max_retries: int | None = None,
        retry_backoff: BackoffKind | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
    ) -> F | Callable[[F], F]:
        """Register a callable as a background task.

        Usable bare (``@tasks.task``) or parameterised
        (``@tasks.task(max_retries=3, retry_backoff="exponential")``). If
        none of the retry kwargs are given, the task uses the engine's
        ``default_retry`` policy; if any are given, they define this
        task's policy in full (unset fields fall back to
        :class:`~errand.retry.RetryPolicy`'s own defaults, not the
        engine's).
        """

        def decorator(func: F) -> F:
            task_name = name or func.__name__
            retry = self._resolve_retry_policy(
                max_retries, retry_backoff, base_delay, max_delay
            )
            self._registry[task_name] = _Registration(fn=func, retry=retry)
            setattr(func, _NAME_ATTR, task_name)
            return func

        if fn is not None:
            return decorator(fn)
        return decorator

    def _resolve_retry_policy(
        self,
        max_retries: int | None,
        retry_backoff: BackoffKind | None,
        base_delay: float | None,
        max_delay: float | None,
    ) -> RetryPolicy:
        if (
            max_retries is None
            and retry_backoff is None
            and base_delay is None
            and max_delay is None
        ):
            return self._default_retry

        policy = RetryPolicy()
        if max_retries is not None:
            policy = replace(policy, max_retries=max_retries)
        if retry_backoff is not None:
            policy = replace(policy, backoff=retry_backoff)
        if base_delay is not None:
            policy = replace(policy, base_delay=base_delay)
        if max_delay is not None:
            policy = replace(policy, max_delay=max_delay)
        return policy

    def enqueue(self, fn: Callable[..., Any] | str, *args: Any, **kwargs: Any) -> Job:
        """Enqueue a registered task for background execution.

        Accepts either the decorated function or its registered name.
        Returns immediately with a ``PENDING`` :class:`~errand.models.Job`;
        persistence and execution are scheduled on the running event loop,
        so this must be called from within one (e.g. an async request
        handler). The job becomes visible via :meth:`get_job` shortly
        after this call returns.
        """
        name = fn if isinstance(fn, str) else getattr(fn, _NAME_ATTR, None)
        if name is None or name not in self._registry:
            raise UnknownTaskError(f"Unknown task: {fn!r}")

        registration = self._registry[name]
        job = Job(name=name)
        submission = asyncio.create_task(
            self._runner.submit(job, registration.fn, args, kwargs, registration.retry)
        )
        self._background.add(submission)
        submission.add_done_callback(self._background.discard)
        return job

    async def get_job(self, job_id: str) -> Job | None:
        """Return the job with ``job_id``, or ``None`` if unknown."""
        return await self._store.get(job_id)

    async def list_jobs(
        self, *, status: JobStatus | None = None, limit: int = 50, offset: int = 0
    ) -> list[Job]:
        """List jobs newest-first, optionally filtered by ``status``."""
        return await self._store.list(status=status, limit=limit, offset=offset)

    @property
    def router(self) -> Any:
        """A read-only FastAPI status router: ``GET /`` and ``GET /{job_id}``.

        Built lazily on first access via :mod:`errand._fastapi`, the only
        module that imports FastAPI. Raises ``ImportError`` with an
        actionable message if FastAPI isn't installed.

        Example::

            app.include_router(tasks.router, prefix="/jobs")
        """
        if self._router is None:
            from ._fastapi import build_router

            self._router = build_router(self)
        return self._router

    async def startup(self) -> None:
        """Start the worker pool. Call from your app's startup/lifespan."""
        await self._runner.start()

    async def shutdown(self) -> None:
        """Drain in-flight jobs (up to the configured timeout) and stop."""
        await self._runner.stop(drain=True, timeout=self._shutdown_timeout)

    @asynccontextmanager
    async def lifespan(self, app: Any) -> AsyncIterator[None]:
        """Plain async context manager: start on enter, drain on exit.

        Pass directly to ``FastAPI(lifespan=tasks.lifespan)`` — this
        module never imports FastAPI, so it works with any ASGI framework
        that follows the same lifespan contract.
        """
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()
