"""Job storage: the single seam for durability.

:class:`JobStore` is an abstract interface; :class:`InMemoryJobStore` is the
default, process-local implementation. A future durable store (Redis,
SQL, ...) implements the same interface as an optional extra — the core
never imports one.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import datetime

from .models import Job, JobStatus


class JobStore(ABC):
    """Abstract persistence interface for :class:`~errand_jobs.models.Job` records."""

    @abstractmethod
    async def create(self, job: Job) -> None:
        """Persist a new job record."""

    @abstractmethod
    async def get(self, job_id: str) -> Job | None:
        """Return the job with ``job_id``, or ``None`` if it doesn't exist."""

    @abstractmethod
    async def update(self, job: Job) -> None:
        """Replace the stored record for ``job.id`` with ``job``."""

    @abstractmethod
    async def list(
        self,
        *,
        status: JobStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs newest-first, optionally filtered by ``status``."""

    @abstractmethod
    async def prune(self, older_than: datetime) -> int:
        """Delete terminal jobs finished before ``older_than``.

        Returns the number of records deleted.
        """


_TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)


class InMemoryJobStore(JobStore):
    """A process-local :class:`JobStore` backed by a ``dict`` and a lock.

    No threading concerns: all access happens on the event loop, guarded by
    an :class:`asyncio.Lock` for consistency under concurrent tasks.

    Example::

        store = InMemoryJobStore()
        await store.create(Job(name="ping"))
    """

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = asyncio.Lock()

    async def create(self, job: Job) -> None:
        async with self._lock:
            self._jobs[job.id] = replace(job)

    async def get(self, job_id: str) -> Job | None:
        async with self._lock:
            job = self._jobs.get(job_id)
            return replace(job) if job is not None else None

    async def update(self, job: Job) -> None:
        async with self._lock:
            self._jobs[job.id] = replace(job)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        async with self._lock:
            jobs = list(self._jobs.values())

        if status is not None:
            jobs = [job for job in jobs if job.status == status]

        jobs.sort(key=lambda job: job.created_at, reverse=True)
        return [replace(job) for job in jobs[offset : offset + limit]]

    async def prune(self, older_than: datetime) -> int:
        async with self._lock:
            to_delete = [
                job_id
                for job_id, job in self._jobs.items()
                if job.status in _TERMINAL_STATUSES
                and job.finished_at is not None
                and job.finished_at < older_than
            ]
            for job_id in to_delete:
                del self._jobs[job_id]
            return len(to_delete)
