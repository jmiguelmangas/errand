"""Job record and status enum.

Everything here is stdlib-only: no framework types leak into the job
record. Callables and raw call arguments are never persisted — the record
is metadata safe to expose over HTTP.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4


class JobStatus(str, Enum):
    """Lifecycle state of a :class:`Job`.

    Terminal states are ``SUCCEEDED``, ``FAILED``, and ``CANCELLED``; a job
    in any other state may still transition.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


def _new_id() -> str:
    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Job:
    """A tracked unit of background work.

    Holds only metadata safe to expose over HTTP — the callable and its
    arguments are kept in memory by the runner and are never persisted here.

    Example::

        job = Job(name="send_welcome_email")
        job.status
        # JobStatus.PENDING
    """

    name: str
    id: str = field(default_factory=_new_id)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_retries: int = 0
    created_at: datetime = field(default_factory=_utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result_repr: str | None = None
    error: str | None = None
    traceback: str | None = None
