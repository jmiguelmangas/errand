"""errand — stateful, in-process background jobs.

A zero-dependency engine for tracked background jobs with retries,
scheduling, and dependency injection. FastAPI is an optional, lazily
imported adapter (see :mod:`errand._fastapi`); importing :mod:`errand`
never imports FastAPI.
"""

from .models import Job, JobStatus
from .store import InMemoryJobStore, JobStore

__version__ = "0.1.0"

__all__ = [
    "InMemoryJobStore",
    "Job",
    "JobStatus",
    "JobStore",
    "__version__",
]
