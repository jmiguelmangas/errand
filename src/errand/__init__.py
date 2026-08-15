"""errand — stateful, in-process background jobs.

A zero-dependency engine for tracked background jobs with retries,
scheduling, and dependency injection. FastAPI is an optional, lazily
imported adapter (see :mod:`errand._fastapi`); importing :mod:`errand`
never imports FastAPI.
"""

from .core import Errand
from .di import Depends
from .errors import ErrandError, UnknownTaskError, UnsupportedDependencyError
from .models import Job, JobStatus
from .retry import RetryPolicy
from .store import InMemoryJobStore, JobStore

__version__ = "0.1.0"

__all__ = [
    "Depends",
    "Errand",
    "ErrandError",
    "InMemoryJobStore",
    "Job",
    "JobStatus",
    "JobStore",
    "RetryPolicy",
    "UnknownTaskError",
    "UnsupportedDependencyError",
    "__version__",
]
