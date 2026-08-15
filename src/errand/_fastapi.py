"""FastAPI adapter — the ONLY module in errand that imports FastAPI.

The import happens lazily, inside :func:`build_router`, never at module
top level. Nothing else in errand imports this module until
:attr:`~errand.core.Errand.router` is accessed for the first time, so
FastAPI is never required just to use the engine.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from .models import Job, JobStatus

if TYPE_CHECKING:
    from fastapi import APIRouter

    from .core import Errand


def build_router(tasks: Errand) -> APIRouter:
    """Build a read-only status :class:`~fastapi.APIRouter` backed by ``tasks``.

    Two endpoints:

    - ``GET /`` — paginated list of jobs, optional ``?status=`` filter.
    - ``GET /{job_id}`` — a single job record, 404 if unknown.

    Raises ``ImportError`` with an actionable message (``pip install
    errand[fastapi]``) if FastAPI isn't installed.
    """
    try:
        from fastapi import APIRouter, HTTPException
    except ImportError as exc:
        raise ImportError(
            "errand.router requires FastAPI. Install it with "
            "`pip install errand[fastapi]`."
        ) from exc

    router = APIRouter()

    @router.get("/")
    async def list_jobs(
        status: JobStatus | None = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        jobs = await tasks.list_jobs(status=status, limit=limit, offset=offset)
        return [_serialize(job) for job in jobs]

    @router.get("/{job_id}")
    async def get_job(job_id: str) -> dict[str, Any]:
        job = await tasks.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return _serialize(job)

    return router


def _serialize(job: Job) -> dict[str, Any]:
    data = asdict(job)
    data["status"] = job.status.value
    for key in ("created_at", "started_at", "finished_at"):
        value = data[key]
        if value is not None:
            data[key] = value.isoformat()
    return data
