import asyncio
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from errand import Errand, JobStatus


def _build_app() -> tuple[FastAPI, Errand]:
    tasks = Errand()
    app = FastAPI(lifespan=tasks.lifespan)

    @tasks.task
    async def send_welcome_email(user_id: int) -> str:
        await asyncio.sleep(0)
        return f"sent to {user_id}"

    @tasks.task
    def explode() -> None:
        raise RuntimeError("nope")

    @app.post("/signup")
    async def signup(user_id: int) -> dict:
        job = tasks.enqueue(send_welcome_email, user_id=user_id)
        return {"job_id": job.id}

    @app.post("/explode")
    async def trigger_explode() -> dict:
        job = tasks.enqueue(explode)
        return {"job_id": job.id}

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict:
        job = await tasks.get_job(job_id)
        assert job is not None
        return {
            "status": job.status.value,
            "result_repr": job.result_repr,
            "error": job.error,
            "traceback": job.traceback,
        }

    return app, tasks


def _poll_until_terminal(client: TestClient, job_id: str, *, attempts: int = 200):
    for _ in range(attempts):
        response = client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] in (JobStatus.SUCCEEDED.value, JobStatus.FAILED.value):
            return body
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


def test_lifespan_starts_and_drains_worker_pool() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        response = client.post("/signup", params={"user_id": 42})
        assert response.status_code == 200
        job_id = response.json()["job_id"]

        job = _poll_until_terminal(client, job_id)
        assert job["status"] == JobStatus.SUCCEEDED.value
        assert job["result_repr"] == "sent to 42"


def test_lifespan_tracks_failing_task() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        response = client.post("/explode")
        job_id = response.json()["job_id"]

        job = _poll_until_terminal(client, job_id)
        assert job["status"] == JobStatus.FAILED.value
        assert job["error"] == "RuntimeError: nope"
        assert job["traceback"] is not None
        assert "RuntimeError: nope" in job["traceback"]
