import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from errand import Errand, JobStatus


def _build_app() -> tuple[FastAPI, Errand]:
    tasks = Errand()
    app = FastAPI(lifespan=tasks.lifespan)
    app.include_router(tasks.router, prefix="/jobs")

    @tasks.task
    def succeed(n: int) -> int:
        return n * 2

    @tasks.task
    def fail() -> None:
        raise RuntimeError("nope")

    @app.post("/run-succeed")
    async def run_succeed(n: int) -> dict:
        job = tasks.enqueue(succeed, n)
        return {"job_id": job.id}

    @app.post("/run-fail")
    async def run_fail() -> dict:
        job = tasks.enqueue(fail)
        return {"job_id": job.id}

    return app, tasks


def _wait_until_terminal(
    client: TestClient, job_id: str, *, attempts: int = 200
) -> dict:
    for _ in range(attempts):
        response = client.get(f"/jobs/{job_id}")
        body = response.json()
        if body["status"] in (JobStatus.SUCCEEDED.value, JobStatus.FAILED.value):
            return body
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


def test_list_empty_initially() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        response = client.get("/jobs/")
        assert response.status_code == 200
        assert response.json() == []


def test_get_unknown_job_returns_404() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        response = client.get("/jobs/does-not-exist")
        assert response.status_code == 404


def test_get_job_shape_matches_record() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        job_id = client.post("/run-succeed", params={"n": 21}).json()["job_id"]
        job = _wait_until_terminal(client, job_id)

        assert job["id"] == job_id
        assert job["name"] == "succeed"
        assert job["status"] == JobStatus.SUCCEEDED.value
        assert job["attempts"] == 1
        assert job["max_retries"] == 0
        assert job["result_repr"] == "42"
        assert job["error"] is None
        assert job["traceback"] is None
        assert job["created_at"] is not None
        assert job["started_at"] is not None
        assert job["finished_at"] is not None


def test_list_returns_enqueued_jobs() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        first_id = client.post("/run-succeed", params={"n": 1}).json()["job_id"]
        second_id = client.post("/run-fail").json()["job_id"]
        _wait_until_terminal(client, first_id)
        _wait_until_terminal(client, second_id)

        response = client.get("/jobs/")
        assert response.status_code == 200
        ids = {job["id"] for job in response.json()}
        assert ids == {first_id, second_id}


def test_list_filters_by_status() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        succeed_id = client.post("/run-succeed", params={"n": 1}).json()["job_id"]
        fail_id = client.post("/run-fail").json()["job_id"]
        _wait_until_terminal(client, succeed_id)
        _wait_until_terminal(client, fail_id)

        response = client.get("/jobs/", params={"status": "FAILED"})
        assert response.status_code == 200
        jobs = response.json()
        assert [job["id"] for job in jobs] == [fail_id]


def test_list_paginates() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        job_ids = []
        for i in range(5):
            job_id = client.post("/run-succeed", params={"n": i}).json()["job_id"]
            job_ids.append(job_id)
            _wait_until_terminal(client, job_id)

        response = client.get("/jobs/", params={"limit": 2, "offset": 0})
        assert response.status_code == 200
        assert len(response.json()) == 2


def test_failed_job_shape_includes_error_and_traceback() -> None:
    app, _tasks = _build_app()
    with TestClient(app) as client:
        job_id = client.post("/run-fail").json()["job_id"]
        job = _wait_until_terminal(client, job_id)

        assert job["status"] == JobStatus.FAILED.value
        assert job["error"] == "RuntimeError: nope"
        assert "RuntimeError: nope" in job["traceback"]
        assert job["result_repr"] is None
