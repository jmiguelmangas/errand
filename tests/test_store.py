from datetime import datetime, timedelta, timezone

import pytest

from errand import InMemoryJobStore, Job, JobStatus


@pytest.fixture
def store() -> InMemoryJobStore:
    return InMemoryJobStore()


async def test_create_and_get(store: InMemoryJobStore) -> None:
    job = Job(name="ping")

    await store.create(job)
    fetched = await store.get(job.id)

    assert fetched is not None
    assert fetched.id == job.id
    assert fetched.name == "ping"
    assert fetched.status == JobStatus.PENDING


async def test_get_missing_returns_none(store: InMemoryJobStore) -> None:
    assert await store.get("does-not-exist") is None


async def test_update_replaces_record(store: InMemoryJobStore) -> None:
    job = Job(name="ping")
    await store.create(job)

    job.status = JobStatus.SUCCEEDED
    job.result_repr = "42"
    await store.update(job)

    fetched = await store.get(job.id)
    assert fetched is not None
    assert fetched.status == JobStatus.SUCCEEDED
    assert fetched.result_repr == "42"


async def test_get_and_list_return_copies_not_aliases(store: InMemoryJobStore) -> None:
    job = Job(name="ping")
    await store.create(job)

    fetched = await store.get(job.id)
    assert fetched is not None
    fetched.status = JobStatus.FAILED

    assert (await store.get(job.id)).status == JobStatus.PENDING  # type: ignore[union-attr]


async def test_list_is_newest_first(store: InMemoryJobStore) -> None:
    base = datetime.now(timezone.utc)
    older = Job(name="older", created_at=base - timedelta(seconds=10))
    newer = Job(name="newer", created_at=base)

    await store.create(older)
    await store.create(newer)

    jobs = await store.list()

    assert [job.name for job in jobs] == ["newer", "older"]


async def test_list_filters_by_status(store: InMemoryJobStore) -> None:
    pending = Job(name="pending", status=JobStatus.PENDING)
    succeeded = Job(name="succeeded", status=JobStatus.SUCCEEDED)
    await store.create(pending)
    await store.create(succeeded)

    jobs = await store.list(status=JobStatus.SUCCEEDED)

    assert [job.name for job in jobs] == ["succeeded"]


async def test_list_paginates(store: InMemoryJobStore) -> None:
    base = datetime.now(timezone.utc)
    for i in range(5):
        await store.create(Job(name=f"job-{i}", created_at=base - timedelta(seconds=i)))

    page = await store.list(limit=2, offset=1)

    assert [job.name for job in page] == ["job-1", "job-2"]


async def test_prune_deletes_old_terminal_jobs(store: InMemoryJobStore) -> None:
    now = datetime.now(timezone.utc)
    old_finished = Job(
        name="old",
        status=JobStatus.SUCCEEDED,
        finished_at=now - timedelta(days=2),
    )
    recent_finished = Job(
        name="recent",
        status=JobStatus.SUCCEEDED,
        finished_at=now - timedelta(seconds=1),
    )
    still_pending = Job(name="pending", status=JobStatus.PENDING)

    await store.create(old_finished)
    await store.create(recent_finished)
    await store.create(still_pending)

    deleted = await store.prune(older_than=now - timedelta(days=1))

    assert deleted == 1
    assert await store.get(old_finished.id) is None
    assert await store.get(recent_finished.id) is not None
    assert await store.get(still_pending.id) is not None
