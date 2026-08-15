# errand

> Stateful background jobs — the missing middle ground between FastAPI's
> `BackgroundTasks` and Celery. **A zero-dependency engine with an optional,
> first-class FastAPI adapter.**

`BackgroundTasks` is fire-and-forget: you can't tell whether a task started,
is running, finished, or failed. The next step up is Celery/ARQ + Redis — a
broker, a worker runtime, and a pile of ops. `errand` fills the valley in
between: in-process jobs with **tracked state**, **retries**, **scheduling**,
and **dependency injection inside tasks** — using nothing but the Python
standard library.

The engine imports **zero third-party packages**. FastAPI is an optional extra:
install it and you get a drop-in lifespan and a status router; skip it and the
engine still runs anywhere. Because FastAPI is user-supplied and only its most
stable public surface is touched (`APIRouter`, the `.dependency` attribute on
`Depends`), a FastAPI release won't leave `errand` stranded.

> **Before first publish:** confirm the `errand` name is still free on PyPI and
> register it. Only `errand-boy` (an abandoned 2014 project) exists today.

## Why

- **State you can query.** Every job has an id and a status
  (`PENDING → RUNNING → SUCCEEDED / FAILED`), with timestamps, result, and
  error captured. An optional router exposes it over HTTP out of the box.
- **No new infrastructure.** Pure `asyncio`. Runs inside your app process (your
  FastAPI app, or any async program). Start with the in-memory store; swap in a
  durable store later without touching your task code.
- **Retries with backoff.** Fixed or exponential, configured per task.
- **Scheduling built in.** `interval`, `at`, and a cron subset — no separate
  beat process.
- **Real dependency injection.** Use the same `Depends(...)` callables you use
  in routes, including `yield`-based resources with proper teardown.
- **Sync tasks don't block the loop.** Plain `def` tasks run in a thread.

## Non-goals

Not a distributed task queue. If you need multi-machine workers, guaranteed
delivery across a broker, or millions of jobs, use Celery/ARQ. `errand`
targets the single-process, "I just need to know if it worked" case that most
apps actually have.

## Install

```bash
pip install errand            # engine only, zero dependencies
pip install "errand[fastapi]" # + the lifespan and status router
# or
uv add "errand[fastapi]"
```

Requires Python 3.10+. FastAPI is only needed for the adapter (the `.router`);
the engine runs standalone.

## Quickstart

```python
from fastapi import FastAPI
from errand import Errand

tasks = Errand()                       # in-memory store, 4 workers
app = FastAPI(lifespan=tasks.lifespan)   # starts/drains workers + scheduler
app.include_router(tasks.router, prefix="/jobs")  # optional status API


@tasks.task(max_retries=3, retry_backoff="exponential")
async def send_welcome_email(user_id: int) -> None:
    ...  # slow work


@app.post("/signup")
async def signup() -> dict:
    job = tasks.enqueue(send_welcome_email, user_id=42)
    return {"job_id": job.id}
```

Check on it:

```
GET /jobs/{job_id}
→ {"id": "...", "name": "send_welcome_email", "status": "RUNNING",
   "attempts": 1, "created_at": "...", "started_at": "...", ...}
```

## Dependency injection in tasks

The same pattern you use in routes, teardown included:

```python
from fastapi import Depends
from errand import Errand

tasks = Errand()


async def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


@tasks.task
async def reindex(db=Depends(get_db)) -> None:
    ...  # db is torn down after the task, success or failure
```

## Scheduling

```python
@tasks.schedule(cron="0 * * * *")     # hourly
async def hourly_cleanup() -> None:
    ...


@tasks.schedule(interval_seconds=30)  # every 30s
async def heartbeat() -> None:
    ...
```

Scheduled runs are tracked exactly like enqueued jobs.

## Backends

The core ships with `InMemoryJobStore`. The `JobStore` interface is the single
seam for durability — a Redis or Postgres store can be added later as an
optional extra without changing any task code.

## Roadmap

See [`TASKS.md`](./TASKS.md). Contributor and architecture notes live in
[`DESIGN.md`](./DESIGN.md) and [`CLAUDE.md`](./CLAUDE.md). Release notes are
in [`CHANGELOG.md`](./CHANGELOG.md).

## License

MIT
