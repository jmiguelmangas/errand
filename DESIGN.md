# DESIGN.md — architecture

The engine is **pure standard library** and imports nothing else. FastAPI is an
**optional integration**, isolated in a single adapter module and imported
lazily — importing `errand_jobs` never imports FastAPI. This document is the
contract; `TASKS.md` builds it incrementally.

## Coupling & version stability

The concern this design answers: if `errand` hard-depends on FastAPI, a FastAPI
release could break it. So:

- **The engine never imports FastAPI.** It works standalone (scripts, CLIs, any
  framework).
- **Dependency markers are duck-typed.** The resolver treats *any* object with a
  `.dependency` attribute as a `Depends` marker. FastAPI's `Depends()` has that
  attribute, so it works without being imported. We also ship our own
  `errand_jobs.Depends` for standalone use.
- **The lifespan is a plain async context manager.** That's the ASGI/Starlette
  contract, not FastAPI's to change — it plugs into `FastAPI(lifespan=...)` with
  no FastAPI import.
- **Only `_fastapi.py` touches FastAPI**, and only its most stable public
  surface (`APIRouter`), imported lazily. If FastAPI is absent and you reach for
  the router, you get a clear, actionable error.

Net effect: FastAPI is a user-supplied optional extra. We adapt to whatever
FastAPI version the user already has, instead of pinning one. The fragile
surface shrinks to `APIRouter` + the `.dependency` attribute — both rock-stable.
FastAPI **internals are never imported** (enforced in `CLAUDE.md`).

## Module layout

```
src/errand_jobs/
├── __init__.py        # public exports: Errand, Job, JobStatus, JobStore, InMemoryJobStore, Depends
├── models.py          # Job dataclass, JobStatus enum          [stdlib only]
├── store.py           # JobStore ABC + InMemoryJobStore         [stdlib only]
├── runner.py          # asyncio worker pool: queue, workers     [stdlib only]
├── retry.py           # RetryPolicy + backoff computation       [stdlib only]
├── di.py              # our Depends marker + duck-typed resolver [stdlib only]
├── scheduler.py       # interval / at / cron scheduling loop     [stdlib only]
├── cron.py            # minimal 5-field cron parser              [stdlib only]
├── _fastapi.py        # the ONLY module importing FastAPI (lazy): status router
└── core.py            # Errand engine + lifespan; .router calls _fastapi lazily
tests/
pyproject.toml
```

Keep modules single-purpose. `core.py` wires the engine together; `_fastapi.py`
is the sole FastAPI boundary. Nothing under the `[stdlib only]` marker may import
FastAPI.

## Data model (`models.py`)

```python
class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

@dataclass
class Job:
    id: str                      # uuid4 hex
    name: str                    # registered task name
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_retries: int = 0
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result_repr: str | None = None      # str() of return value, capped length
    error: str | None = None            # exception type + message
    traceback: str | None = None        # formatted traceback, capped length
    # args/kwargs are held by the runner in memory, NOT persisted in the record
```

Rationale: the record is metadata you can safely expose over HTTP. Callables
and raw arguments never go into the store (keeps stores simple and avoids
serialisation/PII concerns).

## Job store (`store.py`)

The single seam for durability.

```python
class JobStore(ABC):
    async def create(self, job: Job) -> None: ...
    def create_sync(self, job: Job) -> bool: ...   # default: return False
    async def get(self, job_id: str) -> Job | None: ...
    async def update(self, job: Job) -> None: ...
    async def list(self, *, status: JobStatus | None = None,
                   limit: int = 50, offset: int = 0) -> list[Job]: ...
    async def prune(self, older_than: datetime) -> int: ...
```

`InMemoryJobStore`: a `dict[str, Job]` guarded by an `asyncio.Lock`. `list`
returns newest-first. `prune` drops terminal jobs older than a cutoff. That's
it — no threading concerns because everything runs on the event loop.

`create_sync` exists so `Errand.enqueue()` (a plain, non-async method) can
make the job's `PENDING` record immediately visible via `get`/`list` before
it returns, instead of scheduling `create` as a task and returning before
that task has run — which left a real, if brief, window where an
immediately-following `get_job()` returned `None`. `InMemoryJobStore`
overrides it (plain dict write, no lock needed — it can't suspend
mid-write, so nothing else can interleave). The default implementation
returns `False`; a store that can only persist via real I/O (a future
`RedisJobStore` / `SqlJobStore`) doesn't override it and `enqueue()` falls
back to scheduling `create` as before for that store.

A future `RedisJobStore` / `SqlJobStore` implements the same ABC as an optional
extra. **The core never imports them.**

## Runner (`runner.py`)

In-process worker pool on the event loop.

- An `asyncio.Queue[_Envelope]` of pending work. An `_Envelope` carries the
  `job_id`, the registered task, and the bound args/kwargs (kept in memory).
- `max_workers` worker coroutines (default 4), each: pull envelope → mark
  `RUNNING`, set `started_at`, `attempts += 1` → run → on success capture
  `result_repr`, mark `SUCCEEDED`; on exception capture `error`/`traceback`,
  then apply the retry policy (re-enqueue with delay, or mark `FAILED`).
- **Sync vs async:** if the task is a coroutine function, `await` it; otherwise
  run it via `asyncio.to_thread` so it never blocks the loop.
- **Delayed re-enqueue:** schedule with `loop.call_later` / an `asyncio.sleep`
  helper task; don't block a worker while waiting out a backoff.
- **Lifecycle:** `start()` spawns workers; `stop(drain=True, timeout=...)`
  stops accepting new work, lets in-flight jobs finish up to `timeout`, then
  cancels leftovers and marks them `FAILED` with a shutdown note.

## Retry policy (`retry.py`)

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 0
    backoff: Literal["none", "fixed", "exponential"] = "none"
    base_delay: float = 1.0      # seconds
    max_delay: float = 60.0

    def delay_for(self, attempt: int) -> float: ...   # pure, deterministic
```

`exponential` = `min(base_delay * 2 ** (attempt - 1), max_delay)`. Pure
function, trivially unit-tested. No jitter in v1 (keep it deterministic; add
later if needed).

## Dependency resolver (`di.py`)

Our own resolver — imports no FastAPI. It defines a tiny marker and duck-types
foreign ones:

```python
@dataclass(frozen=True)
class Depends:
    dependency: Callable[..., Any]
    use_cache: bool = True
```

A parameter is a dependency if its default has a `.dependency` attribute. That
matches both `errand_jobs.Depends` and `fastapi.Depends()` (which also exposes
`.dependency`), so FastAPI users pass their usual `Depends(...)` and it just
works — with zero import of FastAPI. Supported:

- Parameters whose default is any `.dependency`-bearing marker.
- Dependency callables that are sync, async, generator (`yield`), or async
  generator — `yield` resources are entered/exited via a single
  `contextlib.AsyncExitStack` per job (teardown runs on success **and**
  failure).
- Nested dependencies: a dependency callable may itself declare `Depends(...)`
  parameters; resolve recursively.
- Per-job caching: the same dependency callable resolves once per job
  (`use_cache=True` semantics), keyed by callable identity.

Explicitly **not** supported in v1 (document in the resolver docstring and raise
a clear `ErrandError` if encountered): `Request`/`Response`/`WebSocket`
parameters, security scopes, and FastAPI's `BackgroundTasks` parameter. Tasks
run outside the request cycle, so these have no meaning here.

Resolution flow per job: build an `AsyncExitStack`, resolve declared
dependencies into a kwargs dict, merge with the enqueued kwargs (enqueued
values win on conflict), run the task, then close the stack.

## Scheduler (`scheduler.py`) + cron (`cron.py`)

A single scheduler coroutine started with the app. It holds registered
schedules and, on each tick, enqueues any that are due (which then flow through
the normal runner and get tracked like any job).

Schedule kinds:

- `interval_seconds: float` — run every N seconds.
- `at: datetime` — run once at a time (one-shot).
- `cron: str` — 5-field cron (`min hour dom month dow`).

`cron.py` is a small, self-contained parser: fields support `*`, single values,
lists (`1,2,3`), ranges (`1-5`), and steps (`*/5`). A `CronSchedule.next_after(
now) -> datetime` computes the next fire time by minute-stepping (bounded,
simple, well-tested). No third-party cron library.

Scheduler loop: sleep until the earliest next-fire across all schedules (or a
max tick, e.g. 60s, whichever is sooner), wake, enqueue due schedules, repeat.
Deterministic under an injectable `now()` for tests.

## Status router (`_fastapi.py`)

The **only** module that imports FastAPI, and it does so lazily (inside the
builder function, not at module top level). `Errand.router` calls it on first
access; if FastAPI isn't installed it raises a clear error pointing the user to
`pip install errand-jobs[fastapi]`.

Optional `APIRouter`, opt-in via `app.include_router(tasks.router)`:

- `GET /` → paginated list, optional `?status=` filter, returns job records.
- `GET /{job_id}` → one job record, 404 if unknown.

Read-only. No mutation endpoints in v1 (no remote enqueue/cancel over HTTP —
avoids an obvious abuse surface).

## Public API (`core.py`)

```python
class Errand:
    def __init__(self, *, store: JobStore | None = None, max_workers: int = 4,
                 default_retry: RetryPolicy | None = None,
                 result_repr_max: int = 500) -> None: ...

    # registration
    def task(self, fn=None, *, name=None, max_retries=0,
             retry_backoff="none", base_delay=1.0, max_delay=60.0): ...
    def schedule(self, fn=None, *, cron=None, interval_seconds=None,
                 at=None, name=None, **retry_kwargs): ...

    # enqueue
    def enqueue(self, fn, *args, **kwargs) -> Job: ...     # returns the created Job

    # inspection
    async def get_job(self, job_id: str) -> Job | None: ...

    # FastAPI integration
    @property
    def router(self): ...                     # lazily builds a fastapi APIRouter
    @asynccontextmanager
    async def lifespan(self, app): ...        # plain async CM; no fastapi import
    async def startup(self) -> None: ...      # for composing with an existing lifespan
    async def shutdown(self) -> None: ...
```

`task` and `schedule` work both bare (`@tasks.task`) and parameterised
(`@tasks.task(max_retries=3)`). `enqueue` accepts either the decorated function
or its registered name.

Composing with an existing lifespan (documented pattern):

```python
@asynccontextmanager
async def lifespan(app):
    await tasks.startup()
    try:
        yield
    finally:
        await tasks.shutdown()
```

## Errors

Single public base exception `ErrandError`. Subtypes: `UnknownTaskError`,
`UnsupportedDependencyError`, `CronParseError`. Raised with actionable
messages.

## Proposed `pyproject.toml`

```toml
[project]
name = "errand-jobs"
version = "0.1.0"
description = "Stateful background jobs — a zero-dependency engine with an optional FastAPI adapter."
readme = "README.md"
requires-python = ">=3.10"
license = "MIT"
dependencies = []                      # engine is pure stdlib

[project.optional-dependencies]
fastapi = ["fastapi>=0.100"]           # only needed for the status router
dev = [
  "fastapi>=0.100",
  "pytest>=8",
  "pytest-asyncio>=0.23",
  "httpx>=0.27",
  "mypy>=1.10",
  "ruff>=0.5",
  "coverage[toml]>=7",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 88
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "C90"]

[tool.mypy]
strict = true
files = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"

[tool.coverage.report]
fail_under = 90
show_missing = true
```
