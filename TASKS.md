# TASKS.md — implementation plan

Ordered vertical slices. Do them top to bottom. Each milestone must satisfy the
Definition of Done in `CLAUDE.md` (ruff + mypy strict + pytest + coverage ≥ 90%)
before the next one starts. One branch and one PR per milestone.

---

## M0 — Scaffolding
**Goal:** an empty-but-green repo Claude Code can build on.

Scope:
- `pyproject.toml` as in `DESIGN.md` (hatchling, `dependencies = []`, `fastapi`
  as an optional extra, dev extras for tooling).
- `src/errand_jobs/__init__.py` with a `__version__`.
- ruff, mypy (strict), pytest, coverage config.
- `.github/workflows/ci.yml`: matrix Python 3.10–3.13, run ruff, mypy, pytest,
  coverage gate. Include a **second job that installs the base package without
  the `fastapi` extra** and runs the engine tests, proving the engine has no
  FastAPI dependency.
- One smoke test: `import errand_jobs` works (and does **not** import fastapi).

Acceptance:
- `uv run ruff check .`, `uv run mypy src`, `uv run pytest` all pass locally and
  in CI, including the no-FastAPI job.
- `import errand_jobs` does not pull in `fastapi` (assert `"fastapi" not in
  sys.modules` after import).

Out of scope: any real functionality.

---

## M1 — Job model + store
**Goal:** create, read, update, list, prune job records.

Scope:
- `models.py`: `JobStatus`, `Job` (per `DESIGN.md`).
- `store.py`: `JobStore` ABC + `InMemoryJobStore` (dict + `asyncio.Lock`,
  newest-first `list`, `prune`).
- Export all four from `__init__.py`.

Acceptance:
- Tests cover create/get/update/list (with `status` filter + pagination)/prune,
  including get-missing → `None`.

---

## M2 — Runner: fire-and-forget with tracked state
**Goal:** the MVP that already beats `BackgroundTasks`.

Scope:
- `runner.py`: `asyncio.Queue`, `max_workers` worker coroutines, state
  transitions `PENDING → RUNNING → SUCCEEDED/FAILED`, capture `result_repr` /
  `error` / `traceback`, timestamps, `attempts`.
- Sync tasks run via `asyncio.to_thread`; async tasks awaited.
- `core.py` (minimal): `Errand.__init__`, `@task` (registration only, no retry
  yet), `enqueue`, `get_job`, `lifespan` / `startup` / `shutdown` that
  start/drain the runner.
- Graceful drain with timeout on shutdown.

Acceptance:
- Integration test: FastAPI app with `lifespan=tasks.lifespan`, an endpoint
  enqueues a task, test asserts the job reaches `SUCCEEDED` and a failing task
  reaches `FAILED` with error/traceback populated.
- Drain test: in-flight job completes during shutdown within timeout.

---

## M3 — Status API
**Goal:** query job state over HTTP out of the box.

Scope:
- `_fastapi.py`: the only module importing FastAPI, lazily (inside the builder).
  `GET /` (paginated, `?status=` filter) and `GET /{job_id}` (404 on unknown).
  Read-only.
- `Errand.router` property that builds it on first access; a clear error
  (pointing to `pip install errand-jobs[fastapi]`) if FastAPI is absent.

Acceptance:
- Tests via `TestClient`: list returns enqueued jobs, filter works, unknown id
  → 404, shape matches the `Job` record.
- Accessing `.router` without FastAPI installed raises the actionable error;
  the engine tests still pass in that no-FastAPI environment.

---

## M4 — Retries + backoff
**Goal:** failing tasks retry per policy.

Scope:
- `retry.py`: `RetryPolicy` + pure `delay_for(attempt)` (none/fixed/exponential,
  `max_delay` cap).
- Runner: on failure, if `attempts <= max_retries`, re-enqueue after
  `delay_for` (non-blocking, e.g. a delayed re-enqueue helper); else `FAILED`.
- `@task(max_retries=, retry_backoff=, base_delay=, max_delay=)` and
  `Errand(default_retry=...)`.

Acceptance:
- Unit tests for `delay_for` across policies.
- Integration: a task that fails N times then succeeds ends `SUCCEEDED` with
  `attempts == N+1`; one that always fails ends `FAILED` at `max_retries+1`
  attempts. Tests use tiny delays and remain deterministic.

---

## M5 — Dependency injection in tasks
**Goal:** the differentiator — `Depends(...)` inside tasks, with teardown.

Scope:
- `di.py`: define `errand_jobs.Depends` (a `.dependency`-bearing marker) and a
  resolver that recognises any `.dependency` marker by duck typing — **no import
  of fastapi**. Support sync/async/generator/async-generator deps, nested
  `Depends`, per-job caching, teardown via one `AsyncExitStack` per job (runs on
  success and failure). Raise `UnsupportedDependencyError` for
  `Request`/`Response`/`WebSocket`/security scopes.
- Export `Depends` from `__init__.py`.
- Runner integrates the resolver: resolve → merge with enqueued kwargs
  (enqueued wins) → run → close stack.

Acceptance:
- Tests: plain dep injected; `yield` dep torn down after success; `yield` dep
  torn down after failure; nested deps resolved; enqueued kwargs override a dep;
  unsupported dep raises a clear error.
- Interop test: a `fastapi.Depends(...)` resolves identically to
  `errand_jobs.Depends(...)`. The core DI tests pass with FastAPI **not**
  installed (using `errand_jobs.Depends`).

---

## M6 — Scheduling
**Goal:** recurring / timed jobs, tracked like any other job.

Scope:
- `cron.py`: 5-field parser (`*`, values, lists, ranges, steps) +
  `next_after(now)`. `CronParseError` on bad input. Injectable `now()`.
- `scheduler.py`: scheduler coroutine holding `interval` / `at` / `cron`
  schedules; on tick, enqueue due schedules through the runner. Started/stopped
  by the lifespan.
- `@schedule(cron=, interval_seconds=, at=, ...)`.

Acceptance:
- Cron parser unit tests (each syntax + several `next_after` cases + invalid
  input).
- Integration with an injected clock: an interval schedule fires the expected
  number of times over a simulated window; each run appears as a tracked job.

---

## M7 — Polish + release
**Goal:** publishable 0.1.0.

Scope:
- Docstrings on all public API; `README.md` examples runnable.
- `CHANGELOG.md`.
- Coverage gate confirmed ≥ 90%; consider raising for `retry.py` / `cron.py`
  (pure logic → aim 100%).
- Release workflow: build with hatchling, publish to PyPI via **trusted
  publishing (OIDC)** on tag `v*`. No API tokens committed.
- Verify a clean `pip install errand-jobs` in a fresh venv runs the quickstart.

Acceptance:
- Tag build produces a wheel + sdist; dry-run publish succeeds; fresh-install
  smoke test passes.

---

## Sequencing note

M2 alone is already useful (tracked fire-and-forget) — a natural first release
candidate if you want to publish early and iterate. M5 (DI in tasks) is the real
differentiator versus everything else in the ecosystem; don't cut it. M6 can
ship in a follow-up minor if you want 0.1.0 out sooner.
