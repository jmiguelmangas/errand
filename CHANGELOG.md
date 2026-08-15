# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

Initial release.

### Added

- **Job model and in-memory store.** `Job`, `JobStatus`, the `JobStore`
  abstract interface, and `InMemoryJobStore` — the single seam for
  durability, so a durable store can be added later without touching task
  code.
- **In-process async worker pool.** `Errand.task`/`Errand.enqueue` track
  every job through `PENDING → RUNNING → SUCCEEDED/FAILED`, capturing
  timestamps, attempts, `result_repr`, and `error`/`traceback`. Async tasks
  are awaited directly; sync tasks run via `asyncio.to_thread` so they never
  block the loop. `Errand.lifespan`/`startup`/`shutdown` start the pool and
  drain in-flight work gracefully on shutdown.
- **Read-only FastAPI status router.** `Errand.router` — `GET /` (paginated,
  optional `?status=` filter) and `GET /{job_id}` (404 on unknown) — built
  lazily so importing `errand` never imports FastAPI.
- **Retries with backoff.** `RetryPolicy` (`none`/`fixed`/`exponential`,
  capped by `max_delay`), configurable per task or via
  `Errand(default_retry=...)`. Backoff waits never block a worker.
- **Dependency injection in tasks.** `errand.Depends`, recognised by duck
  typing (`.dependency` attribute) so `fastapi.Depends(...)` works
  identically with zero FastAPI import. Sync/async/generator/async-generator
  dependencies, nested dependencies, per-job caching, and `yield`-based
  teardown on both success and failure.
- **Scheduling.** `Errand.schedule` with `cron`, `interval_seconds`, or `at`.
  A small self-contained 5-field cron parser (no third-party dependency).
  Scheduled runs are tracked exactly like any other enqueued job.

### Notes

- The engine (`errand`, minus `errand._fastapi`) has zero runtime
  dependencies and works without FastAPI installed. FastAPI is an optional
  extra (`pip install errand[fastapi]`) needed only for `Errand.router`.
- 100% test coverage on every module; the engine test suite is verified to
  pass in an environment where FastAPI is not installed, in CI and locally.
