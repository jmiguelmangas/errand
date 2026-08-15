# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

Post-0.1.1 hardening pass (see `NEXT_STEPS.md` for the full plan this
implements).

### Added

- **Lifecycle hooks.** `Errand.on_success`/`on_failure`/`on_retry` — bare
  decorators, each registrable multiple times; every registered hook fires,
  in registration order, with an immutable snapshot of the job as of that
  exact transition. Sync or async; a hook that raises is logged and doesn't
  affect the job or other hooks.
- **Bounded in-memory growth.** `Errand(prune_after=<seconds>)` — off by
  default; when set, automatically prunes terminal jobs older than that on
  a background interval (capped at 60s), reusing the existing scheduler.
  Never touches `PENDING`/`RUNNING` jobs.
- `LICENSE` file (MIT), plus `license-files` in `pyproject.toml` (PEP 639)
  so it ships in the wheel and sdist.
- PyPI metadata: `authors`, `keywords`, `classifiers`, `project.urls`
  (Homepage/Repository/Issues/Changelog).
- PyPI version, Python-versions, CI status, and license badges in the
  README.

### Fixed

- **`enqueue()`/`get_job()` visibility race.** `enqueue()` used to schedule
  the job's initial persistence as a background task and return before
  that task had run, so an immediately-following `get_job()` (no `await`
  in between) could briefly return `None`. `JobStore` gained
  `create_sync()`, a best-effort synchronous create (`InMemoryJobStore`
  overrides it; durable stores default to `False` and keep the old async
  path); for the default store, the `PENDING` record is now durable
  *before* `enqueue()` returns, no polling needed.

### Changed

- Coverage gate raised from 90% to 100% (the suite has been at 100% since
  M1); one Python-3.11-only coverage.py tracer false negative excluded
  with a documented `pragma: no cover`, not by loosening the gate.
- `ruff` now also runs the `ASYNC`, `RUF`, `PT`, and `C4` rule sets.
- Sdist trimmed (~786 kB → ~39 kB): `assets/`, `.github/`, and `uv.lock`
  don't belong in a source distribution.
- CI: GitHub Actions pinned to commit SHAs (was moving tags/branches),
  with Dependabot now keeping them current via reviewable PRs; test job
  now runs across `ubuntu-latest`/`macos-latest`/`windows-latest`, not
  just Ubuntu.

## [0.1.1]

### Fixed

- README's logo and in-repo doc links (`CHANGELOG.md`, `TASKS.md`,
  `DESIGN.md`, `CLAUDE.md`) used relative paths, which GitHub resolves
  against the repo tree but PyPI does not — they rendered broken on the
  PyPI project page. Switched to absolute `raw.githubusercontent.com` /
  `github.com/.../blob/main` URLs, which work on both.

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
  lazily so importing `errand_jobs` never imports FastAPI.
- **Retries with backoff.** `RetryPolicy` (`none`/`fixed`/`exponential`,
  capped by `max_delay`), configurable per task or via
  `Errand(default_retry=...)`. Backoff waits never block a worker.
- **Dependency injection in tasks.** `errand_jobs.Depends`, recognised by duck
  typing (`.dependency` attribute) so `fastapi.Depends(...)` works
  identically with zero FastAPI import. Sync/async/generator/async-generator
  dependencies, nested dependencies, per-job caching, and `yield`-based
  teardown on both success and failure.
- **Scheduling.** `Errand.schedule` with `cron`, `interval_seconds`, or `at`.
  A small self-contained 5-field cron parser (no third-party dependency).
  Scheduled runs are tracked exactly like any other enqueued job.

### Notes

- Published on PyPI as `errand-jobs` (`pip install errand-jobs`); the
  import name is `errand_jobs`. The PyPI name `errand` was already taken by
  an unrelated package by the time of first publish.
- The engine (`errand_jobs`, minus `errand_jobs._fastapi`) has zero runtime
  dependencies and works without FastAPI installed. FastAPI is an optional
  extra (`pip install errand-jobs[fastapi]`) needed only for `Errand.router`.
- 100% test coverage on every module; the engine test suite is verified to
  pass in an environment where FastAPI is not installed, in CI and locally.
