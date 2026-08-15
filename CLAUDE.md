# CLAUDE.md — working instructions

This file tells you (Claude Code) how to build and maintain `errand`. Read
`DESIGN.md` for the architecture and `TASKS.md` for the ordered plan. Do the
milestones in order; each is a vertical slice that must land green.

## North star

A stateful, in-process background-job library for FastAPI that sits between
`BackgroundTasks` and Celery. Small, readable, boring in the best way.

## Hard constraints — do not violate

1. **Zero runtime dependencies in the engine.** Every module except
   `_fastapi.py` imports *only* the standard library. No `fastapi`, no Redis
   client, no Celery, no APScheduler, no croniter, no pydantic. Importing
   `errand_jobs` must not import FastAPI. If something seems to need a dependency,
   stop and flag it instead of adding it.
2. **FastAPI is an optional, lazily-imported adapter.** `_fastapi.py` is the
   only module allowed to import FastAPI, and it must do so *inside* the function
   that needs it, never at module top level. The engine (jobs, store, runner,
   retry, scheduler, cron, DI) must be fully usable and testable with FastAPI
   **not installed**.
3. **Recognise `Depends` by duck typing, never by importing it.** A dependency
   marker is any object with a `.dependency` attribute — this matches both our
   own `errand_jobs.Depends` and `fastapi.Depends()`. Do not `import fastapi` in
   `di.py`.
4. **Do not import FastAPI internals** (`fastapi.dependencies.utils`, private
   modules). The only public FastAPI symbol we touch is `APIRouter`, in
   `_fastapi.py`. The dependency resolver is our own so we don't break on
   FastAPI version bumps.
5. **Everything is typed.** Full type hints on all public and internal
   functions. `mypy --strict` must pass.
6. **Public API is documented.** Every public class/function gets a docstring
   with a short example where it helps.

Dev/test dependencies (pytest, pytest-asyncio, mypy, ruff, coverage, httpx for
`TestClient`) are fine — they are not runtime dependencies of the package.

## Tooling & commands

Use `uv`. Standard commands:

```bash
uv sync --all-extras            # install
uv run ruff check .             # lint
uv run ruff format --check .    # format check
uv run mypy src                 # types (strict)
uv run pytest                   # tests
uv run pytest --cov=errand_jobs --cov-report=term-missing
```

## Definition of Done (every change)

A change is not done until:

- `ruff check` and `ruff format --check` are clean.
- `mypy src` passes with no errors.
- `pytest` is green and **coverage stays at 100%** (the enforced gate, per
  `[tool.coverage.report] fail_under` in `pyproject.toml` — the source of
  truth if this drifts from that file).
- New public behaviour has a test and a docstring.
- The milestone's acceptance criteria in `TASKS.md` are all met.

Run lint, types, and tests after **every** milestone before moving on. Never
open the next milestone with a red suite.

## Code style

- Small functions, one responsibility each. Keep cyclomatic complexity low —
  if a function grows a third level of nesting or a long branch ladder, extract
  a helper. Favour readability over cleverness.
- Prefer standard-library building blocks: `asyncio.Queue`, `asyncio.Semaphore`,
  `asyncio.to_thread`, `contextlib.AsyncExitStack`, `dataclasses`, `enum`,
  `datetime`, `uuid`.
- No global mutable state. State lives on the `Errand` instance and its store.
- Reuse existing helpers before writing new ones. Grep first.
- Errors are explicit: capture exceptions on the job record, never swallow
  silently.

## Testing approach

- `pytest` + `pytest-asyncio`. Use `httpx.ASGITransport` / FastAPI `TestClient`
  for the router and lifespan integration.
- Test the state machine transitions directly, not just happy paths: enqueue →
  run → succeed; enqueue → fail → retry → fail; cancel; teardown-on-failure for
  `yield` dependencies.
- Time-based tests (scheduler, backoff) must be deterministic — inject a clock
  or use short, tolerant intervals; never rely on wall-clock sleeps longer than
  a few hundred ms.
- No secrets, no `.env`, no real PII in fixtures. Use obviously-fake data.
- **Engine-without-FastAPI check:** the engine test suite (everything except the
  router) must pass in an environment where `fastapi` is not installed. Add a CI
  job that installs only the base package + test tools (no `fastapi` extra) and
  runs those tests, to guarantee the engine stays decoupled.

## Git & PR conventions

- Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`,
  `refactor:`).
- One PR per milestone slice, small and reviewable. Branch names:
  `m1-job-store`, `m2-runner`, etc.
- PR description states which `TASKS.md` acceptance criteria it satisfies.
- CI (ruff + mypy + pytest + coverage gate) must pass before merge. `main` is
  protected.

## What to flag instead of deciding alone

- Any temptation to add a runtime dependency.
- Any need to reach into FastAPI internals.
- Any scope beyond the current milestone.
- Cron-parsing edge cases not covered by the spec.
