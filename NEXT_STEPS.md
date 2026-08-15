# NEXT_STEPS.md — post-0.1.1 hardening

> **Status: all of P0–P3 done, shipped in 0.2.0.** Kept below as a record
> of what was done and why; see `CHANGELOG.md`'s `[0.2.0]` entry for the
> shipped summary. Nothing here is still outstanding.

Actionable follow-ups from the post-release audit, ordered by priority. Same
rules as the rest of the repo apply: zero runtime deps in the engine, FastAPI
only in `_fastapi.py`, and every change lands green (ruff + mypy strict +
pytest + coverage gate) per `CLAUDE.md`.

Each item has **Scope** (what to do) and **Done when** (acceptance). Do P0
first; P1 is a single quick PR; P2/P3 can follow.

---

## P0 — correctness & legal (do first)

### 1. Add a `LICENSE` file
The project declares MIT in `pyproject.toml` and the README, but no `LICENSE`
file is committed.

- Scope: add a top-level `LICENSE` with the standard MIT text and the correct
  copyright line/year. Keep `license = "MIT"` (SPDX) in `pyproject.toml` and add
  `license-files = ["LICENSE"]` so the file ships in the sdist/wheel (PEP 639).
- Done when: `LICENSE` exists at repo root; `python -m build` includes it; PyPI
  shows the license file on the next release.

### 2. Remove the `enqueue()` → `get_job()` race
Today `enqueue()` returns a `PENDING` job but the record isn't persisted until
the next event-loop tick, so an immediate `get_job()` / status call can return
`None`/404. That's a design smell the README currently papers over with "poll
tolerantly."

- Scope: make the initial `PENDING` record durable **before `enqueue()`
  returns**, so an immediate read is always consistent. The root cause is that
  `enqueue()` is sync while `JobStore` is async — options: a synchronous
  fast-path write into the in-memory store, or a `create_nowait(job)` on the
  store contract used only for the initial record. Keep the `JobStore` ABC
  intact for durable backends.
- Then delete the "poll tolerantly / may get None for an instant" note from the
  README and the workaround from the quickstart tests.
- Done when: a test enqueues a job and asserts `get_job(id)` returns the record
  **synchronously on the very next line**, with no `await`/sleep in between,
  consistently (run it in a loop to prove there's no flake).

### 3. Ship `py.typed`
A `mypy --strict` library is only useful downstream if the type marker is
packaged.

- Scope: ensure `src/errand_jobs/py.typed` exists (empty file). Confirm it's
  included by the hatchling wheel target.
- Done when: `unzip -l dist/errand_jobs-*.whl | grep py.typed` shows the file,
  and a throwaway downstream project running mypy sees `errand_jobs` types.

---

## P1 — packaging metadata & discoverability (one quick PR)

### 4. Add classifiers, keywords, authors to `pyproject.toml`
Improves PyPI discoverability and advertises supported Python versions,
framework, and typing.

- Scope: add to `[project]`:

  ```toml
  authors = [{ name = "<your name>", email = "<your email>" }]
  keywords = ["fastapi", "asyncio", "background-jobs", "task-queue",
              "scheduler", "background-tasks", "jobs"]
  classifiers = [
    "Development Status :: 4 - Beta",
    "Framework :: AsyncIO",
    "Framework :: FastAPI",
    "Intended Audience :: Developers",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: System :: Distributed Computing",
    "Typing :: Typed",
  ]
  ```

  Note: do **not** add a `License :: OSI Approved :: MIT License` classifier —
  with the SPDX `license = "MIT"` expression, a license classifier is redundant
  and newer packaging tooling rejects the combination.
- Done when: `python -m build` succeeds and the classifiers/keywords render on
  the next PyPI release.

### 5. Trim the sdist
The sdist is ~786 kB vs a 21.6 kB wheel — the `assets/` logo is being bundled
into source releases.

- Scope: add
  ```toml
  [tool.hatch.build.targets.sdist]
  exclude = ["assets/", ".github/"]
  ```
  The README references the logo via a raw GitHub URL, so it never needs to be
  in the distribution.
- Done when: the rebuilt sdist is in the tens-of-kB range and contains no PNG.

### 6. Ratchet the coverage gate
You're at 100%; lock it in.

- Scope: set `fail_under = 100` in `[tool.coverage.report]` (or 95 if you want a
  little headroom). 90 lets coverage silently erode.
- Done when: CI fails if coverage drops below the new threshold.

### 7. Set GitHub repo topics
The repo has 0 topics.

- Scope: add topics `fastapi`, `asyncio`, `background-jobs`, `task-queue`,
  `scheduler`, `python`.
- Done when: topics show on the repo home.

### 8. Add README badges
- Scope: PyPI version, supported Python versions, CI status, and license badges
  at the top of the README.
- Done when: badges render and link correctly.

---

## P2 — tooling & supply chain

### 9. Turn on ruff async/lint rules
High value for an `asyncio` library — catches blocking calls in async code,
your most likely bug class.

- Scope: extend `[tool.ruff.lint] select` with `ASYNC` (flake8-async), plus
  `RUF`, `PT` (pytest style), and `C4` (comprehensions). Fix or `# noqa` with a
  reason anything they surface.
- Done when: `ruff check .` is clean with the expanded rule set.

### 10. Harden CI supply chain
- Scope: pin all GitHub Actions to a commit SHA (not a moving tag), and add a
  `dependabot.yml` for the `github-actions` ecosystem so pins get bumped with
  review. Trusted Publishing is already in place — this closes the loop.
- Done when: workflows reference SHAs and Dependabot opens update PRs.

### 11. Add an OS matrix to CI
- Scope: run the test job on `ubuntu-latest`, `macos-latest`, and
  `windows-latest` across the existing Python matrix. Threading + `asyncio`
  behaviour can diverge on Windows.
- Done when: CI is green on all three OSes (or platform-specific skips are
  documented).

---

## P3 — product / observability (future minor)

### 12. Lifecycle hooks / logging
- Scope: optional `on_success` / `on_failure` / `on_retry` callbacks (or a
  thin logging layer) so production users get visibility without wrapping every
  task. Keep it stdlib-only.
- Done when: a registered hook fires on the matching transition, covered by
  tests.

### 13. Bound in-memory growth
- Scope: either an automatic periodic `prune` of terminal jobs (configurable
  TTL) or a clearly documented note that the in-memory store grows unbounded in
  long-running processes and how to prune. Ties into the deferred durable store.
- Done when: long-running usage has a documented, tested memory-management path.

---

## Release

After P0–P1 land: bump the version (P0 item 2 changes observable behaviour, so
consider `0.2.0`; otherwise `0.1.2`), update `CHANGELOG.md`, tag `vX.Y.Z`, and
let the existing Trusted Publishing workflow ship it. Verify a clean
`pip install "errand-jobs[fastapi]"` in a fresh venv runs the quickstart.
