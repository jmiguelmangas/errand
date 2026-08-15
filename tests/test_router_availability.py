"""Engine-only: accessing .router without FastAPI must fail clearly.

Deliberately does NOT match tests/test_fastapi*.py, so it also runs in
the CI job that installs errand-jobs without the fastapi extra.
"""

import sys

import pytest

from errand_jobs import Errand


def test_router_without_fastapi_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "fastapi", None)
    tasks = Errand()

    with pytest.raises(ImportError, match=r"pip install errand-jobs\[fastapi\]"):
        _ = tasks.router
