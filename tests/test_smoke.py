"""Smoke test: the engine must not pull in FastAPI on import."""

import sys


def test_import_does_not_pull_in_fastapi() -> None:
    for module in list(sys.modules):
        if module == "fastapi" or module.startswith("fastapi."):
            del sys.modules[module]

    import errand_jobs

    assert errand_jobs.__version__
    assert "fastapi" not in sys.modules
