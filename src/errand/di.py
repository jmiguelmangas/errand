"""Dependency injection: our own ``Depends`` marker + a duck-typed resolver.

stdlib-only — never imports FastAPI. A parameter is recognised as a
dependency by duck typing: any default value with a ``.dependency``
attribute matches, so this resolves both :class:`Depends` and
``fastapi.Depends(...)`` identically, without ever importing FastAPI.

Not supported (raises :class:`~errand.errors.UnsupportedDependencyError`):

- Parameters annotated ``Request``, ``Response``, ``WebSocket``, or
  ``BackgroundTasks`` (matched by annotation name, again without
  importing FastAPI/Starlette) — these have no meaning outside a
  request/response cycle, which tasks don't run inside of.
- ``fastapi.Security(...)`` markers (detected by the presence of a
  ``.scopes`` attribute) — security scopes assume an authenticated
  request, which a background task doesn't have.

This is a heuristic, name-based check, not a type check (we can't
``isinstance`` against classes we don't import) — documented here so
the trade-off is explicit.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any

from .errors import UnsupportedDependencyError

_UNSUPPORTED_ANNOTATION_NAMES = frozenset(
    {"Request", "Response", "WebSocket", "BackgroundTasks"}
)


@dataclass(frozen=True)
class Depends:
    """Marks a task parameter as resolved via a dependency callable.

    Duck-typed: the resolver recognises *any* object with a
    ``.dependency`` attribute, including ``fastapi.Depends()`` — so
    FastAPI users can reuse their usual ``Depends(...)`` dependencies in
    tasks with zero import of FastAPI here.

    Example::

        async def get_db():
            db = Session()
            try:
                yield db
            finally:
                db.close()

        @tasks.task
        async def reindex(db=Depends(get_db)) -> None: ...
    """

    dependency: Callable[..., Any]
    use_cache: bool = True


def _is_dependency_marker(value: Any) -> bool:
    return hasattr(value, "dependency")


async def resolve_dependencies(
    fn: Callable[..., Any],
    enqueued_kwargs: Mapping[str, Any],
    stack: AsyncExitStack,
    cache: dict[Callable[..., Any], Any],
) -> dict[str, Any]:
    """Resolve ``fn``'s ``Depends(...)`` parameters into a kwargs dict.

    Parameters already present in ``enqueued_kwargs`` are left alone (the
    caller wins, and nothing is resolved or torn down for them).
    ``yield``-based dependencies are entered via ``stack``, which the
    caller closes after the task runs — on both success and failure — to
    run their teardown.
    """
    resolved: dict[str, Any] = {}
    for param_name, param in inspect.signature(fn).parameters.items():
        if param_name in enqueued_kwargs:
            continue

        marker = param.default
        if marker is not inspect.Parameter.empty and _is_dependency_marker(marker):
            resolved[param_name] = await _resolve_one(marker, stack, cache)
            continue

        _reject_if_unsupported_annotation(param)

    return resolved


async def _resolve_one(
    marker: Any, stack: AsyncExitStack, cache: dict[Callable[..., Any], Any]
) -> Any:
    if hasattr(marker, "scopes"):
        raise UnsupportedDependencyError(
            "Security scopes are not supported in errand tasks (no "
            "authenticated request to check them against); use a plain "
            "Depends(...) dependency instead."
        )

    dependency = marker.dependency
    use_cache = getattr(marker, "use_cache", True)

    if use_cache and dependency in cache:
        return cache[dependency]

    nested_kwargs = await resolve_dependencies(dependency, {}, stack, cache)
    value = await _call_dependency(dependency, nested_kwargs, stack)

    if use_cache:
        cache[dependency] = value
    return value


async def _call_dependency(
    dependency: Callable[..., Any], kwargs: dict[str, Any], stack: AsyncExitStack
) -> Any:
    if inspect.isasyncgenfunction(dependency):
        cm = asynccontextmanager(dependency)(**kwargs)
        return await stack.enter_async_context(cm)
    if inspect.isgeneratorfunction(dependency):
        sync_cm = contextmanager(dependency)(**kwargs)
        return stack.enter_context(sync_cm)
    if inspect.iscoroutinefunction(dependency):
        return await dependency(**kwargs)
    return dependency(**kwargs)


def _reject_if_unsupported_annotation(param: inspect.Parameter) -> None:
    annotation = param.annotation
    if annotation is inspect.Parameter.empty:
        return
    name = (
        annotation
        if isinstance(annotation, str)
        else getattr(annotation, "__name__", None)
    )
    if name in _UNSUPPORTED_ANNOTATION_NAMES:
        raise UnsupportedDependencyError(
            f"Parameter {param.name!r} is annotated as {name}, which has no "
            "meaning outside a request/response cycle. errand tasks run in "
            "the background, not as part of a request; remove this "
            "parameter."
        )
