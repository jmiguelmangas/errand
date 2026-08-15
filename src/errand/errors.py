"""Public exception hierarchy. stdlib-only."""

from __future__ import annotations


class ErrandError(Exception):
    """Base class for all exceptions raised by errand."""


class UnknownTaskError(ErrandError):
    """Raised when enqueuing a callable or name that was never registered.

    Only functions decorated with :meth:`~errand.core.Errand.task` (or their
    registered name) can be passed to :meth:`~errand.core.Errand.enqueue`.
    """
