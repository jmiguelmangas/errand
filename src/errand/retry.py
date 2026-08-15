"""Retry policy: pure, deterministic backoff computation. stdlib-only."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

BackoffKind = Literal["none", "fixed", "exponential"]


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to retry a failed task, and how long to wait between.

    ``delay_for(attempt)`` is pure and deterministic — no jitter in v1.
    ``attempt`` is 1-indexed and counts completed (failed) attempts, so
    ``delay_for(1)`` is the wait before the first retry, right after the
    first attempt failed.

    Example::

        policy = RetryPolicy(max_retries=3, backoff="exponential", base_delay=1.0)
        policy.delay_for(1)  # 1.0
        policy.delay_for(2)  # 2.0
        policy.delay_for(3)  # 4.0
    """

    max_retries: int = 0
    backoff: BackoffKind = "none"
    base_delay: float = 1.0
    max_delay: float = 60.0

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait before the given (1-indexed) retry attempt."""
        if self.backoff == "none":
            return 0.0
        if self.backoff == "fixed":
            return min(self.base_delay, self.max_delay)
        exponential_delay = self.base_delay * (2.0 ** (attempt - 1))
        return min(exponential_delay, self.max_delay)
