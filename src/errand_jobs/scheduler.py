"""Scheduler: interval / at / cron schedules, enqueued as tracked jobs.

stdlib-only. A single coroutine loop, started with the app, holds
registered schedules and enqueues whichever are due on each tick;
enqueued work flows through the normal runner and is tracked exactly
like any other job -- there's no separate "scheduled job" concept.

:meth:`Scheduler.tick` is the deterministic core: given an explicit
``now``, it enqueues due schedules and advances their next-fire time,
with no wall-clock sleeping involved -- the seam that makes scheduling
logic testable under an injected clock. :meth:`Scheduler.start` wraps
it in the real, sleeping production loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .cron import CronSchedule

_MAX_TICK_SECONDS = 60.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _Schedule:
    name: str
    enqueue: Callable[[], None]
    cron: CronSchedule | None
    interval_seconds: float | None
    at: datetime | None
    next_fire: datetime | None = None


class Scheduler:
    """Holds registered schedules and enqueues them when due.

    Example::

        scheduler = Scheduler()
        scheduler.add_interval("heartbeat", 30.0, lambda: tasks.enqueue(heartbeat))
        await scheduler.start()
        ...
        await scheduler.stop()
    """

    def __init__(self, *, now: Callable[[], datetime] = _utcnow) -> None:
        self._now = now
        self._schedules: list[_Schedule] = []
        self._task: asyncio.Task[None] | None = None

    def add_cron(self, name: str, expression: str, enqueue: Callable[[], None]) -> None:
        """Register a schedule that fires per the given cron expression."""
        cron = CronSchedule.parse(expression)
        self._add(_Schedule(name, enqueue, cron=cron, interval_seconds=None, at=None))

    def add_interval(
        self, name: str, seconds: float, enqueue: Callable[[], None]
    ) -> None:
        """Register a schedule firing every ``seconds``, starting one interval out."""
        self._add(
            _Schedule(name, enqueue, cron=None, interval_seconds=seconds, at=None)
        )

    def add_at(self, name: str, at: datetime, enqueue: Callable[[], None]) -> None:
        """Register a one-shot schedule that fires once at ``at``."""
        self._add(_Schedule(name, enqueue, cron=None, interval_seconds=None, at=at))

    def _add(self, schedule: _Schedule) -> None:
        schedule.next_fire = self._first_fire(schedule)
        self._schedules.append(schedule)

    def _first_fire(self, schedule: _Schedule) -> datetime:
        now = self._now()
        if schedule.cron is not None:
            return schedule.cron.next_after(now)
        if schedule.interval_seconds is not None:
            return now + timedelta(seconds=schedule.interval_seconds)
        assert schedule.at is not None
        return schedule.at

    def tick(self, now: datetime) -> None:
        """Enqueue every schedule due as of ``now`` and advance its next fire.

        Pure with respect to wall-clock time -- the deterministic seam
        for testing under a simulated/injected clock.
        """
        for schedule in self._schedules:
            if schedule.next_fire is not None and schedule.next_fire <= now:
                schedule.enqueue()
                schedule.next_fire = self._next_fire(schedule, now)

    def _next_fire(self, schedule: _Schedule, now: datetime) -> datetime | None:
        if schedule.cron is not None:
            return schedule.cron.next_after(now)
        if schedule.interval_seconds is not None:
            return now + timedelta(seconds=schedule.interval_seconds)
        return None  # one-shot `at` schedule: fired, no next occurrence

    def seconds_until_next_wake(self, now: datetime) -> float:
        """Seconds to sleep before the next tick, capped at a max tick length."""
        upcoming = [s.next_fire for s in self._schedules if s.next_fire is not None]
        if not upcoming:
            return _MAX_TICK_SECONDS
        remaining = (min(upcoming) - now).total_seconds()
        return max(0.0, min(remaining, _MAX_TICK_SECONDS))

    async def start(self) -> None:
        """Start the scheduler loop. Safe to call at most once."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop the scheduler loop."""
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        while True:
            now = self._now()
            self.tick(now)
            await asyncio.sleep(self.seconds_until_next_wake(now))
