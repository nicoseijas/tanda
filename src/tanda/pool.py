"""Pool — the public entry point.

A Pool creates and owns its ThreadPoolExecutor outright; per the
BoundedScheduler contract, nothing outside the pool may shut it down or
cancel its futures, which is why no executor parameter is accepted.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, cast

from tanda._scheduler import BoundedScheduler, default_max_pending
from tanda._states import TaskState

T = TypeVar("T")
R = TypeVar("R")


def _default_workers() -> int:
    # Same formula as ThreadPoolExecutor's default: sized for I/O-bound work.
    return min(32, (os.cpu_count() or 1) + 4)


class Pool:
    """A bounded thread pool for I/O-bound batch work.

    The common case needs zero configuration::

        with Pool() as pool:
            results = pool.map(files, process)

    Input is consumed lazily with at most ``max_pending`` tasks in flight
    (default ``workers x 4``), so arbitrarily large or infinite iterables run
    in constant memory.

    A Pool belongs to one coordinator thread. Not reentrant: a task function
    must not call back into the same pool — with all workers busy, the inner
    call would wait for slots that can only be freed by the very tasks doing
    the waiting. Not thread-safe either: calling ``close()`` (or exiting the
    ``with`` block) while another thread is inside ``map()`` can hang that
    thread permanently (see BoundedScheduler), and concurrent ``map()`` calls
    would each get their own ``max_pending`` window, breaking the pool-wide
    bound. Drive everything from the owning thread.

    On KeyboardInterrupt inside ``map()``, pending tasks are cancelled
    immediately; the executor itself is shut down by ``__exit__``/``close()``
    — one more reason the ``with`` block is the primary path.
    """

    def __init__(
        self, workers: int | None = None, max_pending: int | None = None
    ) -> None:
        if workers is not None and workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")
        if max_pending is not None and max_pending < 1:
            raise ValueError(f"max_pending must be >= 1, got {max_pending}")
        self._workers = workers if workers is not None else _default_workers()
        self._max_pending = (
            max_pending
            if max_pending is not None
            else default_max_pending(self._workers)
        )
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="tanda"
        )
        self._scheduler: BoundedScheduler | None = BoundedScheduler(
            self._executor, self._max_pending
        )

    @property
    def workers(self) -> int:
        return self._workers

    @property
    def max_pending(self) -> int:
        return self._max_pending

    def map(self, items: Iterable[T], fn: Callable[[T], R]) -> list[R]:
        """Apply ``fn`` to every item, returning results in input order.

        The first definitive failure cancels not-yet-started tasks and
        re-raises the worker's exception immediately (fail fast); running
        tasks cannot be stopped and finish in the background. The re-raised
        exception keeps its worker-thread traceback, so it pins those frames
        (and their locals) for as long as the caller holds it. Error policies
        and structured errors (TaskError, collect mode) arrive with #7.
        """
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        completed_values: dict[int, R] = {}
        completion_stream = scheduler.run(items, fn)
        try:
            for work in completion_stream:
                if work.state is TaskState.SUCCESS:
                    completed_values[work.index] = cast(R, work.value)
                elif work.state is TaskState.FAILED:
                    assert work.exception is not None
                    raise work.exception
                else:  # no other terminal state is producible yet — fail loudly
                    raise RuntimeError(
                        f"unexpected terminal state {work.state} for item "
                        f"{work.index}"
                    )
        finally:
            # Deterministic cleanup: cancel pending tasks now, not at GC.
            completion_stream.close()
        try:
            return [completed_values[i] for i in range(len(completed_values))]
        except KeyError as exc:
            raise RuntimeError(
                "internal invariant violated: non-contiguous result indices "
                f"(missing {exc.args[0]})"
            ) from exc

    def close(self) -> None:
        """Shut down the pool, waiting for running tasks. Idempotent."""
        if self._executor is None:
            return
        # Safe against the wait()-hang documented on BoundedScheduler ONLY
        # because of the single-coordinator contract in the class docstring:
        # called from the owning thread, no run() loop can be waiting.
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._executor = None
        self._scheduler = None

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
