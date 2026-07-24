"""Bounded submission over a ThreadPoolExecutor.

The coordinator (the thread iterating ``BoundedScheduler.run``) owns the
in-flight map and pulls input lazily, keeping at most ``max_pending`` tasks
alive at once — memory is O(window), never O(input). No helper threads:
progress, retries, and timeouts (later issues) also live in the coordinator.

Ownership of a ``WorkItem`` moves in one direction and never overlaps:
coordinator (before submit) → worker (while executing) → coordinator (after
its future completes). The happens-before edge comes from the future's own
internal lock in CPython's ``concurrent.futures``, which is why this module
requires a real ``ThreadPoolExecutor``: a process pool would execute against
a pickled copy of the item and silently discard results.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Generic, TypeVar

from tanda._states import TERMINAL_STATES, TaskState, check_transition

T = TypeVar("T")
R = TypeVar("R")

_DEFAULT_PENDING_FACTOR = 4


def default_max_pending(workers: int) -> int:
    return workers * _DEFAULT_PENDING_FACTOR


class WorkItem(Generic[T, R]):
    """One unit of work and its lifecycle metadata.

    Results travel out-of-band on ``value``/``exception``, not through the
    future — the future only signals completion.
    """

    __slots__ = ("index", "item", "state", "value", "exception")

    def __init__(self, index: int, item: T) -> None:
        self.index = index
        self.item = item
        self.state = TaskState.PENDING
        self.value: R | None = None
        self.exception: BaseException | None = None

    def transition_to(self, new_state: TaskState) -> None:
        """Advance the lifecycle; raises InvalidStateTransition on a tanda bug."""
        check_transition(self.state, new_state)
        self.state = new_state


class BoundedScheduler:
    """Coordinator-driven scheduler over a borrowed ThreadPoolExecutor.

    The executor is borrowed for *exclusive* use: while ``run()`` is
    iterating, nothing else may shut the executor down or cancel its
    futures. ``shutdown(cancel_futures=True)`` from another thread drains
    the queue and leaves those futures permanently in CANCELLED — never
    CANCELLED_AND_NOTIFIED, which only a worker's
    ``set_running_or_notify_cancel()`` sets — and ``concurrent.futures.wait``
    never reports plain-CANCELLED futures as done, so the coordinator would
    block forever. The Pool layer must therefore own its executor outright
    and drive shutdown from the coordinator thread only.
    """

    def __init__(self, executor: ThreadPoolExecutor, max_pending: int) -> None:
        if not isinstance(executor, ThreadPoolExecutor):
            raise TypeError(
                "BoundedScheduler requires a ThreadPoolExecutor (results are "
                "handed off through shared memory); got "
                f"{type(executor).__name__}"
            )
        if max_pending < 1:
            raise ValueError(f"max_pending must be >= 1, got {max_pending}")
        self._executor = executor
        self._max_pending = max_pending

    def run(
        self, items: Iterable[T], fn: Callable[[T], R]
    ) -> Iterator[WorkItem[T, R]]:
        """Execute ``fn`` over ``items``, yielding WorkItems in completion order.

        Worker exceptions are captured on the WorkItem (state FAILED), never
        raised here. An exception from the input iterable itself does
        propagate. Closing the generator cancels not-yet-started tasks;
        running tasks cannot be stopped and are left to finish. If several
        tasks completed in the same wait() cycle and the generator is closed
        before all of them were yielded, the unyielded ones are dropped —
        never yielded late, never double-counted.
        """
        iterator = iter(items)
        in_flight: dict[Future[None], WorkItem[T, R]] = {}
        next_index = 0
        exhausted = False
        try:
            while True:
                while not exhausted and len(in_flight) < self._max_pending:
                    try:
                        item = next(iterator)
                    except StopIteration:
                        exhausted = True
                    else:
                        work = WorkItem(next_index, item)
                        next_index += 1
                        future = self._executor.submit(_run_one, work, fn)
                        in_flight[future] = work
                if not in_flight:
                    return
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    work = in_flight.pop(future)
                    _record_escaped_exception(work, future)
                    yield work
        finally:
            _cancel_pending(in_flight)


def _run_one(work: WorkItem[T, R], fn: Callable[[T], R]) -> None:
    """Runs on a worker thread; the worker owns ``work`` until it returns."""
    work.transition_to(TaskState.RUNNING)
    try:
        result = fn(work.item)
    except Exception as exc:
        work.exception = exc
        work.transition_to(TaskState.FAILED)
    else:
        work.value = result
        work.transition_to(TaskState.SUCCESS)


def _record_escaped_exception(
    work: WorkItem[Any, Any], future: Future[None]
) -> None:
    # _run_one only catches Exception; a BaseException (e.g. KeyboardInterrupt
    # inside fn) lands on the future with the item short of a terminal state.
    # If the item is in a state with no legal path to FAILED, transition_to
    # raises InvalidStateTransition — an internal invariant broke, and that
    # must surface loudly rather than yield a stale item.
    if work.state not in TERMINAL_STATES:
        work.exception = future.exception()
        work.transition_to(TaskState.FAILED)


def _cancel_pending(in_flight: dict[Future[None], WorkItem[Any, Any]]) -> None:
    for future, work in in_flight.items():
        # cancel() only succeeds for tasks the executor has not started, so a
        # True result guarantees the item is still PENDING.
        if future.cancel():
            work.transition_to(TaskState.CANCELLED)
