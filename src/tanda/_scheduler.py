"""Bounded submission over a ThreadPoolExecutor.

The coordinator (the thread iterating ``BoundedScheduler.run``) owns the
in-flight map and pulls input lazily, keeping at most ``max_pending`` tasks
alive at once — memory is O(window), never O(input). No helper threads:
progress, retries, and timeouts all live in the coordinator.

A ``WorkItem`` is normally handed off coordinator → worker → coordinator,
ordered by future completion. ``task_timeout`` breaks that exclusivity: the
coordinator may finalize an item the worker still holds. Every finalizing
transition therefore goes through the item's own lock as a compare-and-set —
whoever finalizes first wins, and the loser's outcome is discarded (a timed
out task's late result goes nowhere). This module requires a real
``ThreadPoolExecutor``: a process pool would execute against a pickled copy
of the item and silently discard results.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Generic, TypeVar

from tanda._states import TERMINAL_STATES, TaskState, check_transition
from tanda.cancellation import Cancellation
from tanda.exceptions import Cancelled, OverallTimeout
from tanda.retry import RetryPolicy

logger = logging.getLogger("tanda")

T = TypeVar("T")
R = TypeVar("R")

_DEFAULT_PENDING_FACTOR = 4

# Upper bound on any single coordinator wait. Bounds the observation latency
# for a Cancellation requested from another thread, and for Ctrl+C on
# Windows, where a lock/event wait cannot be interrupted mid-wait.
_MAX_WAIT_SLICE = 0.5


def default_max_pending(workers: int) -> int:
    return workers * _DEFAULT_PENDING_FACTOR


class WorkItem(Generic[T, R]):
    """One unit of work and its lifecycle metadata.

    Results travel out-of-band on ``value``/``exception``, not through the
    future — the future only signals completion. All finalizing transitions
    are compare-and-set under ``lock`` because the coordinator (timeout,
    cancellation) and the worker (completion) can race to finalize.
    """

    __slots__ = (
        "index",
        "item",
        "state",
        "value",
        "exception",
        "attempts",
        "elapsed",
        "started_at",
        "lock",
    )

    def __init__(self, index: int, item: T) -> None:
        self.index = index
        self.item = item
        self.state = TaskState.PENDING
        self.value: R | None = None
        self.exception: BaseException | None = None
        self.attempts = 1  # executions performed; bumped on each resubmission
        self.elapsed = 0.0  # total seconds inside fn across attempts (no backoff)
        self.started_at: float | None = None  # monotonic start of current run
        self.lock = threading.Lock()

    def transition_to(self, new_state: TaskState) -> None:
        """Raw state advance; callers hold ``lock`` when racing is possible."""
        check_transition(self.state, new_state)
        self.state = new_state

    # --- worker side ---------------------------------------------------------

    def start_execution(self) -> None:
        with self.lock:
            self.transition_to(TaskState.RUNNING)
            self.started_at = time.monotonic()

    def finish_success(self, value: R, elapsed: float) -> None:
        with self.lock:
            if self.state is TaskState.TIMED_OUT:
                return  # coordinator finalized first; drop the late result
            self.elapsed += elapsed
            self.value = value
            self.exception = None  # clear a previous attempt's failure
            self.transition_to(TaskState.SUCCESS)

    def finish_failure(
        self, exc: Exception, elapsed: float, retry: RetryPolicy | None
    ) -> None:
        with self.lock:
            if self.state is TaskState.TIMED_OUT:
                return  # coordinator finalized first; drop the late failure
            self.elapsed += elapsed
            self.exception = exc
            if (
                self.state is TaskState.RUNNING  # never after a cancel request
                and not isinstance(exc, Cancelled)  # cancellation never retries
                and retry is not None
                and retry.should_retry(exc, self.attempts)
            ):
                self.transition_to(TaskState.RETRY_WAIT)
            else:
                self.transition_to(TaskState.FAILED)

    def finish_escape(self, exc: BaseException, elapsed: float) -> None:
        """CAS finalization for a BaseException escaping ``fn``.

        Recording it under the lock *before* it propagates to the future
        closes the window where the item would sit RUNNING-but-actually-done
        and a concurrent timeout scan could win the CAS and silently replace
        a real KeyboardInterrupt/SystemExit with a TaskTimeout. If the
        deadline genuinely expired first, the timeout outcome stands — the
        race is decided deterministically under the lock, never by scan
        latency. Never retried.
        """
        with self.lock:
            if self.state is TaskState.TIMED_OUT:
                return
            self.elapsed += elapsed
            self.exception = exc
            self.transition_to(TaskState.FAILED)

    # --- coordinator side ----------------------------------------------------

    def running_deadline(self, task_timeout: float) -> float | None:
        with self.lock:
            if self.state is TaskState.RUNNING and self.started_at is not None:
                return self.started_at + task_timeout
            return None

    def time_out_if_expired(self, now: float, task_timeout: float) -> bool:
        """CAS: finalize as TIMED_OUT iff still running past its deadline."""
        with self.lock:
            if self.state is not TaskState.RUNNING or self.started_at is None:
                return False
            if now - self.started_at < task_timeout:
                return False
            self.elapsed += now - self.started_at  # observed execution time
            self.exception = TimeoutError(
                f"task_timeout of {task_timeout}s exceeded"
            )
            self.transition_to(TaskState.TIMED_OUT)
            return True

    def cancel(self) -> None:
        """CAS + idempotent: only not-yet-running states become CANCELLED."""
        with self.lock:
            if self.state in (TaskState.PENDING, TaskState.RETRY_WAIT):
                self.transition_to(TaskState.CANCELLED)

    def request_cancellation(self) -> None:
        """Cooperative level: a running task is asked, never killed."""
        with self.lock:
            if self.state is TaskState.RUNNING:
                self.transition_to(TaskState.CANCELLATION_REQUESTED)

    def retry_clone(self) -> WorkItem[T, R]:
        """A fresh item for re-running after a timeout: the original stays
        with the abandoned execution, which can no longer touch this one."""
        # Lock-free reads are safe here: only the coordinator calls this,
        # synchronously after its own time_out_if_expired() won the CAS, so
        # same-thread program order guarantees these values are final.
        clone: WorkItem[T, R] = WorkItem(self.index, self.item)
        clone.attempts = self.attempts
        clone.elapsed = self.elapsed
        # The clone continues the logical task where the original left off:
        # it is *constructed* parked in RETRY_WAIT (PENDING -> RETRY_WAIT is
        # deliberately not a legal runtime transition).
        clone.state = TaskState.RETRY_WAIT
        return clone


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

    def __init__(
        self,
        executor: ThreadPoolExecutor,
        max_pending: int,
        on_leak: Callable[[set[Future[None]]], None] | None = None,
    ) -> None:
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
        # Called once per run() teardown with the futures whose workers are
        # still inside fn — the only executions a bounded shutdown can wait
        # on. Cancelled futures are excluded on purpose: they are drained
        # from the queue and never reported done by wait().
        self._on_leak = on_leak

    def run(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
    ) -> Iterator[WorkItem[T, R]]:
        """Execute ``fn`` over ``items``, yielding WorkItems in completion order.

        Worker exceptions are captured on the WorkItem (state FAILED), never
        raised here; an exception from the input iterable itself does
        propagate, and ``overall_timeout`` expiry raises OverallTimeout. With
        a ``retry`` policy, retryable failures move to RETRY_WAIT and are
        resubmitted through this same bounded window once their backoff
        elapses. A ``task_timeout`` is measured per execution, from the
        moment the worker starts the item — queue time never counts. A timed
        out execution cannot be killed: its future moves to an "abandoned"
        set that keeps occupying a window slot until the worker returns, and
        the item is yielded as TIMED_OUT (or cloned into RETRY_WAIT when the
        policy retries TimeoutError). Only terminal items are yielded.

        Closing the generator cancels not-yet-started tasks and any items in
        retry backoff; running tasks cannot be stopped and are left to
        finish. If several tasks completed in the same wait() cycle and the
        generator is closed before all of them were yielded, the unyielded
        ones are dropped — never yielded late, never double-counted. A
        generator abandoned without close() only runs this cleanup when it is
        finalized — immediate under CPython refcounting, possibly delayed on
        other implementations — so prefer exhausting it or closing it
        explicitly.
        """
        iterator = iter(items)
        in_flight: dict[Future[None], WorkItem[T, R]] = {}
        retry_wait: dict[WorkItem[T, R], float] = {}  # item -> monotonic due
        abandoned: set[Future[None]] = set()  # timed-out, still-running
        overall_deadline = (
            time.monotonic() + overall_timeout if overall_timeout else None
        )
        next_index = 0
        exhausted = False

        def submit(work: WorkItem[T, R]) -> None:
            in_flight[self._executor.submit(_run_one, work, fn, retry)] = work

        try:
            while True:
                now = time.monotonic()
                if cancel is not None and cancel.requested:
                    # Checked before the overall deadline: an explicit stop
                    # request wins over an automatic timeout when both expire
                    # in the same iteration.
                    # Two honest levels: running tasks are asked (their late
                    # outcomes are dropped with the abandoned executions);
                    # everything not yet started is cancelled by the finally.
                    for work in in_flight.values():
                        work.request_cancellation()
                    raise Cancelled("cancellation requested")
                if overall_deadline is not None and now >= overall_deadline:
                    raise OverallTimeout(
                        f"overall_timeout of {overall_timeout}s exceeded"
                    )
                for work in [w for w, due in retry_wait.items() if due <= now]:
                    del retry_wait[work]
                    work.attempts += 1
                    with work.lock:
                        work.transition_to(TaskState.PENDING)
                    submit(work)
                while (
                    not exhausted
                    and len(in_flight) + len(retry_wait) + len(abandoned)
                    < self._max_pending
                ):
                    try:
                        item = next(iterator)
                    except StopIteration:
                        exhausted = True
                    else:
                        work = WorkItem(next_index, item)
                        next_index += 1
                        submit(work)
                if not in_flight and not retry_wait and exhausted:
                    # Abandoned executions can produce nothing further; the
                    # executor's shutdown is what waits for those workers.
                    # But when input REMAINS and abandoned futures fill the
                    # whole window, we must keep waiting for a leaked worker
                    # to return and free a slot — returning here would
                    # silently truncate the batch.
                    return

                timeout = self._next_wakeup(
                    in_flight, retry_wait, overall_deadline, task_timeout
                )
                # The slice cap bounds how stale a Cancellation request (or a
                # Windows Ctrl+C) can go unnoticed while blocked here.
                timeout = (
                    _MAX_WAIT_SLICE
                    if timeout is None
                    else min(timeout, _MAX_WAIT_SLICE)
                )
                wait_set = in_flight.keys() | abandoned
                if wait_set:
                    done, _ = wait(
                        wait_set, return_when=FIRST_COMPLETED, timeout=timeout
                    )
                else:
                    # wait() over an EMPTY set returns instantly regardless of
                    # timeout (len(done) == len(fs) short-circuit), which
                    # would turn a pure-backoff phase into a CPU-pegging spin.
                    # Nothing can complete here, so a plain sleep is exact.
                    if timeout > 0:
                        time.sleep(timeout)
                    done = ()

                for future in done:
                    if future in abandoned:
                        abandoned.discard(future)  # leaked worker freed a slot
                        continue
                    work = in_flight.pop(future)
                    _record_escaped_exception(work, future)
                    if work.state is TaskState.RETRY_WAIT:
                        assert retry is not None
                        retry_wait[work] = time.monotonic() + retry.delay_for(
                            work.attempts
                        )
                    else:
                        yield work

                if task_timeout is not None:
                    now = time.monotonic()
                    for future, work in list(in_flight.items()):
                        if not work.time_out_if_expired(now, task_timeout):
                            continue
                        del in_flight[future]
                        abandoned.add(future)
                        assert work.exception is not None
                        if retry is not None and retry.should_retry(
                            work.exception, work.attempts
                        ):
                            clone = work.retry_clone()
                            retry_wait[clone] = time.monotonic() + retry.delay_for(
                                clone.attempts
                            )
                        else:
                            yield work
        finally:
            cancelled = _cancel_pending(in_flight)
            for work in retry_wait:
                work.cancel()
            cancelled += len(retry_wait)
            if self._on_leak is not None:
                # After _cancel_pending, an uncancelled future is one a worker
                # already picked up: genuinely running, plus the abandoned
                # (timed-out) executions.
                self._on_leak(
                    {f for f in in_flight if not f.cancelled()} | abandoned
                )
            if cancelled:
                logger.info(
                    "cancelled %d pending task(s); %d running task(s) will "
                    "finish in the background",
                    cancelled,
                    len(in_flight) + len(abandoned) - _cancelled_in(in_flight),
                )

    def _next_wakeup(
        self,
        in_flight: dict[Future[None], WorkItem[T, R]],
        retry_wait: dict[WorkItem[T, R], float],
        overall_deadline: float | None,
        task_timeout: float | None,
    ) -> float | None:
        """Earliest moment the coordinator must act, as a wait() timeout."""
        candidates: list[float] = []
        if retry_wait:
            candidates.append(min(retry_wait.values()))
        if overall_deadline is not None:
            candidates.append(overall_deadline)
        if task_timeout is not None and in_flight:
            deadlines = [
                deadline
                for work in in_flight.values()
                if (deadline := work.running_deadline(task_timeout)) is not None
            ]
            if deadlines:
                candidates.append(min(deadlines))
            else:
                # Nothing running yet, but a queued item may start any moment
                # — re-check within one timeout period at the latest.
                candidates.append(time.monotonic() + task_timeout)
        if not candidates:
            return None
        return max(0.0, min(candidates) - time.monotonic())


def _run_one(
    work: WorkItem[T, R], fn: Callable[[T], R], retry: RetryPolicy | None
) -> None:
    """Runs on a worker thread.

    The retry decision is made here, where the exception and attempt count
    are both at hand: a retryable failure parks the item in RETRY_WAIT for
    the coordinator to reschedule; anything else finalizes — unless the
    coordinator already timed the item out, in which case the outcome is
    dropped inside the finish_* compare-and-set.
    """
    work.start_execution()
    start = time.perf_counter()
    try:
        result = fn(work.item)
    except Exception as exc:
        work.finish_failure(exc, time.perf_counter() - start, retry)
    except BaseException as exc:
        # Finalize under the item lock BEFORE the exception reaches the
        # future, then keep propagating — see finish_escape.
        work.finish_escape(exc, time.perf_counter() - start)
        raise
    else:
        work.finish_success(result, time.perf_counter() - start)


def _record_escaped_exception(
    work: WorkItem[Any, Any], future: Future[None]
) -> None:
    # Escaping BaseExceptions are normally finalized worker-side by
    # finish_escape, so this is pure defense: it only fires if something
    # inside tanda's own worker path raised (e.g. start_execution hitting an
    # invariant violation). A completed future with no exception and a
    # non-terminal state is the normal RETRY_WAIT hand-off — leave it. If
    # the item is in a state with no legal path to FAILED, transition_to
    # raises InvalidStateTransition — an internal invariant broke, and that
    # must surface loudly rather than yield a stale item.
    exc = future.exception()
    if exc is None:
        return
    with work.lock:
        if work.state in TERMINAL_STATES:
            return
        work.exception = exc
        work.transition_to(TaskState.FAILED)


def _cancel_pending(in_flight: dict[Future[None], WorkItem[Any, Any]]) -> int:
    cancelled = 0
    for future, work in in_flight.items():
        # cancel() only succeeds for tasks the executor has not started, so a
        # True result guarantees the item is still PENDING.
        if future.cancel():
            work.cancel()
            cancelled += 1
    return cancelled


def _cancelled_in(in_flight: dict[Future[None], WorkItem[Any, Any]]) -> int:
    return sum(1 for w in in_flight.values() if w.state is TaskState.CANCELLED)
