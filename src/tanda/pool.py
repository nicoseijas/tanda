"""Pool — the public entry point.

A Pool creates and owns its ThreadPoolExecutor outright; per the
BoundedScheduler contract, nothing outside the pool may shut it down or
cancel its futures, which is why no executor parameter is accepted.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, NoReturn, TypeVar, cast, overload

from tanda._scheduler import BoundedScheduler, WorkItem, default_max_pending
from tanda._states import TaskState
from tanda.cancellation import Cancellation
from tanda.exceptions import Cancelled, TaskError, TaskTimeout
from tanda.results import BatchResult, TaskFailure, TaskResult
from tanda.retry import RetryPolicy

T = TypeVar("T")
R = TypeVar("R")

ErrorPolicy = Literal["raise", "collect"]
_ERROR_POLICIES = ("raise", "collect")

_FAILURE_STATES = (TaskState.FAILED, TaskState.TIMED_OUT)


def _validate_timeouts(
    task_timeout: float | None, overall_timeout: float | None
) -> None:
    if task_timeout is not None and task_timeout <= 0:
        raise ValueError(f"task_timeout must be > 0, got {task_timeout}")
    if overall_timeout is not None and overall_timeout <= 0:
        raise ValueError(f"overall_timeout must be > 0, got {overall_timeout}")


def _successful_value(work: WorkItem[T, R]) -> R:
    """Unwrap a completed WorkItem: return its value or raise TaskError."""
    if work.state is TaskState.SUCCESS:
        return cast(R, work.value)
    if work.state in _FAILURE_STATES:
        assert work.exception is not None
        if isinstance(work.exception, Cancelled):
            # A cancellation is an outcome, not a task error — never wrapped.
            raise work.exception
        if not isinstance(work.exception, Exception):
            # KeyboardInterrupt/SystemExit must keep their category: wrapping
            # them in TaskError (an Exception) would make `except
            # KeyboardInterrupt` around map() silently stop matching,
            # breaking the Ctrl+C contract in GUIDELINES.md.
            raise work.exception
        error_type = (
            TaskTimeout if work.state is TaskState.TIMED_OUT else TaskError
        )
        raise error_type(
            item=work.item,
            index=work.index,
            exception=work.exception,
            attempts=work.attempts,
            elapsed=work.elapsed,
        ) from work.exception
    _unexpected_terminal_state(work)


def _unexpected_terminal_state(work: WorkItem[T, R]) -> NoReturn:
    # No terminal state beyond SUCCESS/FAILED is producible yet — fail loudly.
    raise RuntimeError(
        f"unexpected terminal state {work.state} for item {work.index}"
    )


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
    ``with`` block) while another thread is inside ``map()`` or consuming an
    ``imap_unordered()`` stream can hang that thread permanently (see
    BoundedScheduler), and concurrent ``map()``/``imap_unordered()`` calls
    would each get their own ``max_pending`` window, breaking the pool-wide
    bound. Drive everything from the owning thread.

    On KeyboardInterrupt inside ``map()``, pending tasks are cancelled
    immediately; the executor itself is shut down by ``__exit__``/``close()``
    — one more reason the ``with`` block is the primary path.

    ``close()`` waits for running tasks — including executions already
    declared timed out, which cannot be killed. A task stuck forever will
    hang ``close()`` forever, even though ``map()`` itself returned at the
    deadline.
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

    @overload
    def map(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
        error_policy: Literal["raise"] = "raise",
    ) -> list[R]: ...

    @overload
    def map(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
        error_policy: Literal["collect"],
    ) -> BatchResult[T, R]: ...

    def map(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
        error_policy: ErrorPolicy = "raise",
    ) -> list[R] | BatchResult[T, R]:
        """Apply ``fn`` to every item, returning results in input order.

        With a ``retry`` policy, failures matching ``retry.retry_on`` are
        re-executed up to ``retry.max_attempts`` total runs (with backoff)
        before counting as definitive; retrying is only safe for idempotent
        operations — see :class:`RetryPolicy`.

        ``task_timeout`` bounds each *execution* (queue time never counts).
        Honest semantics: Python cannot kill a running thread — a timed-out
        task raises :class:`TaskTimeout` (or becomes a TaskFailure in collect
        mode, or retries when the policy matches TimeoutError — note the
        default ``retry_on=(Exception,)`` does match it) while the underlying
        call keeps running until it returns; its late result is discarded.
        ``overall_timeout`` bounds the whole call and raises
        :class:`OverallTimeout` under every error policy.

        With ``error_policy="raise"`` (default), the first definitive failure
        cancels not-yet-started tasks and raises :class:`TaskError` — which
        carries the item, index, underlying exception (also chained as
        ``__cause__``), attempts, and elapsed time. Running tasks cannot be
        stopped and finish in the background. The chained exception keeps its
        worker-thread traceback, so it pins those frames (and their locals)
        for as long as the caller holds it.

        With ``error_policy="collect"``, every item runs regardless of
        failures and the return value is a :class:`BatchResult` with
        ``successful`` and ``failed`` lists (both ordered by input index) —
        never a mixed list of results and exceptions.

        Exceptions raised by the input iterable itself are the caller's bug
        and propagate unwrapped under both policies, as do BaseExceptions
        (KeyboardInterrupt, SystemExit) escaping the task function.

        Note for type-checker users: the overloads key on literal policy
        values; a dynamically chosen policy needs an explicit branch or a
        ``cast``.
        """
        if error_policy not in _ERROR_POLICIES:
            raise ValueError(
                f"error_policy must be 'raise' or 'collect', got {error_policy!r}"
            )
        _validate_timeouts(task_timeout, overall_timeout)
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        if error_policy == "collect":
            return _collect(
                scheduler, items, fn, retry, task_timeout, overall_timeout, cancel
            )
        completed_values: dict[int, R] = {}
        completion_stream = scheduler.run(
            items, fn, retry, task_timeout, overall_timeout, cancel
        )
        try:
            for work in completion_stream:
                completed_values[work.index] = _successful_value(work)
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

    def imap_unordered(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
    ) -> Iterator[R]:
        """Apply ``fn`` to every item, yielding results as they complete.

        Completion order, not input order — use this to consume results
        immediately. The stream is lazy end to end: a slow consumer applies
        backpressure to submission instead of accumulating results. Error
        semantics match ``map()`` — the first definitive failure cancels
        pending tasks and re-raises. If you stop consuming early, close the
        iterator (a ``for`` loop's ``break`` does not; ``close()`` or
        exhausting it does) — an abandoned iterator only cleans up when the
        GC finalizes it.
        """
        _validate_timeouts(task_timeout, overall_timeout)
        if self._scheduler is None:
            # Raise here, at call time, not lazily inside the generator.
            raise RuntimeError("Pool is closed")
        return self._stream_unordered(
            items, fn, retry, task_timeout, overall_timeout, cancel
        )

    def _stream_unordered(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        retry: RetryPolicy | None,
        task_timeout: float | None,
        overall_timeout: float | None,
        cancel: Cancellation | None,
    ) -> Iterator[R]:
        # The pool's closed state is re-checked before every resumption: a
        # same-thread close() while this generator is suspended leaves
        # drained futures in plain CANCELLED, and re-entering the
        # scheduler's wait() over those would hang forever (see
        # BoundedScheduler). Failing loudly here makes that impossible.
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        completion_stream = scheduler.run(
            items, fn, retry, task_timeout, overall_timeout, cancel
        )
        try:
            while True:
                if self._scheduler is None:
                    raise RuntimeError("Pool was closed while streaming")
                try:
                    work = next(completion_stream)
                except StopIteration:
                    return
                yield _successful_value(work)
        finally:
            # Runs on close(), exhaustion, or an in-flight failure: cancel
            # not-yet-started tasks deterministically.
            completion_stream.close()

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


def _collect(
    scheduler: BoundedScheduler,
    items: Iterable[T],
    fn: Callable[[T], R],
    retry: RetryPolicy | None,
    task_timeout: float | None,
    overall_timeout: float | None,
    cancel: Cancellation | None,
) -> BatchResult[T, R]:
    successful: list[TaskResult[T, R]] = []
    failed: list[TaskFailure[T]] = []
    completion_stream = scheduler.run(
        items, fn, retry, task_timeout, overall_timeout, cancel
    )
    try:
        for work in completion_stream:
            if work.state is TaskState.SUCCESS:
                successful.append(
                    TaskResult(
                        item=work.item,
                        index=work.index,
                        value=cast(R, work.value),
                        attempts=work.attempts,
                        elapsed=work.elapsed,
                    )
                )
            elif work.state in _FAILURE_STATES:
                assert work.exception is not None
                if isinstance(work.exception, Cancelled):
                    # Same rule as _successful_value: a cancellation is an
                    # outcome, not a task error — collecting it as a routine
                    # TaskFailure would silently downgrade a stop request.
                    raise work.exception
                if not isinstance(work.exception, Exception):
                    # Collecting a KeyboardInterrupt/SystemExit would suppress
                    # it; category-preserving propagation wins over collect.
                    raise work.exception
                failed.append(
                    TaskFailure(
                        item=work.item,
                        index=work.index,
                        exception=work.exception,
                        attempts=work.attempts,
                        elapsed=work.elapsed,
                    )
                )
            else:
                _unexpected_terminal_state(work)
    finally:
        completion_stream.close()
    successful.sort(key=lambda r: r.index)
    failed.sort(key=lambda f: f.index)
    return BatchResult(successful=tuple(successful), failed=tuple(failed))
