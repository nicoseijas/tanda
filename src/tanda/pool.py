"""Pool — the public entry point.

A Pool creates and owns its ThreadPoolExecutor outright; per the
BoundedScheduler contract, nothing outside the pool may shut it down or
cancel its futures, which is why no executor parameter is accepted.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from typing import Final, Literal, NoReturn, TypeVar, cast, overload

from tanda._scheduler import BoundedScheduler, WorkItem, default_max_pending
from tanda._states import TaskState
from tanda.cancellation import Cancellation
from tanda.exceptions import Cancelled, ShutdownTimeout, TaskError, TaskTimeout
from tanda.progress import DefaultProgress, ProgressReporter
from tanda.results import BatchResult, TaskFailure, TaskResult
from tanda.retry import RetryPolicy

T = TypeVar("T")
R = TypeVar("R")

logger = logging.getLogger("tanda")

ErrorPolicy = Literal["raise", "collect"]
_ERROR_POLICIES = ("raise", "collect")


class _Unset:
    """Distinguishes ``close()`` with no timeout argument from an explicit
    ``timeout=None`` (wait forever), which must override a pool default."""


_UNSET: Final = _Unset()


def _report(reporter_call: Callable[..., None], *args: object) -> None:
    """Progress is an observation and never controls execution: a reporter's
    own Exception must not outrank or replace the task outcome the caller is
    about to receive. Logged, never propagated (BaseExceptions still do)."""
    try:
        reporter_call(*args)
    except Exception:
        logger.warning("progress reporter raised; ignoring", exc_info=True)

_FAILURE_STATES = (TaskState.FAILED, TaskState.TIMED_OUT)


def _resolve_reporter(progress: bool | ProgressReporter) -> ProgressReporter | None:
    if progress is False or progress is None:
        return None
    if progress is True:
        return DefaultProgress()
    methods = ("start", "advance", "finish")
    if all(callable(getattr(progress, name, None)) for name in methods):
        return progress
    raise TypeError(
        "progress must be a bool or an object with start/advance/finish, "
        f"got {type(progress).__name__}"
    )


def _total_of(items: Iterable[object]) -> int | None:
    try:
        return len(items)  # type: ignore[arg-type]
    except TypeError:
        return None  # unsized input: never materialized just to count it


def _validate_timeouts(
    task_timeout: float | None, overall_timeout: float | None
) -> None:
    if task_timeout is not None and task_timeout <= 0:
        raise ValueError(f"task_timeout must be > 0, got {task_timeout}")
    if overall_timeout is not None and overall_timeout <= 0:
        raise ValueError(f"overall_timeout must be > 0, got {overall_timeout}")


def _validate_shutdown_timeout(shutdown_timeout: float | None) -> None:
    # 0 is meaningful here (unlike the task timeouts): "cancel what is queued
    # and give up on the rest immediately".
    if shutdown_timeout is not None and shutdown_timeout < 0:
        raise ValueError(
            f"shutdown_timeout must be >= 0, got {shutdown_timeout}"
        )


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

    A Pool belongs to one coordinator thread, and that contract is enforced
    rather than merely documented: a second thread entering ``map()``,
    ``imap()``, or ``imap_unordered()``, or calling ``close()`` while a
    batch runs, raises
    ``RuntimeError``. This covers reentrancy too — a task function calling
    back into its own pool runs on a worker thread, so it fails loudly
    instead of waiting for slots that only its own caller can free. Without
    the check, concurrent runs would each get their own ``max_pending``
    window (breaking the pool-wide bound) and a cross-thread ``close()``
    could hang the coordinator forever (see BoundedScheduler).

    On KeyboardInterrupt inside ``map()``, pending tasks are cancelled
    immediately; the executor itself is shut down by ``__exit__``/``close()``
    — one more reason the ``with`` block is the primary path.

    ``close()`` waits for running tasks — including executions already
    declared timed out, which cannot be killed. By default it waits
    indefinitely, so a task stuck forever hangs ``close()`` forever even
    though ``map()`` returned at its deadline; ``shutdown_timeout`` bounds
    that wait and raises :class:`ShutdownTimeout` instead.
    """

    def __init__(
        self,
        workers: int | None = None,
        max_pending: int | None = None,
        *,
        shutdown_timeout: float | None = None,
    ) -> None:
        if workers is not None and workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")
        if max_pending is not None and max_pending < 1:
            raise ValueError(f"max_pending must be >= 1, got {max_pending}")
        _validate_shutdown_timeout(shutdown_timeout)
        self._workers = workers if workers is not None else _default_workers()
        self._max_pending = (
            max_pending
            if max_pending is not None
            else default_max_pending(self._workers)
        )
        self._shutdown_timeout = shutdown_timeout
        # Futures whose workers are still inside fn after a run ended:
        # abandoned (timed-out) executions, plus tasks left running by a
        # cancelled or failed batch. The only things close() can wait on.
        self._leaked: set[Future[None]] = set()
        # Guards the ownership bookkeeping only — never held across user code
        # or across a wait.
        self._lock = threading.Lock()
        self._run_owner: int | None = None  # thread ident of the coordinator
        self._run_depth = 0
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="tanda"
        )
        self._scheduler: BoundedScheduler | None = BoundedScheduler(
            self._executor, self._max_pending, on_leak=self._record_leaks
        )

    @property
    def workers(self) -> int:
        return self._workers

    @property
    def max_pending(self) -> int:
        return self._max_pending

    @property
    def shutdown_timeout(self) -> float | None:
        return self._shutdown_timeout

    def _record_leaks(self, futures: set[Future[None]]) -> None:
        # Called from the scheduler's teardown. Drop settled futures from
        # earlier runs so the set stays O(in-flight), not O(runs).
        with self._lock:
            self._leaked = {f for f in self._leaked if not f.done()} | futures

    @contextmanager
    def _active_run(self) -> Iterator[None]:
        """Marks a batch as running on this thread, enforcing the
        single-coordinator contract loudly instead of hanging.

        Same-thread nesting is allowed (it is bounded by the same window and
        cannot deadlock); a second thread entering — including a task function
        calling back into its own pool, which always runs on a worker thread —
        is a bug that would otherwise corrupt the pool-wide ``max_pending``
        bound or deadlock, so it raises.
        """
        ident = threading.get_ident()
        with self._lock:
            if self._run_owner is not None and self._run_owner != ident:
                raise RuntimeError(
                    "Pool is already running a batch on another thread "
                    f"({self._run_owner}); a Pool belongs to one coordinator "
                    "thread and task functions must not call back into it. "
                    "Use a separate Pool per thread."
                )
            self._run_owner = ident
            self._run_depth += 1
        try:
            yield
        finally:
            with self._lock:
                # A generator abandoned without close() runs this from
                # whatever thread finalizes it; leaving another coordinator's
                # claim alone is the safe direction to err in.
                if self._run_owner == ident:
                    self._run_depth -= 1
                    if self._run_depth == 0:
                        self._run_owner = None

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
        progress: bool | ProgressReporter = False,
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
        progress: bool | ProgressReporter = False,
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
        progress: bool | ProgressReporter = False,
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
        reporter = _resolve_reporter(progress)
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        with self._active_run():
            if error_policy == "collect":
                return _collect(
                    scheduler,
                    items,
                    fn,
                    retry,
                    task_timeout,
                    overall_timeout,
                    cancel,
                    reporter,
                )
            if reporter is not None:
                _report(reporter.start, _total_of(items))
            completed_values: dict[int, R] = {}
            completion_stream = scheduler.run(
                items, fn, retry, task_timeout, overall_timeout, cancel
            )
            try:
                for work in completion_stream:
                    if reporter is not None:
                        _report(reporter.advance)
                    completed_values[work.index] = _successful_value(work)
            finally:
                # Deterministic cleanup: cancel pending tasks now, not at GC.
                completion_stream.close()
                if reporter is not None:
                    _report(reporter.finish)
        try:
            return [completed_values[i] for i in range(len(completed_values))]
        except KeyError as exc:
            raise RuntimeError(
                "internal invariant violated: non-contiguous result indices "
                f"(missing {exc.args[0]})"
            ) from exc

    def imap(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
        progress: bool | ProgressReporter = False,
    ) -> Iterator[R]:
        """Apply ``fn`` to every item, yielding results in input order.

        The streaming counterpart of ``map()``: result ``i`` is yielded as
        soon as items ``0..i`` have all completed, without waiting for the
        rest of the batch. Completions that arrive out of order are held in
        an internal buffer until their turn. Task submission stays bounded
        by ``max_pending``, but that buffer holds finished *results* and is
        not bounded by the window: one slow head item can grow it up to
        O(completed) while later items finish behind it. When completion
        order is acceptable, ``imap_unordered()`` streams in constant
        memory.

        Error semantics match ``map()``: the first definitive failure
        cancels pending tasks and raises immediately — in completion order,
        even if earlier-index results were still being held. Everything
        else follows ``imap_unordered()``: close the iterator if you stop
        consuming early, and with ``progress`` the reporter starts eagerly
        at call time, advances when an item *completes* (not when its
        result is yielded), and finishes in the stream's cleanup.
        """
        _validate_timeouts(task_timeout, overall_timeout)
        if self._scheduler is None:
            # Raise here, at call time, not lazily inside the generator.
            raise RuntimeError("Pool is closed")
        reporter = _resolve_reporter(progress)
        if reporter is not None:
            _report(reporter.start, _total_of(items))
        return self._stream_ordered(
            items, fn, retry, task_timeout, overall_timeout, cancel, reporter
        )

    def _stream_ordered(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        retry: RetryPolicy | None,
        task_timeout: float | None,
        overall_timeout: float | None,
        cancel: Cancellation | None,
        reporter: ProgressReporter | None,
    ) -> Iterator[R]:
        # Same closed-state discipline as _stream_unordered, checked before
        # EVERY resumption — including yields served from the reorder
        # buffer. Buffered values don't touch the scheduler, but serving
        # them after close() would make post-close behavior depend on which
        # completions happened to be buffered; a deterministic error wins.
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        # As in _stream_unordered, the run is claimed on first resumption,
        # not at imap() call time: a stream that is created and never
        # iterated must not leave the pool looking permanently busy.
        with self._active_run():
            completion_stream = scheduler.run(
                items, fn, retry, task_timeout, overall_timeout, cancel
            )
            # Completed values whose turn has not come. Unwrapped at pull
            # time so a failure raises fail-fast, never sits buffered.
            held: dict[int, R] = {}
            next_index = 0
            try:
                while True:
                    if self._scheduler is None:
                        raise RuntimeError("Pool was closed while streaming")
                    if next_index in held:
                        yield held.pop(next_index)
                        next_index += 1
                        continue
                    try:
                        work = next(completion_stream)
                    except StopIteration:
                        break
                    if reporter is not None:
                        _report(reporter.advance)
                    held[work.index] = _successful_value(work)
            finally:
                # Runs on close(), exhaustion, or an in-flight failure: cancel
                # not-yet-started tasks deterministically.
                completion_stream.close()
                if reporter is not None:
                    _report(reporter.finish)
            if held:
                raise RuntimeError(
                    "internal invariant violated: non-contiguous result "
                    f"indices (missing {next_index})"
                )

    def imap_unordered(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        *,
        retry: RetryPolicy | None = None,
        task_timeout: float | None = None,
        overall_timeout: float | None = None,
        cancel: Cancellation | None = None,
        progress: bool | ProgressReporter = False,
    ) -> Iterator[R]:
        """Apply ``fn`` to every item, yielding results as they complete.

        Completion order, not input order — use this to consume results
        immediately. The stream is lazy end to end: a slow consumer applies
        backpressure to submission instead of accumulating results. Error
        semantics match ``map()`` — the first definitive failure cancels
        pending tasks and re-raises. If you stop consuming early, close the
        iterator (a ``for`` loop's ``break`` does not; ``close()`` or
        exhausting it does) — an abandoned iterator only cleans up when the
        GC finalizes it. With ``progress``, the reporter starts eagerly at
        call time, but ``finish()`` lives in the stream's cleanup: a stream
        that is never iterated (nor closed after starting) leaves the
        reporter started and never finished.
        """
        _validate_timeouts(task_timeout, overall_timeout)
        if self._scheduler is None:
            # Raise here, at call time, not lazily inside the generator.
            raise RuntimeError("Pool is closed")
        reporter = _resolve_reporter(progress)
        if reporter is not None:
            # Eager, like the closed-check: the display exists (and shows the
            # total) as soon as the stream is created, not at first next().
            _report(reporter.start, _total_of(items))
        return self._stream_unordered(
            items, fn, retry, task_timeout, overall_timeout, cancel, reporter
        )

    def _stream_unordered(
        self,
        items: Iterable[T],
        fn: Callable[[T], R],
        retry: RetryPolicy | None,
        task_timeout: float | None,
        overall_timeout: float | None,
        cancel: Cancellation | None,
        reporter: ProgressReporter | None,
    ) -> Iterator[R]:
        # The pool's closed state is re-checked before every resumption: a
        # same-thread close() while this generator is suspended leaves
        # drained futures in plain CANCELLED, and re-entering the
        # scheduler's wait() over those would hang forever (see
        # BoundedScheduler). Failing loudly here makes that impossible.
        scheduler = self._scheduler
        if scheduler is None:
            raise RuntimeError("Pool is closed")
        # Claimed on first resumption, not at imap_unordered() call time: a
        # stream that is created and never iterated must not leave the pool
        # looking permanently busy. The claim then spans the suspensions too,
        # so a *different* thread cannot start a batch mid-stream.
        with self._active_run():
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
                    if reporter is not None:
                        _report(reporter.advance)
                    yield _successful_value(work)
            finally:
                # Runs on close(), exhaustion, or an in-flight failure: cancel
                # not-yet-started tasks deterministically.
                completion_stream.close()
                if reporter is not None:
                    _report(reporter.finish)

    def close(self, *, timeout: float | None | _Unset = _UNSET) -> None:
        """Shut down the pool. Idempotent.

        Queued work is cancelled and no further batch can start. Tasks already
        inside ``fn`` cannot be killed, so the only choice is how long to wait
        for them: ``timeout=None`` waits indefinitely (the default, and what
        the stdlib does), a number waits at most that many seconds and then
        raises :class:`ShutdownTimeout`, and ``0`` gives up immediately.
        Omitting the argument uses the pool's ``shutdown_timeout``.

        Giving up does not stop anything — the abandoned workers keep running
        and, because pool threads are not daemons, keep the interpreter alive
        at exit. A bounded shutdown buys a diagnosable error instead of a
        silent hang, not the ability to kill a thread.

        Must be called from the coordinator thread. Calling it while another
        thread is inside ``map()`` or an ``imap_unordered()`` stream raises
        rather than hanging that thread on futures that can never report done.
        """
        resolved = (
            self._shutdown_timeout if isinstance(timeout, _Unset) else timeout
        )
        _validate_shutdown_timeout(resolved)
        with self._lock:
            owner = self._run_owner
            if owner is not None and owner != threading.get_ident():
                raise RuntimeError(
                    "close() called while a batch is running on thread "
                    f"{owner}; shutting the executor down under a "
                    "running coordinator would hang it. Close the pool from "
                    "the thread that owns it."
                )
        if self._executor is None:
            return
        executor, self._executor = self._executor, None
        self._scheduler = None
        if resolved is None:
            # Safe against the wait()-hang documented on BoundedScheduler ONLY
            # because of the single-coordinator contract enforced above: no
            # run() loop can be waiting on the futures this drains.
            executor.shutdown(wait=True, cancel_futures=True)
            return
        # Drain the queue without joining, then bound the wait ourselves —
        # shutdown(wait=True) has no timeout. Only leaked futures are waited
        # on: cancelled ones are drained and never reported done by wait().
        executor.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            leaked = {f for f in self._leaked if not f.done()}
            self._leaked = set()
        if not leaked:
            return
        _, still_running = wait(leaked, timeout=resolved)
        if still_running:
            raise ShutdownTimeout(len(still_running), resolved)

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        try:
            self.close()
        except ShutdownTimeout:
            if exc_type is None:
                raise
            # Never let a shutdown complaint replace the error that actually
            # ended the block — that one is what the caller needs to see.
            logger.warning("shutdown timed out while unwinding", exc_info=True)


def _collect(
    scheduler: BoundedScheduler,
    items: Iterable[T],
    fn: Callable[[T], R],
    retry: RetryPolicy | None,
    task_timeout: float | None,
    overall_timeout: float | None,
    cancel: Cancellation | None,
    reporter: ProgressReporter | None,
) -> BatchResult[T, R]:
    successful: list[TaskResult[T, R]] = []
    failed: list[TaskFailure[T]] = []
    if reporter is not None:
        _report(reporter.start, _total_of(items))
    completion_stream = scheduler.run(
        items, fn, retry, task_timeout, overall_timeout, cancel
    )
    try:
        for work in completion_stream:
            if reporter is not None:
                _report(reporter.advance)
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
        if reporter is not None:
            _report(reporter.finish)
    successful.sort(key=lambda r: r.index)
    failed.sort(key=lambda f: f.index)
    return BatchResult(successful=tuple(successful), failed=tuple(failed))
