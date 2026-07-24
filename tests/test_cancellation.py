"""Cancellation: token semantics, two levels, backoff interruption."""

import threading
import time

import pytest

from tanda import Cancellation, Cancelled, Pool, RetryPolicy

WAIT = 5.0


# --- token object ------------------------------------------------------------


def test_token_starts_unrequested():
    cancel = Cancellation()
    assert cancel.requested is False
    cancel.raise_if_requested()  # no-op


def test_request_is_idempotent_and_observable():
    cancel = Cancellation()
    cancel.request()
    cancel.request()
    assert cancel.requested is True
    with pytest.raises(Cancelled):
        cancel.raise_if_requested()


def test_wait_unblocks_on_request():
    cancel = Cancellation()
    threading.Timer(0.05, cancel.request).start()
    assert cancel.wait(WAIT) is True


# --- cancel before anything ran ----------------------------------------------


def test_pre_requested_token_cancels_before_any_execution():
    cancel = Cancellation()
    cancel.request()
    executed = []

    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled):
            pool.map([1, 2, 3], executed.append, cancel=cancel)
    assert executed == []


def test_pre_requested_token_applies_to_imap_and_collect():
    cancel = Cancellation()
    cancel.request()
    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled):
            list(pool.imap_unordered([1], lambda x: x, cancel=cancel))
        with pytest.raises(Cancelled):
            pool.map([1], lambda x: x, cancel=cancel, error_policy="collect")


# --- cancel while running ----------------------------------------------------


def test_cancel_from_another_thread_cancels_pending_and_returns_promptly():
    # workers=1: "blocked" pins the worker; "never" stays queued. Requesting
    # cancellation must cancel "never" and raise Cancelled without waiting
    # for the blocked task.
    cancel = Cancellation()
    gate = threading.Event()
    started = threading.Event()
    executed = []
    outcome = {}

    def fn(item):
        executed.append(item)
        if item == "blocked":
            started.set()
            assert gate.wait(WAIT)
        return item

    def consume(pool):
        try:
            pool.map(["blocked", "never"], fn, cancel=cancel)
        except BaseException as exc:  # noqa: BLE001 - capturing for assertion
            outcome["error"] = exc

    with Pool(workers=1) as pool:
        consumer = threading.Thread(target=consume, args=(pool,))
        consumer.start()
        assert started.wait(WAIT)
        start = time.perf_counter()
        cancel.request()
        consumer.join(timeout=WAIT)
        elapsed = time.perf_counter() - start
        assert not consumer.is_alive()
        gate.set()  # free the pinned worker so close() can finish

    assert isinstance(outcome["error"], Cancelled)
    assert executed == ["blocked"]  # "never" was genuinely cancelled
    assert elapsed < WAIT


def test_cooperative_function_observes_the_token():
    # Opt-in cooperation needs no special signature: the task function just
    # closes over the caller's own token.
    cancel = Cancellation()
    running = threading.Event()
    finished = threading.Event()
    outcome = {}

    def fn(item):
        running.set()
        cancel.wait(WAIT)  # cooperative: parks until cancellation
        finished.set()
        return item

    def consume(pool):
        try:
            pool.map([1], fn, cancel=cancel)
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    with Pool(workers=2) as pool:
        consumer = threading.Thread(target=consume, args=(pool,))
        consumer.start()
        assert running.wait(WAIT)
        cancel.request()
        assert finished.wait(WAIT)  # the running fn saw the request and exited
        consumer.join(timeout=WAIT)
        assert not consumer.is_alive()

    assert isinstance(outcome["error"], Cancelled)


# --- cancel during retry backoff ---------------------------------------------


def test_cancel_interrupts_retry_backoff():
    cancel = Cancellation()
    attempted = threading.Event()
    calls = []
    outcome = {}

    def fn(item):
        calls.append(item)
        attempted.set()
        raise ValueError("flaky")

    def consume(pool):
        try:
            pool.map(
                [1],
                fn,
                retry=RetryPolicy(max_attempts=5, backoff=30.0),
                cancel=cancel,
            )
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    start = time.perf_counter()
    with Pool(workers=2) as pool:
        consumer = threading.Thread(target=consume, args=(pool,))
        consumer.start()
        assert attempted.wait(WAIT)
        cancel.request()
        consumer.join(timeout=WAIT)
        assert not consumer.is_alive()

    assert isinstance(outcome["error"], Cancelled)
    assert calls == [1]  # the 30s backoff never produced a second attempt
    assert time.perf_counter() - start < WAIT


# --- Cancelled raised by the task function -----------------------------------


def test_cancelled_from_fn_propagates_in_collect_mode_too():
    # Collect mode must not downgrade a stop request to a routine
    # TaskFailure — same category-preservation rule as KeyboardInterrupt.
    def fn(item):
        raise Cancelled("stop everything")

    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled):
            pool.map([1], fn, error_policy="collect")


def test_cancel_request_wins_over_overall_timeout():
    # Documented precedence: when both are expired at the same check, the
    # explicit stop request raises Cancelled, not OverallTimeout.
    from tanda import OverallTimeout  # noqa: F401 - documents the loser

    cancel = Cancellation()
    cancel.request()
    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled):
            pool.map(
                [1],
                lambda x: x,
                cancel=cancel,
                overall_timeout=0.000001,  # also already expired at the check
            )


def test_cancelled_from_fn_propagates_unwrapped_and_is_never_retried():
    calls = []

    def fn(item):
        calls.append(item)
        raise Cancelled("stop everything")

    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled, match="stop everything"):
            pool.map([1], fn, retry=RetryPolicy(max_attempts=5))
    assert calls == [1]


# --- token reuse -------------------------------------------------------------


def test_requested_token_stays_requested_across_calls():
    cancel = Cancellation()
    cancel.request()
    with Pool(workers=2) as pool:
        with pytest.raises(Cancelled):
            pool.map([1], lambda x: x, cancel=cancel)
        with pytest.raises(Cancelled):
            pool.map([2], lambda x: x, cancel=cancel)
        # The pool itself remains fully usable without the token.
        assert pool.map([3], lambda x: x) == [3]
