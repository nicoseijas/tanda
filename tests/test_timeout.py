"""task_timeout and overall_timeout: honest semantics, no fake kills."""

import threading
import time

import pytest

from tanda import (
    OverallTimeout,
    Pool,
    RetryPolicy,
    TaskError,
    TaskTimeout,
)

WAIT = 5.0


# --- validation --------------------------------------------------------------


@pytest.mark.parametrize("kwargs", [{"task_timeout": 0}, {"task_timeout": -1}])
def test_invalid_task_timeout_raises(kwargs):
    with Pool(workers=2) as pool:
        with pytest.raises(ValueError):
            pool.map([1], lambda x: x, **kwargs)


@pytest.mark.parametrize("kwargs", [{"overall_timeout": 0}, {"overall_timeout": -1}])
def test_invalid_overall_timeout_raises(kwargs):
    with Pool(workers=2) as pool:
        with pytest.raises(ValueError):
            pool.map([1], lambda x: x, **kwargs)


# --- task_timeout ------------------------------------------------------------


def test_fast_tasks_are_unaffected_by_task_timeout():
    with Pool(workers=4) as pool:
        assert pool.map(range(10), lambda x: x * 2, task_timeout=30) == [
            x * 2 for x in range(10)
        ]


def test_blocked_task_raises_task_timeout_promptly():
    gate = threading.Event()

    def fn(item):
        assert gate.wait(WAIT)
        return item

    start = time.perf_counter()
    with Pool(workers=2) as pool:
        with pytest.raises(TaskTimeout) as excinfo:
            pool.map(["x"], fn, task_timeout=0.05)
        elapsed = time.perf_counter() - start
        gate.set()

    err = excinfo.value
    assert err.item == "x"
    assert err.index == 0
    assert err.attempts == 1
    assert isinstance(err.exception, TimeoutError)
    assert isinstance(err, TaskError)  # catchable as the general error too
    assert elapsed < WAIT  # raised at the deadline, not when fn finished


def test_running_task_is_not_killed_and_its_result_is_discarded():
    # The honest semantics: after the timeout fires, the underlying call
    # keeps executing to completion; its late result goes nowhere.
    gate = threading.Event()
    finished = threading.Event()

    def fn(item):
        assert gate.wait(WAIT)
        finished.set()
        return "late value"

    with Pool(workers=2) as pool:
        with pytest.raises(TaskTimeout):
            pool.map(["x"], fn, task_timeout=0.05)
        assert not finished.is_set()  # still running after map() raised
        gate.set()
        assert finished.wait(WAIT)  # not killed: it ran to completion
        # The pool remains usable; the leaked execution's result was dropped.
        assert pool.map([1, 2], lambda x: x + 1) == [2, 3]


def test_queued_time_does_not_count_against_task_timeout():
    # workers=1: item "slow" blocks the only worker for ~0.4s, far past the
    # 0.1s task_timeout of item "queued" — which must still succeed, because
    # its clock only starts when it begins executing.
    release = threading.Event()

    def fn(item):
        if item == "slow":
            release.wait(0.4)
        return item

    with Pool(workers=1) as pool:
        batch = pool.map(
            ["slow", "queued"], fn, task_timeout=0.1, error_policy="collect"
        )

    assert [f.item for f in batch.failed] == ["slow"]
    assert isinstance(batch.failed[0].exception, TimeoutError)
    assert [r.item for r in batch.successful] == ["queued"]


def test_collect_mode_records_timeouts_as_failures():
    gate = threading.Event()

    def fn(item):
        if item == "stuck":
            assert gate.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        batch = pool.map(
            ["stuck", "ok"], fn, task_timeout=0.05, error_policy="collect"
        )
        gate.set()

    [failure] = batch.failed
    assert failure.item == "stuck"
    assert isinstance(failure.exception, TimeoutError)
    assert [r.item for r in batch.successful] == ["ok"]


def test_imap_unordered_supports_task_timeout():
    gate = threading.Event()

    def fn(item):
        if item == "stuck":
            assert gate.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        stream = pool.imap_unordered(["stuck", "ok"], fn, task_timeout=0.05)
        assert next(stream) == "ok"
        with pytest.raises(TaskTimeout):
            next(stream)
        gate.set()


def test_abandoned_execution_blocks_new_submission_until_it_returns():
    # workers=1, max_pending=1: the timed-out-but-still-running execution
    # must keep the sole window slot occupied — "next" may only start after
    # the leaked worker actually returns.
    hold = threading.Event()
    order: list[str] = []
    lock = threading.Lock()

    def fn(item):
        if item == "stuck":
            hold.wait(0.3)
            with lock:
                order.append("stuck-finished")
            return item
        with lock:
            order.append("next-ran")
        return item

    with Pool(workers=1, max_pending=1) as pool:
        batch = pool.map(
            ["stuck", "next"], fn, task_timeout=0.05, error_policy="collect"
        )

    assert order == ["stuck-finished", "next-ran"]
    assert [f.item for f in batch.failed] == ["stuck"]
    assert [r.item for r in batch.successful] == ["next"]


def test_abandoned_tasks_filling_the_window_do_not_truncate_the_batch():
    # Two blocked items time out and their leaked executions occupy the whole
    # window (max_pending=2). The remaining input must still be processed
    # once the leaked workers return — never silently dropped.
    hold = threading.Event()

    def fn(item):
        if item in ("block0", "block1"):
            hold.wait(0.3)  # far past the 0.05s timeout, then frees the slot
        return item

    with Pool(workers=2, max_pending=2) as pool:
        batch = pool.map(
            ["block0", "block1", "fast2", "fast3"],
            fn,
            task_timeout=0.05,
            error_policy="collect",
        )

    assert sorted(f.item for f in batch.failed) == ["block0", "block1"]
    assert all(isinstance(f.exception, TimeoutError) for f in batch.failed)
    assert sorted(r.item for r in batch.successful) == ["fast2", "fast3"]


# --- task_timeout + retry ----------------------------------------------------


def test_timed_out_task_is_retried_when_policy_matches():
    gate = threading.Event()
    calls: list[int] = []
    lock = threading.Lock()

    def fn(item):
        with lock:
            calls.append(1)
            n = len(calls)
        if n == 1:
            assert gate.wait(WAIT)  # first execution hangs past the timeout
            return "stale"
        return "fresh"

    with Pool(workers=2) as pool:
        batch = pool.map(
            ["a"],
            fn,
            task_timeout=0.05,
            retry=RetryPolicy(max_attempts=2),  # TimeoutError is an Exception
            error_policy="collect",
        )
        gate.set()

    [ok] = batch.successful
    assert ok.value == "fresh"  # the fresh attempt's value, never "stale"
    assert ok.attempts == 2
    assert len(calls) == 2
    # elapsed carries the observed time of the timed-out execution.
    assert ok.elapsed >= 0.05


def test_timeout_not_retried_when_policy_excludes_it():
    gate = threading.Event()

    def fn(item):
        assert gate.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        with pytest.raises(TaskTimeout) as excinfo:
            pool.map(
                ["x"],
                fn,
                task_timeout=0.05,
                retry=RetryPolicy(max_attempts=5, retry_on=(ValueError,)),
            )
        gate.set()
    assert excinfo.value.attempts == 1


# --- overall_timeout ---------------------------------------------------------


def test_overall_timeout_raises_promptly():
    gate = threading.Event()

    def fn(item):
        assert gate.wait(WAIT)
        return item

    start = time.perf_counter()
    with Pool(workers=2) as pool:
        with pytest.raises(OverallTimeout):
            pool.map([1, 2, 3], fn, overall_timeout=0.1)
        elapsed = time.perf_counter() - start
        gate.set()
    assert elapsed < WAIT


def test_overall_timeout_applies_in_collect_mode_too():
    # Partial results cannot be silently returned; the deadline fails loudly.
    gate = threading.Event()

    def fn(item):
        if item == "stuck":
            assert gate.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        with pytest.raises(OverallTimeout):
            pool.map(["ok", "stuck"], fn, overall_timeout=0.1, error_policy="collect")
        gate.set()


def test_overall_timeout_fires_during_retry_backoff():
    # The overall deadline must bound the coordinator's backoff sleep too.
    def fn(item):
        raise ValueError("flaky")

    start = time.perf_counter()
    with Pool(workers=2) as pool:
        with pytest.raises(OverallTimeout):
            pool.map(
                [1],
                fn,
                retry=RetryPolicy(max_attempts=5, backoff=10.0),
                overall_timeout=0.1,
            )
    assert time.perf_counter() - start < WAIT


def test_overall_timeout_not_triggered_when_work_finishes_first():
    with Pool(workers=2) as pool:
        assert pool.map([1, 2], lambda x: x, overall_timeout=30) == [1, 2]
