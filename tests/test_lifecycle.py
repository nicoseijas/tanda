"""Lifecycle: graceful shutdown, shutdown_timeout, single-coordinator contract.

Every test that pins a worker releases its gate in a ``finally``: an abandoned
worker keeps the (non-daemon) pool thread alive and would hang the test run at
interpreter exit.
"""

import threading
import time

import pytest

from tanda import Pool, ShutdownTimeout, TaskError

WAIT = 5.0
BRIEF = 0.05


def _pinned(gate: threading.Event, started: threading.Event | None = None):
    """A task function that blocks until ``gate`` is set."""

    def fn(item):
        if started is not None:
            started.set()
        gate.wait(WAIT)
        return item

    return fn


def _leak_one_worker(pool: Pool, gate: threading.Event) -> None:
    """Return from map() with one execution still running inside fn.

    task_timeout is the only honest way to get there: the coordinator gives
    up on the item, the worker keeps going.
    """
    result = pool.map(
        ["stuck"], _pinned(gate), task_timeout=BRIEF, error_policy="collect"
    )
    assert len(result.failed) == 1


# --- configuration -----------------------------------------------------------


def test_shutdown_timeout_defaults_to_waiting_forever():
    with Pool(workers=2) as pool:
        assert pool.shutdown_timeout is None


@pytest.mark.parametrize("value", [-1, -0.5])
def test_negative_shutdown_timeout_raises(value):
    with pytest.raises(ValueError, match="shutdown_timeout"):
        Pool(workers=2, shutdown_timeout=value)


def test_negative_close_timeout_raises():
    with Pool(workers=2) as pool:
        with pytest.raises(ValueError, match="shutdown_timeout"):
            pool.close(timeout=-1)


# --- bounded shutdown --------------------------------------------------------


def test_close_returns_immediately_when_nothing_is_running():
    pool = Pool(workers=2)
    assert pool.map([1, 2], lambda x: x) == [1, 2]
    pool.close(timeout=0)  # nothing to wait for, so 0 is not a timeout


def test_close_times_out_on_a_worker_still_inside_fn():
    gate = threading.Event()
    pool = Pool(workers=1)
    try:
        _leak_one_worker(pool, gate)
        with pytest.raises(ShutdownTimeout) as info:
            pool.close(timeout=BRIEF)
        assert info.value.running == 1
        assert info.value.timeout == BRIEF
    finally:
        gate.set()


def test_close_waits_for_a_worker_that_finishes_in_time():
    gate = threading.Event()
    pool = Pool(workers=1)
    try:
        _leak_one_worker(pool, gate)
        threading.Thread(target=lambda: (time.sleep(BRIEF), gate.set())).start()
        pool.close(timeout=WAIT)  # the worker returns first: no error
    finally:
        gate.set()


def test_close_is_idempotent_after_a_shutdown_timeout():
    gate = threading.Event()
    pool = Pool(workers=1)
    try:
        _leak_one_worker(pool, gate)
        with pytest.raises(ShutdownTimeout):
            pool.close(timeout=0)
        pool.close(timeout=0)  # already shut down: no second complaint
    finally:
        gate.set()


def test_pool_is_closed_after_a_shutdown_timeout():
    gate = threading.Event()
    pool = Pool(workers=1)
    try:
        _leak_one_worker(pool, gate)
        with pytest.raises(ShutdownTimeout):
            pool.close(timeout=0)
        with pytest.raises(RuntimeError, match="closed"):
            pool.map([1], lambda x: x)
    finally:
        gate.set()


def test_pool_shutdown_timeout_applies_to_the_context_manager():
    gate = threading.Event()
    try:
        with pytest.raises(ShutdownTimeout):
            with Pool(workers=1, shutdown_timeout=BRIEF) as pool:
                _leak_one_worker(pool, gate)
    finally:
        gate.set()


def test_explicit_close_timeout_overrides_the_pool_default():
    gate = threading.Event()
    pool = Pool(workers=1, shutdown_timeout=0)
    try:
        _leak_one_worker(pool, gate)
        threading.Thread(target=lambda: (time.sleep(BRIEF), gate.set())).start()
        pool.close(timeout=None)  # explicit None means wait, default or not
    finally:
        gate.set()


def test_shutdown_timeout_never_masks_the_bodys_exception(caplog):
    gate = threading.Event()
    try:
        with pytest.raises(ValueError, match="boom"):
            with Pool(workers=1, shutdown_timeout=0) as pool:
                _leak_one_worker(pool, gate)
                raise ValueError("boom")
    finally:
        gate.set()
    # Swallowed, but not silently: the leak is still reported.
    assert "shutdown timed out" in caplog.text


def test_leaked_workers_from_an_earlier_batch_are_not_waited_on_twice():
    gate = threading.Event()
    pool = Pool(workers=2)
    try:
        _leak_one_worker(pool, gate)
        gate.set()
        # The leaked execution has returned; a later clean batch must not
        # inherit it and time the shutdown out.
        assert pool.map([1, 2], lambda x: x) == [1, 2]
        pool.close(timeout=WAIT)
    finally:
        gate.set()


# --- single-coordinator contract --------------------------------------------


def test_close_from_another_thread_during_a_batch_raises():
    started, gate = threading.Event(), threading.Event()
    with Pool(workers=1) as pool:
        worker = threading.Thread(
            target=lambda: pool.map([1], _pinned(gate, started))
        )
        worker.start()
        try:
            assert started.wait(WAIT)
            with pytest.raises(RuntimeError, match="batch is running"):
                pool.close()
        finally:
            gate.set()
            worker.join(WAIT)


def test_concurrent_map_from_another_thread_raises():
    started, gate = threading.Event(), threading.Event()
    outcome = {}

    def second_map(pool):
        try:
            pool.map([2], lambda x: x)
        except RuntimeError as exc:
            outcome["error"] = exc

    with Pool(workers=2) as pool:
        first = threading.Thread(
            target=lambda: pool.map([1], _pinned(gate, started))
        )
        first.start()
        try:
            assert started.wait(WAIT)
            intruder = threading.Thread(target=second_map, args=(pool,))
            intruder.start()
            intruder.join(WAIT)
        finally:
            gate.set()
            first.join(WAIT)

    assert isinstance(outcome.get("error"), RuntimeError)
    assert "another thread" in str(outcome["error"])


def test_task_calling_back_into_its_own_pool_fails_loudly():
    with Pool(workers=2) as pool:

        def reenter(item):
            return pool.map([item], lambda x: x)

        with pytest.raises(TaskError) as info:
            pool.map([1], reenter)
    # A deadlock would have been the alternative; the error names the cause.
    assert isinstance(info.value.exception, RuntimeError)
    assert "another thread" in str(info.value.exception)


def test_sequential_batches_release_the_claim():
    with Pool(workers=2) as pool:
        assert pool.map([1], lambda x: x) == [1]
        assert sorted(pool.imap_unordered([2, 3], lambda x: x)) == [2, 3]
        assert pool.map([4], lambda x: x) == [4]


def test_uniterated_stream_does_not_claim_the_pool():
    with Pool(workers=2) as pool:
        pool.imap_unordered([1, 2], lambda x: x)  # created, never started
        assert pool.map([3], lambda x: x) == [3]


def test_close_during_a_suspended_stream_is_allowed_from_the_owner():
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered([1, 2, 3], lambda x: x)
        assert next(stream) in {1, 2, 3}
        pool.close()  # the claim is this thread's own; not a violation
        with pytest.raises(RuntimeError, match="closed"):
            next(stream)
