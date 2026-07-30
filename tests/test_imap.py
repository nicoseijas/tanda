"""Pool.imap(): streaming results in input order."""

import itertools
import threading
from collections.abc import Iterator

import pytest

from tanda import Pool, RetryPolicy, TaskError, TaskTimeout

WAIT = 5.0


def test_yields_all_results_in_input_order():
    with Pool(workers=4) as pool:
        results = list(pool.imap(range(50), lambda x: x * 2))
    assert results == [x * 2 for x in range(50)]


def test_returns_a_lazy_iterator_not_a_list():
    with Pool(workers=2) as pool:
        stream = pool.imap([1, 2], lambda x: x)
        assert isinstance(stream, Iterator)
        assert not isinstance(stream, list)
        list(stream)  # drain before close


def test_head_result_streams_before_later_items_complete():
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
        return item.upper()

    with Pool(workers=2) as pool:
        stream = pool.imap(["fast", "slow"], fn)
        # Index 0 is done; it must be yielded without waiting for index 1.
        assert next(stream) == "FAST"
        gate.set()
        assert next(stream) == "SLOW"
        with pytest.raises(StopIteration):
            next(stream)


def test_out_of_order_completion_is_reordered_to_input_order():
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
        return item.upper()

    with Pool(workers=2) as pool:
        stream = pool.imap(["slow", "fast"], fn)
        # "fast" completes first but is index 1: it must be held back.
        gate.set()
        assert list(stream) == ["SLOW", "FAST"]


def test_empty_iterable_yields_nothing():
    with Pool(workers=2) as pool:
        assert list(pool.imap([], lambda x: x)) == []


def test_infinite_input_stays_bounded():
    max_pending = 4
    last_pulled = [0]

    def items():
        for i in itertools.count():
            last_pulled[0] = i
            yield i

    with Pool(workers=2, max_pending=max_pending) as pool:
        stream = pool.imap(items(), lambda x: x)
        taken = [next(stream) for _ in range(5)]
        stream.close()

    assert taken == [0, 1, 2, 3, 4]
    # Same provable bound as the scheduler-level test: each next() refills
    # the window at most once.
    assert last_pulled[0] <= (5 + 1) * max_pending


def test_worker_exception_raises_task_error():
    def fn(item):
        raise ValueError(f"bad item {item}")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError, match="ValueError"):
            list(pool.imap([1], fn))


def test_failure_raises_fail_fast_not_at_its_input_position():
    """Error semantics match map(): the first definitive failure raises as
    soon as it is observed, even while an earlier index is still running."""
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
            return item
        raise ValueError("later item failed first")

    with Pool(workers=2) as pool:
        stream = pool.imap(["slow", "bad"], fn)
        try:
            with pytest.raises(TaskError) as excinfo:
                next(stream)
            assert excinfo.value.index == 1
        finally:
            gate.set()  # let the abandoned "slow" worker finish for close()


def test_task_error_carries_item_index_and_cause():
    def fn(item):
        raise ValueError("boom")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            list(pool.imap(["only"], fn))
    err = excinfo.value
    assert err.item == "only"
    assert err.index == 0
    assert isinstance(err.__cause__, ValueError)


def test_retry_recovers_before_yielding():
    lock = threading.Lock()
    calls = {}

    def flaky(item):
        with lock:
            calls[item] = calls.get(item, 0) + 1
            if calls[item] == 1:
                raise OSError("transient")
        return item * 10

    with Pool(workers=2) as pool:
        results = list(
            pool.imap(
                [1, 2, 3],
                flaky,
                retry=RetryPolicy(max_attempts=2, retry_on=(OSError,)),
            )
        )
    assert results == [10, 20, 30]


def test_task_timeout_raises_task_timeout():
    release = threading.Event()

    def stuck(item):
        assert release.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        try:
            with pytest.raises(TaskTimeout):
                list(pool.imap([1], stuck, task_timeout=0.1))
        finally:
            release.set()  # unblock the abandoned execution for close()


def test_exception_from_input_iterable_propagates():
    def bad_items():
        yield 1
        raise RuntimeError("iterator broke")

    with Pool(workers=2) as pool:
        with pytest.raises(RuntimeError, match="iterator broke"):
            list(pool.imap(bad_items(), lambda x: x))


def test_closed_pool_raises_at_call_time_not_first_next():
    pool = Pool(workers=2)
    pool.close()
    # The misuse must surface where it happens, not later at iteration.
    with pytest.raises(RuntimeError, match="closed"):
        pool.imap([1], lambda x: x)


def test_close_between_call_and_first_next_raises_cleanly():
    with Pool(workers=2) as pool:
        stream = pool.imap([1, 2], lambda x: x)
        pool.close()
        # Must be tanda's clear error, not the stdlib's raw
        # "cannot schedule new futures after shutdown".
        with pytest.raises(RuntimeError, match="closed"):
            next(stream)


def test_resuming_a_stream_after_close_raises_instead_of_hanging():
    with Pool(workers=2) as pool:
        stream = pool.imap([1, 2, 3], lambda x: x)
        assert next(stream) == 1
        pool.close()
        # Without the pre-resumption check this could re-enter the
        # scheduler's wait() over drained-and-cancelled futures — forever.
        with pytest.raises(RuntimeError, match="closed"):
            next(stream)


def test_pool_is_reusable_after_streaming():
    with Pool(workers=2) as pool:
        assert list(pool.imap([1, 2], lambda x: x + 1)) == [2, 3]
        assert pool.map([3, 4], lambda x: x + 1) == [4, 5]


def test_another_thread_cannot_start_a_batch_mid_stream():
    with Pool(workers=2) as pool:
        stream = pool.imap([1, 2, 3], lambda x: x)
        assert next(stream) == 1  # the stream now owns the pool

        outcome = {}

        def intruder():
            try:
                pool.map([9], lambda x: x)
            except RuntimeError as exc:
                outcome["error"] = exc

        thread = threading.Thread(target=intruder)
        thread.start()
        thread.join(WAIT)
        assert not thread.is_alive()
        assert "another thread" in str(outcome["error"])
        list(stream)  # drain before close
