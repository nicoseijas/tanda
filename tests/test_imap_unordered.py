"""Pool.imap_unordered(): streaming results in completion order."""

import itertools
import threading
from collections.abc import Iterator

import pytest

from tanda import Pool

WAIT = 5.0


def test_yields_all_results():
    with Pool(workers=4) as pool:
        results = list(pool.imap_unordered(range(50), lambda x: x * 2))
    assert sorted(results) == [x * 2 for x in range(50)]


def test_returns_a_lazy_iterator_not_a_list():
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered([1, 2], lambda x: x)
        assert isinstance(stream, Iterator)
        assert not isinstance(stream, list)
        list(stream)  # drain before close


def test_yields_in_completion_order():
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
        return item.upper()

    with Pool(workers=2) as pool:
        stream = pool.imap_unordered(["slow", "fast"], fn)
        # Only "fast" can complete while the gate is closed.
        assert next(stream) == "FAST"
        gate.set()
        assert next(stream) == "SLOW"
        with pytest.raises(StopIteration):
            next(stream)


def test_empty_iterable_yields_nothing():
    with Pool(workers=2) as pool:
        assert list(pool.imap_unordered([], lambda x: x)) == []


def test_infinite_input_stays_bounded():
    max_pending = 4
    last_pulled = [0]

    def items():
        for i in itertools.count():
            last_pulled[0] = i
            yield i

    with Pool(workers=2, max_pending=max_pending) as pool:
        stream = pool.imap_unordered(items(), lambda x: x)
        taken = [next(stream) for _ in range(5)]
        stream.close()

    assert len(taken) == 5
    # Same provable bound as the scheduler-level test: each next() refills
    # the window at most once.
    assert last_pulled[0] <= (5 + 1) * max_pending


def test_worker_exception_raises_task_error():
    from tanda import TaskError

    def fn(item):
        raise ValueError(f"bad item {item}")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError, match="ValueError"):
            list(pool.imap_unordered([1], fn))


def test_exception_from_input_iterable_propagates():
    def bad_items():
        yield 1
        raise RuntimeError("iterator broke")

    with Pool(workers=2) as pool:
        with pytest.raises(RuntimeError, match="iterator broke"):
            list(pool.imap_unordered(bad_items(), lambda x: x))


def test_closed_pool_raises_at_call_time_not_first_next():
    pool = Pool(workers=2)
    pool.close()
    # The misuse must surface where it happens, not later at iteration.
    with pytest.raises(RuntimeError, match="closed"):
        pool.imap_unordered([1], lambda x: x)


def test_close_between_call_and_first_next_raises_cleanly():
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered([1, 2], lambda x: x)
        pool.close()
        # Must be tanda's clear error, not the stdlib's raw
        # "cannot schedule new futures after shutdown".
        with pytest.raises(RuntimeError, match="closed"):
            next(stream)


def test_resuming_a_stream_after_close_raises_instead_of_hanging():
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered([1, 2, 3], lambda x: x)
        assert next(stream) in {1, 2, 3}
        pool.close()
        # Without the pre-resumption check this could re-enter the
        # scheduler's wait() over drained-and-cancelled futures — forever.
        with pytest.raises(RuntimeError, match="closed"):
            next(stream)


def test_pool_is_reusable_after_streaming():
    with Pool(workers=2) as pool:
        assert sorted(pool.imap_unordered([1, 2], lambda x: x + 1)) == [2, 3]
        assert pool.map([3, 4], lambda x: x + 1) == [4, 5]
