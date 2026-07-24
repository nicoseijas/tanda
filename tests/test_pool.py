"""Pool.map(): ordered results, plain lists, owned executor, clean lifecycle."""

import threading

import pytest

from tanda import Pool
from tanda._scheduler import default_max_pending

WAIT = 5.0


# --- construction and defaults ---------------------------------------------


def test_zero_config_pool_works():
    with Pool() as pool:
        assert pool.map(range(5), lambda x: x + 1) == [1, 2, 3, 4, 5]


def test_explicit_workers_sets_default_max_pending():
    with Pool(workers=4) as pool:
        assert pool.workers == 4
        assert pool.max_pending == default_max_pending(4)


def test_explicit_max_pending_wins():
    with Pool(workers=4, max_pending=7) as pool:
        assert pool.max_pending == 7


@pytest.mark.parametrize("kwargs", [{"workers": 0}, {"max_pending": 0}, {"workers": -2}])
def test_invalid_configuration_raises(kwargs):
    with pytest.raises(ValueError):
        Pool(**kwargs)


# --- map: ordering and shape -----------------------------------------------


def test_map_returns_plain_list_in_input_order():
    with Pool(workers=4) as pool:
        result = pool.map(range(100), lambda x: x * 2)
    assert isinstance(result, list)
    assert result == [x * 2 for x in range(100)]


def test_map_preserves_order_when_completion_order_is_reversed():
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
        else:
            gate.set()
        return item.upper()

    with Pool(workers=2) as pool:
        # "fast" opens the gate, so it always finishes first internally;
        # map must still return input order.
        assert pool.map(["slow", "fast"], fn) == ["SLOW", "FAST"]


def test_map_empty_iterable():
    with Pool() as pool:
        assert pool.map([], lambda x: x) == []


def test_map_accepts_generator_input():
    with Pool(workers=2) as pool:
        items = (i for i in range(20))
        assert pool.map(items, lambda x: -x) == [-i for i in range(20)]


def test_map_preserves_none_results():
    with Pool(workers=2) as pool:
        assert pool.map([1, 2, 3], lambda x: None) == [None, None, None]


# --- map: errors ------------------------------------------------------------


def test_worker_exception_raises_task_error_with_cause():
    from tanda import TaskError

    def fn(item):
        raise ValueError(f"bad item {item}")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map([1], fn)
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_map_raises_on_first_failure_without_draining():
    # Cancellation of pending futures on close is proven deterministically at
    # the scheduler level; here we verify map() wires it: the first definitive
    # failure surfaces immediately instead of waiting for blocked work.
    import time

    gate = threading.Event()

    def fn(item):
        if item == 0:
            raise ValueError("boom")
        assert gate.wait(WAIT)
        return item

    from tanda import TaskError

    with Pool(workers=2, max_pending=2) as pool:
        start = time.perf_counter()
        with pytest.raises(TaskError):
            pool.map([1, 0], fn)  # item 1 pins a worker; item 0 fails fast
        elapsed = time.perf_counter() - start
        gate.set()

    # A drain-everything implementation could only surface the error after
    # fn(1)'s WAIT-long gate timeout; fail-fast returns orders of magnitude
    # sooner. The bound is an upper limit, not a sleep.
    assert elapsed < WAIT


# --- lifecycle ---------------------------------------------------------------


def test_map_after_close_raises():
    pool = Pool(workers=2)
    pool.close()
    with pytest.raises(RuntimeError, match="closed"):
        pool.map([1], lambda x: x)


def test_map_after_context_exit_raises():
    with Pool(workers=2) as pool:
        pass
    with pytest.raises(RuntimeError, match="closed"):
        pool.map([1], lambda x: x)


def test_close_is_idempotent():
    pool = Pool(workers=2)
    pool.close()
    pool.close()


def test_explicit_close_without_context_manager():
    pool = Pool(workers=2)
    try:
        assert pool.map([1, 2], lambda x: x) == [1, 2]
    finally:
        pool.close()


def test_pool_is_reusable_across_map_calls():
    with Pool(workers=2) as pool:
        assert pool.map([1, 2], lambda x: x + 1) == [2, 3]
        assert pool.map([3, 4], lambda x: x + 1) == [4, 5]


def test_pool_is_reusable_after_a_failed_map():
    from tanda import TaskError

    def failing(item):
        raise ValueError("boom")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError):
            pool.map([1, 2], failing)
        assert pool.map([1, 2], lambda x: x + 1) == [2, 3]


def test_map_propagates_exception_from_input_iterable():
    # Distinct path from a worker failure: no WorkItem is involved at all.
    def bad_items():
        yield 1
        raise RuntimeError("iterator broke")

    with Pool(workers=2) as pool:
        with pytest.raises(RuntimeError, match="iterator broke"):
            pool.map(bad_items(), lambda x: x)
