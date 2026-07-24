"""TaskError, error_policy="raise"/"collect", BatchResult."""

import pytest

from tanda import BatchResult, Pool, TaskError, TaskFailure, TaskResult

WAIT = 5.0


def _fail_odd(item):
    if item % 2:
        raise ValueError(f"odd: {item}")
    return item * 10


# --- error_policy="raise" (default) ----------------------------------------


def test_map_wraps_worker_failure_in_task_error():
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map(["a"], lambda item: 1 / 0)

    err = excinfo.value
    assert err.item == "a"
    assert err.index == 0
    assert isinstance(err.exception, ZeroDivisionError)
    assert err.attempts == 1
    assert err.elapsed >= 0.0
    # The original exception is chained for full tracebacks.
    assert err.__cause__ is err.exception


def test_task_error_message_names_index_and_cause():
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError, match="ZeroDivisionError"):
            pool.map([1], lambda item: 1 / 0)


def test_imap_unordered_also_raises_task_error():
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            list(pool.imap_unordered([7], _fail_odd))
    assert excinfo.value.item == 7
    assert isinstance(excinfo.value.exception, ValueError)


def test_explicit_raise_policy_matches_default():
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError):
            pool.map([1], _fail_odd, error_policy="raise")


def test_input_iterable_errors_are_not_wrapped():
    # Only item failures become TaskError; a broken input iterable is the
    # caller's bug and propagates untouched.
    def bad_items():
        yield 0
        raise RuntimeError("iterator broke")

    with Pool(workers=2) as pool:
        with pytest.raises(RuntimeError, match="iterator broke"):
            pool.map(bad_items(), _fail_odd)


def test_invalid_error_policy_raises_at_call_time():
    with Pool(workers=2) as pool:
        with pytest.raises(ValueError, match="error_policy"):
            pool.map([1], _fail_odd, error_policy="ignore")


# --- error_policy="collect" --------------------------------------------------


def test_collect_runs_everything_and_separates_outcomes():
    with Pool(workers=4) as pool:
        batch = pool.map(range(10), _fail_odd, error_policy="collect")

    assert isinstance(batch, BatchResult)
    assert [r.index for r in batch.successful] == [0, 2, 4, 6, 8]
    assert [r.value for r in batch.successful] == [0, 20, 40, 60, 80]
    assert [f.index for f in batch.failed] == [1, 3, 5, 7, 9]
    assert all(isinstance(f.exception, ValueError) for f in batch.failed)
    assert all(r.attempts == 1 for r in batch.successful)
    assert all(f.elapsed >= 0.0 for f in batch.failed)


def test_collect_with_no_failures():
    with Pool(workers=2) as pool:
        batch = pool.map([2, 4], _fail_odd, error_policy="collect")
    assert [r.value for r in batch.successful] == [20, 40]
    assert batch.failed == ()


def test_collect_with_no_successes():
    with Pool(workers=2) as pool:
        batch = pool.map([1, 3], _fail_odd, error_policy="collect")
    assert batch.successful == ()
    assert [f.item for f in batch.failed] == [1, 3]


def test_collect_lists_are_ordered_by_input_index():
    import threading

    gate = threading.Event()

    def fn(item):
        if item == 0:
            assert gate.wait(WAIT)  # forces item 0 to complete last
        else:
            gate.set()
        return item

    with Pool(workers=2) as pool:
        batch = pool.map([0, 1], fn, error_policy="collect")
    assert [r.index for r in batch.successful] == [0, 1]


def test_collect_metadata_carries_the_item():
    with Pool(workers=2) as pool:
        batch = pool.map(["x", "y"], lambda s: s.upper(), error_policy="collect")
    assert [(r.item, r.value) for r in batch.successful] == [("x", "X"), ("y", "Y")]


def test_keyboard_interrupt_from_worker_keeps_its_category():
    # Wrapping it in TaskError (an Exception) would make `except
    # KeyboardInterrupt` around map() stop matching — Ctrl+C contract.
    def fn(item):
        raise KeyboardInterrupt

    with Pool(workers=2) as pool:
        with pytest.raises(KeyboardInterrupt):
            pool.map([1], fn)


def test_keyboard_interrupt_propagates_in_collect_mode_too():
    def fn(item):
        raise KeyboardInterrupt

    with Pool(workers=2) as pool:
        with pytest.raises(KeyboardInterrupt):
            pool.map([1], fn, error_policy="collect")


def test_collect_does_not_cancel_pending_after_a_failure():
    import threading

    gate = threading.Event()

    def fn(item):
        if item == 0:
            raise ValueError("first failure")
        if item == 1:
            assert gate.wait(WAIT)  # pinned until item 2 runs
            return item
        gate.set()  # item 2 only runs if the failure did NOT cancel it
        return item

    # workers=2: item 0 fails immediately, item 1 pins a worker; a fail-fast
    # regression would cancel pending item 2 and the gate would never open.
    with Pool(workers=2, max_pending=3) as pool:
        batch = pool.map([0, 1, 2], fn, error_policy="collect")

    assert [f.index for f in batch.failed] == [0]
    assert [r.index for r in batch.successful] == [1, 2]


def test_batch_result_fields_are_immutable_tuples():
    with Pool(workers=2) as pool:
        batch = pool.map(range(4), _fail_odd, error_policy="collect")
    assert isinstance(batch.successful, tuple)
    assert isinstance(batch.failed, tuple)
    with pytest.raises(AttributeError):
        batch.successful = ()


def test_result_types_are_immutable():
    result = TaskResult(item=1, index=0, value=2, attempts=1, elapsed=0.0)
    failure = TaskFailure(
        item=1, index=0, exception=ValueError(), attempts=1, elapsed=0.0
    )
    with pytest.raises(AttributeError):
        result.value = 99
    with pytest.raises(AttributeError):
        failure.attempts = 2
