"""BoundedScheduler: bounded submission window, lazy input, future lifecycle.

All synchronization is explicit (Event / Semaphore / locks) — no
sleep-and-assert, per CONTRIBUTING.md.
"""

import itertools
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from tanda._scheduler import BoundedScheduler, default_max_pending
from tanda._states import TaskState

WAIT = 5.0  # generous upper bound for any single synchronization point


@pytest.fixture
def executor():
    with ThreadPoolExecutor(max_workers=2) as ex:
        yield ex


# --- construction ----------------------------------------------------------


def test_default_max_pending_is_workers_times_four():
    assert default_max_pending(8) == 32


@pytest.mark.parametrize("bad", [0, -1])
def test_max_pending_must_be_positive(executor, bad):
    with pytest.raises(ValueError):
        BoundedScheduler(executor, max_pending=bad)


def test_requires_a_thread_pool_executor():
    # A process pool would run against a pickled copy of the WorkItem and
    # silently lose results; the scheduler must refuse anything but threads.
    class NotAThreadPool:
        def submit(self, fn, *args):  # pragma: no cover - never reached
            raise AssertionError

    with pytest.raises(TypeError, match="ThreadPoolExecutor"):
        BoundedScheduler(NotAThreadPool(), max_pending=4)


# --- basics ----------------------------------------------------------------


def test_empty_iterable_yields_nothing(executor):
    scheduler = BoundedScheduler(executor, max_pending=4)
    assert list(scheduler.run([], lambda x: x)) == []


def test_single_item(executor):
    scheduler = BoundedScheduler(executor, max_pending=4)
    [work] = list(scheduler.run([21], lambda x: x * 2))
    assert (work.index, work.item, work.value) == (0, 21, 42)
    assert work.state is TaskState.SUCCESS
    assert work.exception is None


def test_all_items_complete_with_correct_values(executor):
    scheduler = BoundedScheduler(executor, max_pending=4)
    completed = list(scheduler.run(range(50), lambda x: x * 2))
    assert len(completed) == 50
    assert {(w.index, w.item, w.value) for w in completed} == {
        (i, i, i * 2) for i in range(50)
    }
    assert all(w.state is TaskState.SUCCESS for w in completed)


def test_generator_input_is_accepted(executor):
    scheduler = BoundedScheduler(executor, max_pending=4)
    items = (i for i in range(10))
    values = sorted(w.value for w in scheduler.run(items, lambda x: x + 1))
    assert values == list(range(1, 11))


# --- completion order ------------------------------------------------------


def test_yields_in_completion_order_not_input_order(executor):
    gate = threading.Event()

    def fn(item):
        if item == "slow":
            assert gate.wait(WAIT)
        return item

    scheduler = BoundedScheduler(executor, max_pending=2)
    gen = scheduler.run(["slow", "fast"], fn)

    # "fast" is the only task that can complete while the gate is closed.
    first = next(gen)
    assert first.item == "fast"

    gate.set()
    second = next(gen)
    assert second.item == "slow"


# --- bounded window / laziness ---------------------------------------------


def test_window_invariant_pulled_never_exceeds_completed_plus_max_pending(executor):
    # "completed" is counted when fn starts finishing, slightly before the
    # coordinator observes the future as done — the bound is deliberately
    # generous in that direction, but still catches any over-pull bug.
    max_pending = 4
    lock = threading.Lock()
    counters = {"pulled": 0, "completed": 0}
    violations = []

    def items():
        for i in range(200):
            with lock:
                counters["pulled"] += 1
                overhang = counters["pulled"] - counters["completed"]
                if overhang > max_pending:
                    violations.append(overhang)
            yield i

    def fn(item):
        with lock:
            counters["completed"] += 1
        return item

    scheduler = BoundedScheduler(executor, max_pending=max_pending)
    completed = list(scheduler.run(items(), fn))

    assert violations == []
    assert len(completed) == 200
    assert counters["pulled"] == 200


def test_input_not_consumed_beyond_window_while_workers_blocked(executor):
    # workers=2 (fixture), max_pending=3: with every worker blocked, the
    # scheduler must stop pulling at exactly max_pending items.
    max_pending = 3
    pulled = []
    started = threading.Semaphore(0)
    release = threading.Event()

    def items():
        for i in range(10):
            pulled.append(i)
            yield i

    def fn(item):
        started.release()
        assert release.wait(WAIT)
        return item

    scheduler = BoundedScheduler(executor, max_pending=max_pending)
    results = []
    consumer = threading.Thread(
        target=lambda: results.extend(scheduler.run(items(), fn))
    )
    consumer.start()
    try:
        # Both workers are inside fn, a third task is queued: the window is
        # full and nothing has completed, so no further input may be pulled.
        assert started.acquire(timeout=WAIT)
        assert started.acquire(timeout=WAIT)
        assert len(pulled) <= max_pending
    finally:
        release.set()
        consumer.join(timeout=WAIT)

    assert not consumer.is_alive()
    assert len(results) == 10
    assert len(pulled) == 10


def test_infinite_input_stays_bounded_when_consumer_stops(executor):
    max_pending = 4
    pulled = itertools.count()
    last_pulled = [0]

    def items():
        for i in pulled:
            last_pulled[0] = i
            yield i

    scheduler = BoundedScheduler(executor, max_pending=max_pending)
    gen = scheduler.run(items(), lambda x: x)
    taken = [next(gen) for _ in range(5)]
    gen.close()

    assert len(taken) == 5
    # Each of the 5 next() calls refills the window at most once, so pulls
    # are bounded by (yields + 1) windows — minuscule against infinity.
    assert last_pulled[0] <= (5 + 1) * max_pending


# --- close cancels pending work --------------------------------------------


def test_closing_the_generator_cancels_pending_tasks():
    executed = set()
    started_blocker = threading.Semaphore(0)
    gate = threading.Event()

    def fn(item):
        executed.add(item)
        if item == 1:
            started_blocker.release()
            assert gate.wait(WAIT)
        return item

    with ThreadPoolExecutor(max_workers=1) as ex:
        scheduler = BoundedScheduler(ex, max_pending=3)
        gen = scheduler.run([0, 1, 2], fn)

        results = []
        consumer = threading.Thread(target=lambda: results.append(next(gen)))
        consumer.start()
        consumer.join(timeout=WAIT)
        assert not consumer.is_alive()
        assert results[0].item == 0

        # The single worker is now pinned inside fn(1); item 2 is queued and
        # can only ever run if close() fails to cancel it.
        assert started_blocker.acquire(timeout=WAIT)
        gen.close()
        gate.set()

    assert executed == {0, 1}


# --- errors ----------------------------------------------------------------


def test_worker_exception_is_captured_not_raised(executor):
    def fn(item):
        if item % 2:
            raise ValueError(f"odd: {item}")
        return item

    scheduler = BoundedScheduler(executor, max_pending=4)
    completed = {w.item: w for w in scheduler.run(range(6), fn)}

    assert len(completed) == 6
    for item, work in completed.items():
        if item % 2:
            assert work.state is TaskState.FAILED
            assert isinstance(work.exception, ValueError)
            assert work.value is None
        else:
            assert work.state is TaskState.SUCCESS
            assert work.value == item
            assert work.exception is None


def test_input_iterator_error_propagates(executor):
    def items():
        yield 1
        yield 2
        raise RuntimeError("broken input")

    scheduler = BoundedScheduler(executor, max_pending=8)
    with pytest.raises(RuntimeError, match="broken input"):
        list(scheduler.run(items(), lambda x: x))
