"""Concurrency stress, backpressure under load, races, and reentrancy.

The scale tests (10k items) run no-op functions on purpose: what is under test
is the coordinator's bookkeeping at volume, not the workload.

The race tests do not assert *who* wins — that is genuinely
nondeterministic — they assert the invariant that must hold whoever wins:
exactly one outcome per item, no hang, no lost or duplicated result. Every
test that pins a worker releases its gate in a ``finally``: an abandoned
worker keeps the (non-daemon) pool thread alive and would hang the test run
at interpreter exit.
"""

import threading

import pytest

from tanda import (
    Cancellation,
    Cancelled,
    Pool,
    RetryPolicy,
    TaskError,
    TaskTimeout,
)

WAIT = 5.0
BRIEF = 0.05
MANY = 10_000


class _CountingReporter:
    """Counts terminal items. ``advance`` runs on the coordinator thread, so
    reading it from the input generator (also the coordinator) is race-free."""

    def __init__(self) -> None:
        self.total = None
        self.advanced = 0
        self.finished = 0

    def start(self, total):
        self.total = total

    def advance(self, n=1):
        self.advanced += n

    def finish(self):
        self.finished += 1


# --- scale -------------------------------------------------------------------


@pytest.mark.parametrize("workers", [1, 100])
def test_map_over_10k_items_preserves_order(workers):
    with Pool(workers=workers) as pool:
        assert pool.map(range(MANY), lambda x: x * 2) == [
            x * 2 for x in range(MANY)
        ]


@pytest.mark.parametrize("workers", [1, 100])
def test_imap_unordered_over_10k_items_yields_each_item_exactly_once(workers):
    with Pool(workers=workers) as pool:
        seen = sorted(pool.imap_unordered(range(MANY), lambda x: x))
    assert seen == list(range(MANY))


def test_single_failure_among_10k_items_raises_without_hanging():
    def fn(item):
        if item == MANY // 2:
            raise ValueError("boom")
        return item

    with Pool(workers=100) as pool:
        with pytest.raises(TaskError) as info:
            pool.map(range(MANY), fn)
    assert info.value.index == MANY // 2


def test_collect_over_10k_items_separates_every_outcome():
    with Pool(workers=100) as pool:
        result = pool.map(
            range(MANY),
            lambda x: x if x % 2 else _raise(ValueError(x)),
            error_policy="collect",
        )
    assert len(result.successful) + len(result.failed) == MANY
    assert [f.index for f in result.failed] == list(range(0, MANY, 2))


def _raise(exc):
    raise exc


def test_retry_storm_reruns_every_item_exactly_once():
    attempts: dict[int, int] = {}
    lock = threading.Lock()

    def flaky(item):
        with lock:
            attempts[item] = attempts.get(item, 0) + 1
            count = attempts[item]
        if count == 1:
            raise OSError("transient")
        return item

    with Pool(workers=32) as pool:
        results = pool.map(
            range(1000),
            flaky,
            retry=RetryPolicy(max_attempts=3, retry_on=(OSError,)),
        )
    assert results == list(range(1000))
    assert set(attempts.values()) == {2}


# --- backpressure under load -------------------------------------------------


def test_in_flight_never_exceeds_max_pending():
    # The window bound is the whole point of bounded submission, so assert it
    # at every pull rather than sampling: the coordinator pulls input only
    # after the caller consumed (and the reporter counted) what it yielded,
    # so `consumed - advanced` is the exact in-flight count at that moment.
    reporter = _CountingReporter()
    max_pending = 8
    consumed = 0

    def producer():
        nonlocal consumed
        for item in range(500):
            consumed += 1
            assert consumed - reporter.advanced <= max_pending
            yield item

    with Pool(workers=4, max_pending=max_pending) as pool:
        assert pool.map(producer(), lambda x: x, progress=reporter) == list(
            range(500)
        )
    assert reporter.total is None  # a generator is never sized to count it
    assert reporter.advanced == 500


def test_slow_consumer_stops_the_producer():
    # A stream consumed one item at a time must not run ahead: after the
    # first result, at most one full window has been pulled from the input.
    max_pending = 4
    consumed = 0

    def producer():
        nonlocal consumed
        for item in range(1_000_000):
            consumed += 1
            yield item

    with Pool(workers=2, max_pending=max_pending) as pool:
        stream = pool.imap_unordered(producer(), lambda x: x)
        try:
            next(stream)
            assert consumed <= max_pending + 1
        finally:
            stream.close()


def test_slow_producer_does_not_starve_the_batch():
    # The input arrives in two bursts, the second released only after the
    # first burst is fully processed — an input the coordinator must wait on
    # without giving up on the batch.
    first_burst_done = threading.Event()
    processed = []
    lock = threading.Lock()

    def fn(item):
        with lock:
            processed.append(item)
            if len(processed) == 5:
                first_burst_done.set()
        return item

    def producer():
        yield from range(5)
        assert first_burst_done.wait(WAIT)
        yield from range(5, 10)

    with Pool(workers=4) as pool:
        assert pool.map(producer(), fn) == list(range(10))


# --- races -------------------------------------------------------------------


def test_completion_racing_shutdown_neither_hangs_nor_loses_the_worker():
    # The leaked worker returns at the exact moment close() starts waiting on
    # it. Whoever wins, close() must return without a ShutdownTimeout.
    gate = threading.Barrier(2, timeout=WAIT)
    pool = Pool(workers=1)
    try:
        result = pool.map(
            ["stuck"],
            lambda item: gate.wait(),
            task_timeout=BRIEF,
            error_policy="collect",
        )
        assert len(result.failed) == 1
        gate.wait()  # release the abandoned execution...
        pool.close(timeout=WAIT)  # ...while shutdown waits for it
    finally:
        gate.abort()


def test_exception_racing_cancellation_yields_exactly_one_outcome():
    # One task raises while another thread requests cancellation. Both are
    # legal endings; a hang, a mixed result, or a swallowed error are not.
    cancel = Cancellation()
    gate = threading.Barrier(2, timeout=WAIT)
    outcome = {}

    def fn(item):
        gate.wait()
        raise ValueError("boom")

    def consume(pool):
        try:
            pool.map([1], fn, cancel=cancel)
        except BaseException as exc:  # noqa: BLE001 - capturing for assertion
            outcome["error"] = exc

    with Pool(workers=2) as pool:
        consumer = threading.Thread(target=consume, args=(pool,))
        consumer.start()
        try:
            gate.wait()
            cancel.request()
        finally:
            gate.abort()
            consumer.join(WAIT)
        assert not consumer.is_alive()

    assert isinstance(outcome["error"], TaskError | Cancelled)


@pytest.mark.parametrize("attempt", range(25))
def test_timeout_racing_completion_finalizes_an_item_once(attempt):
    # The coordinator's timeout scan and the worker's completion compete for
    # the same compare-and-set. The winner varies; "exactly one terminal
    # outcome for one item" does not.
    with Pool(workers=2) as pool:
        result = pool.map(
            [attempt],
            lambda item: item,
            task_timeout=0.000_001,
            error_policy="collect",
        )
    assert len(result.successful) + len(result.failed) == 1
    for failure in result.failed:
        assert isinstance(failure.exception, TimeoutError)


def test_a_thousand_racing_timeouts_lose_nothing():
    with Pool(workers=16) as pool:
        result = pool.map(
            range(1000),
            lambda x: x,
            task_timeout=0.000_001,
            error_policy="collect",
        )
    indices = [r.index for r in result.successful] + [
        f.index for f in result.failed
    ]
    assert sorted(indices) == list(range(1000))


def test_timed_out_execution_still_reports_its_lateness_under_load():
    gate = threading.Event()
    pool = Pool(workers=8)
    try:
        with pytest.raises(TaskTimeout):
            pool.map(range(8), lambda item: gate.wait(WAIT), task_timeout=BRIEF)
    finally:
        gate.set()
        pool.close(timeout=WAIT)


# --- reentrancy --------------------------------------------------------------


class _ReentrantReporter:
    """A progress reporter that runs a nested batch from its callbacks.

    Reporter callbacks run on the coordinator thread, so this is same-thread
    nesting — legal by design (GUIDELINES.md: it is bounded by the same
    window and cannot deadlock). This is exactly where a lock held across a
    user callback would deadlock instead.
    """

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self.nested: list[int] = []

    def start(self, total):
        self.nested.extend(self.pool.map([0], lambda x: x))

    def advance(self, n=1):
        self.nested.extend(self.pool.map([n], lambda x: x))

    def finish(self):
        self.nested.extend(self.pool.map([-1], lambda x: x))


def test_reporter_calling_back_into_the_pool_does_not_deadlock():
    with Pool(workers=2, max_pending=2) as pool:
        reporter = _ReentrantReporter(pool)
        assert pool.map([1, 2, 3], lambda x: x, progress=reporter) == [1, 2, 3]
    assert reporter.nested == [0, 1, 1, 1, -1]


def test_nested_batch_inside_a_stream_is_allowed_on_the_owner_thread():
    with Pool(workers=4, max_pending=4) as pool:
        collected = []
        for value in pool.imap_unordered(range(20), lambda x: x):
            collected.append(pool.map([value], lambda x: x * 10)[0])
    assert sorted(collected) == [x * 10 for x in range(20)]


def test_reentrancy_from_a_worker_thread_still_fails_loudly_under_load():
    # The same-thread allowance must not leak into worker threads: they would
    # each get their own window and break the pool-wide bound.
    with Pool(workers=4) as pool:

        def reenter(item):
            if item == 7:
                return pool.map([item], lambda x: x)
            return item

        with pytest.raises(TaskError) as info:
            pool.map(range(50), reenter)
    assert isinstance(info.value.exception, RuntimeError)
    assert "another thread" in str(info.value.exception)
