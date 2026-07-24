"""RetryPolicy: attempt counting, selective retry, backoff, cancellation."""

import threading
import time

import pytest

from tanda import Pool, RetryPolicy, TaskError

WAIT = 5.0


# --- policy object: validation and pure logic --------------------------------


def test_defaults():
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.retry_on == (Exception,)
    assert policy.backoff == 0.0
    assert policy.backoff_strategy == "fixed"
    assert policy.jitter is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"backoff": -1.0},
        {"backoff_strategy": "cubic"},
        {"retry_on": ()},
        {"retry_on": (KeyboardInterrupt,)},  # BaseException: never retryable
        {"retry_on": (int,)},
    ],
)
def test_invalid_policy_raises(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_should_retry_respects_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    exc = ValueError()
    assert policy.should_retry(exc, attempts=1)
    assert policy.should_retry(exc, attempts=2)
    assert not policy.should_retry(exc, attempts=3)


def test_should_retry_is_selective_and_matches_subclasses():
    policy = RetryPolicy(retry_on=(OSError,))
    assert policy.should_retry(ConnectionError(), attempts=1)  # subclass
    assert not policy.should_retry(ValueError(), attempts=1)


def test_delay_fixed():
    policy = RetryPolicy(backoff=1.5, backoff_strategy="fixed")
    assert policy.delay_for(1) == 1.5
    assert policy.delay_for(4) == 1.5


def test_delay_exponential_doubles_per_attempt():
    policy = RetryPolicy(max_attempts=4, backoff=1.0, backoff_strategy="exponential")
    assert [policy.delay_for(n) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]


def test_delay_zero_backoff_is_always_zero():
    policy = RetryPolicy(backoff=0.0, backoff_strategy="exponential", jitter=True)
    assert policy.delay_for(3) == 0.0


def test_delay_jitter_stays_within_full_jitter_bounds():
    policy = RetryPolicy(backoff=2.0, jitter=True)
    for _ in range(100):
        assert 0.0 <= policy.delay_for(1) <= 2.0


# --- integration: map/collect ------------------------------------------------


class Flaky:
    """Fails the first ``failures`` calls per item, then succeeds."""

    def __init__(self, failures, exc_type=ValueError):
        self.failures = failures
        self.exc_type = exc_type
        self.calls: dict[object, int] = {}
        self.lock = threading.Lock()

    def __call__(self, item):
        with self.lock:
            self.calls[item] = self.calls.get(item, 0) + 1
            count = self.calls[item]
        if count <= self.failures:
            raise self.exc_type(f"attempt {count} for {item!r}")
        return item


def test_fails_once_then_succeeds():
    fn = Flaky(failures=1)
    with Pool(workers=2) as pool:
        batch = pool.map(
            [1, 2],
            fn,
            retry=RetryPolicy(max_attempts=3),
            error_policy="collect",
        )
    assert [r.value for r in batch.successful] == [1, 2]
    assert all(r.attempts == 2 for r in batch.successful)
    assert fn.calls == {1: 2, 2: 2}


def test_always_failing_item_reports_exact_attempts():
    fn = Flaky(failures=10)
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map([7], fn, retry=RetryPolicy(max_attempts=3))
    assert excinfo.value.attempts == 3
    assert fn.calls == {7: 3}


def test_non_listed_exception_fails_immediately():
    fn = Flaky(failures=10, exc_type=ValueError)
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map([1], fn, retry=RetryPolicy(max_attempts=5, retry_on=(OSError,)))
    assert excinfo.value.attempts == 1
    assert fn.calls == {1: 1}


def test_without_policy_no_retry_happens():
    fn = Flaky(failures=1)
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map([1], fn)
    assert excinfo.value.attempts == 1


def test_retries_flow_through_bounded_window():
    # 20 items each failing once, small window: every retry must re-enter the
    # same bounded submission path and still complete.
    fn = Flaky(failures=1)
    with Pool(workers=2, max_pending=3) as pool:
        batch = pool.map(
            range(20),
            fn,
            retry=RetryPolicy(max_attempts=2),
            error_policy="collect",
        )
    assert len(batch.successful) == 20
    assert all(r.attempts == 2 for r in batch.successful)
    assert batch.failed == ()


def test_imap_unordered_supports_retry():
    fn = Flaky(failures=2)
    with Pool(workers=2) as pool:
        results = list(
            pool.imap_unordered([5], fn, retry=RetryPolicy(max_attempts=3))
        )
    assert results == [5]
    assert fn.calls == {5: 3}


# --- backoff timing ----------------------------------------------------------


def test_fixed_backoff_delays_reexecution():
    stamps: list[float] = []
    lock = threading.Lock()

    def fn(item):
        with lock:
            stamps.append(time.perf_counter())
        if len(stamps) < 3:
            raise ValueError("flaky")
        return item

    with Pool(workers=2) as pool:
        pool.map([1], fn, retry=RetryPolicy(max_attempts=3, backoff=0.05))

    assert len(stamps) == 3
    # Lower-bound check only: the enforced delay must have elapsed between
    # consecutive executions (never an upper bound — that would be flaky).
    assert stamps[1] - stamps[0] >= 0.04
    assert stamps[2] - stamps[1] >= 0.04


def test_max_attempts_one_never_retries():
    # The smallest valid policy must behave exactly like no policy at all —
    # the boundary where a < vs <= off-by-one in should_retry would hide.
    fn = Flaky(failures=1)
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError) as excinfo:
            pool.map([1], fn, retry=RetryPolicy(max_attempts=1))
    assert excinfo.value.attempts == 1
    assert fn.calls == {1: 1}


def test_collect_mode_respects_selective_retry_on():
    # A retry_on-mismatched exception must fail immediately even in collect
    # mode, alongside items that do retry and succeed.
    fn = Flaky(failures=1, exc_type=ValueError)

    def dispatch(item):
        if item == "no-retry":
            raise TypeError("not retryable")
        return fn(item)

    with Pool(workers=2) as pool:
        batch = pool.map(
            ["retries", "no-retry"],
            dispatch,
            retry=RetryPolicy(max_attempts=3, retry_on=(ValueError,)),
            error_policy="collect",
        )
    [ok] = batch.successful
    [bad] = batch.failed
    assert (ok.item, ok.attempts) == ("retries", 2)
    assert (bad.item, bad.attempts) == ("no-retry", 1)
    assert isinstance(bad.exception, TypeError)


def test_pure_backoff_phase_sleeps_instead_of_spinning():
    # With a single item in backoff and nothing in flight, the coordinator
    # must sleep until the due time. A busy loop (wait() over an empty set
    # returns instantly) would burn ~backoff seconds of CPU; sleeping burns
    # almost none. process_time() counts only this process's CPU, so
    # unrelated system load cannot fail this.
    fn = Flaky(failures=1)
    cpu_before = time.process_time()
    with Pool(workers=2) as pool:
        result = pool.map([1], fn, retry=RetryPolicy(max_attempts=2, backoff=0.3))
    cpu_used = time.process_time() - cpu_before

    assert result == [1]
    assert cpu_used < 0.15


def test_closing_stream_during_backoff_does_not_wait_it_out():
    # Item "a" enters a 30s backoff; item "b" succeeds. Consuming "b" and
    # closing must return promptly and never re-execute "a".
    fn = Flaky(failures=10)

    def dispatch(item):
        if item == "a":
            return fn(item)
        return item

    policy = RetryPolicy(max_attempts=5, backoff=30.0)
    start = time.perf_counter()
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered(["a", "b"], dispatch, retry=policy)
        assert next(stream) == "b"
        stream.close()
    elapsed = time.perf_counter() - start

    assert fn.calls == {"a": 1}
    assert elapsed < WAIT  # nowhere near the 30s backoff
