"""Scenario 3: retrying a 10% transient failure rate.

The baseline is what the retry loop looks like when it is hand-rolled inside
the task function — the thing tanda replaces. Note what the two are *not*
doing identically: a hand-rolled backoff sleeps inside the worker, holding a
pool thread hostage for the whole wait, while tanda parks the item in the
coordinator and frees the worker. The zero-backoff row compares the raw
mechanism; the backoff row shows where that difference starts to pay.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from tanda import Pool, RetryPolicy

from _harness import Timing, print_timings, time_it

ITEMS = 2_000
WORKERS = 16
FAILURE_RATE = 10  # every Nth item fails its first execution
BACKOFF = 0.01
MAX_ATTEMPTS = 3

_attempts: dict[int, int] = {}
_lock = threading.Lock()


def _reset() -> None:
    _attempts.clear()


def _flaky(item: int) -> int:
    """Fails once per marked item, then succeeds — a transient error."""
    with _lock:
        _attempts[item] = _attempts.get(item, 0) + 1
        count = _attempts[item]
    if item % FAILURE_RATE == 0 and count == 1:
        raise OSError("transient")
    return item


def _hand_rolled(backoff: float):
    def fn(item: int) -> int:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return _flaky(item)
            except OSError:
                if attempt == MAX_ATTEMPTS:
                    raise
                if backoff:
                    time.sleep(backoff)  # blocks a worker thread
        raise AssertionError("unreachable")

    return fn


def _stdlib(backoff: float):
    def run() -> None:
        _reset()
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            list(executor.map(_hand_rolled(backoff), range(ITEMS)))

    return run


def _tanda(backoff: float):
    policy = RetryPolicy(
        max_attempts=MAX_ATTEMPTS, retry_on=(OSError,), backoff=backoff
    )

    def run() -> None:
        _reset()
        with Pool(workers=WORKERS) as pool:
            pool.map(range(ITEMS), _flaky, retry=policy)

    return run


def run() -> list[Timing]:
    no_backoff = [
        time_it("hand-rolled retry loop", _stdlib(0.0), repeats=3),
        time_it("tanda RetryPolicy", _tanda(0.0), repeats=3),
    ]
    print_timings(
        f"Retries, no backoff ({ITEMS:,} items, {100 / FAILURE_RATE:.0f}% "
        f"transient failures, {WORKERS} workers)",
        no_backoff,
    )
    with_backoff = [
        time_it(
            "hand-rolled, sleep in worker", _stdlib(BACKOFF), repeats=3
        ),
        time_it("tanda, backoff in coordinator", _tanda(BACKOFF), repeats=3),
    ]
    print_timings(
        f"Retries, {BACKOFF * 1000:.0f} ms backoff (same workload)",
        with_backoff,
        note="the hand-rolled version sleeps on a pool thread; tanda does not",
    )
    return no_backoff + with_backoff


if __name__ == "__main__":
    run()
