"""Scenario 2: the workload tanda actually exists for.

1000 tasks with 5-20 ms of simulated latency. Per-item coordination overhead
is measured in microseconds, so it should disappear against milliseconds of
waiting: the two implementations are expected to land within noise of each
other. Latencies come from a seeded RNG so both implementations run the exact
same sequence.
"""

from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor

from tanda import Pool

from _harness import Timing, print_timings, time_it

ITEMS = 1_000
WORKERS = 32
SEED = 20260728

_LATENCIES = tuple(
    random.Random(SEED).uniform(0.005, 0.020) for _ in range(ITEMS)
)


def _fake_io(index: int) -> int:
    time.sleep(_LATENCIES[index])
    return index


def _stdlib() -> None:
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        list(executor.map(_fake_io, range(ITEMS)))


def _tanda() -> None:
    with Pool(workers=WORKERS) as pool:
        pool.map(range(ITEMS), _fake_io)


def run() -> list[Timing]:
    timings = [
        time_it("ThreadPoolExecutor.map", _stdlib, repeats=3),
        time_it("tanda Pool.map", _tanda, repeats=3),
    ]
    print_timings(
        f"Simulated I/O ({ITEMS:,} tasks, 5-20 ms each, {WORKERS} workers)",
        timings,
        note="ideal time is bounded by sum(latencies) / workers",
    )
    return timings


if __name__ == "__main__":
    run()
