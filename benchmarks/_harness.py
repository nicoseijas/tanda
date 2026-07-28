"""Measurement helpers shared by the benchmark scenarios.

Two rules, both from CONTRIBUTING.md: every scenario measures tanda *and* a
raw ``ThreadPoolExecutor`` baseline doing the same work, and every result is
printed whether tanda wins or loses.
"""

from __future__ import annotations

import platform
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Timing:
    """Wall-clock samples for one implementation of one scenario."""

    label: str
    samples: tuple[float, ...]

    @property
    def best(self) -> float:
        return min(self.samples)

    @property
    def median(self) -> float:
        return statistics.median(self.samples)


@dataclass(frozen=True)
class Memory:
    """Peak Python-heap allocation for one implementation of one scenario."""

    label: str
    peak_bytes: int


def time_it(label: str, run: Callable[[], object], repeats: int = 5) -> Timing:
    """Run ``run`` ``repeats`` times, discarding one warm-up run.

    Thread pools pay a one-off cost for spawning their threads and for the
    first import of everything on the path; charging it to the first sample
    only would make the comparison depend on which implementation ran first.
    """
    run()
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        run()
        samples.append(time.perf_counter() - start)
    return Timing(label, tuple(samples))


def measure_peak_memory(label: str, run: Callable[[], object]) -> Memory:
    """Peak Python allocation during ``run``, via tracemalloc.

    This measures what the process allocated on the Python heap, not RSS:
    it is the number that isolates "how many objects did the strategy keep
    alive at once" from the interpreter's own allocator behaviour.
    """
    tracemalloc.start()
    try:
        run()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return Memory(label, peak)


def print_environment() -> None:
    print(
        f"python {platform.python_version()} | {platform.system()} "
        f"{platform.machine()} | {sys.implementation.name}"
    )


def print_timings(title: str, timings: Iterable[Timing], note: str = "") -> None:
    timings = list(timings)
    baseline = timings[0]
    print(f"\n{title}")
    if note:
        print(f"  {note}")
    width = max(len(t.label) for t in timings)
    for timing in timings:
        delta = ""
        if timing is not baseline:
            change = (timing.best / baseline.best - 1) * 100
            delta = f"   ({change:+.0f}% vs {baseline.label})"
        print(
            f"  {timing.label:<{width}}  {timing.best * 1000:8.1f} ms"
            f"  (median {timing.median * 1000:.1f} ms){delta}"
        )


def print_memory(title: str, measurements: Iterable[Memory]) -> None:
    measurements = list(measurements)
    baseline = measurements[0]
    print(f"\n{title}")
    width = max(len(m.label) for m in measurements)
    for measurement in measurements:
        delta = ""
        if measurement is not baseline:
            ratio = baseline.peak_bytes / max(measurement.peak_bytes, 1)
            delta = f"   ({ratio:.0f}x less than {baseline.label})"
        print(
            f"  {measurement.label:<{width}}  "
            f"peak {measurement.peak_bytes / 1e6:8.1f} MB{delta}"
        )
