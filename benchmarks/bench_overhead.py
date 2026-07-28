"""Scenario 1: what the abstraction costs on no-op tasks.

This is the benchmark tanda is expected to lose. With nothing to wait for,
every microsecond of coordination is pure overhead, and the coordinator does
strictly more per item than ``ThreadPoolExecutor.map``: a WorkItem with a
lock, a state machine, a completion wait, an ordered collection step. The
number matters anyway — it is the price of the lifecycle, and it is what the
performance budget is frozen against.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from tanda import Pool

from _harness import Timing, print_timings, time_it

ITEMS = 50_000
WORKERS = 8


def _noop(item: int) -> int:
    return item


def _stdlib() -> None:
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        list(executor.map(_noop, range(ITEMS)))


def _tanda() -> None:
    with Pool(workers=WORKERS) as pool:
        pool.map(range(ITEMS), _noop)


def _tanda_streaming() -> None:
    with Pool(workers=WORKERS) as pool:
        for _ in pool.imap_unordered(range(ITEMS), _noop):
            pass


def run() -> list[Timing]:
    timings = [
        time_it("ThreadPoolExecutor.map", _stdlib),
        time_it("tanda Pool.map", _tanda),
        time_it("tanda imap_unordered", _tanda_streaming),
    ]
    print_timings(
        f"No-op tasks ({ITEMS:,} items, {WORKERS} workers)",
        timings,
        note="pure coordination cost: nothing here is I/O-bound",
    )
    return timings


if __name__ == "__main__":
    run()
