"""Scenario 4: a large input, submit-all versus bounded submission.

This is the benchmark that justifies the whole bounded-window design. Both
sides stream their results and discard them, so what is being compared is the
number of live Future objects, not the results: submit-all keeps one per
input item, tanda keeps at most ``max_pending``.

Peak allocation is measured with tracemalloc, which tracks the Python heap.
It also slows both sides down considerably — this scenario measures memory,
never time.
"""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from tanda import Pool

from _harness import Memory, measure_peak_memory, print_memory

ITEMS = 1_000_000
WORKERS = 8


def _noop(item: int) -> int:
    return item


def _submit_all(items: int):
    def run() -> None:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(_noop, item) for item in range(items)]
            for future in as_completed(futures):
                future.result()

    return run


def _tanda_bounded(items: int):
    def run() -> None:
        with Pool(workers=WORKERS) as pool:
            for _ in pool.imap_unordered(range(items), _noop):
                pass

    return run


def run(items: int = ITEMS) -> list[Memory]:
    measurements = [
        measure_peak_memory("submit-all", _submit_all(items)),
        measure_peak_memory("tanda bounded", _tanda_bounded(items)),
    ]
    print_memory(f"Peak memory over {items:,} input items", measurements)
    return measurements


if __name__ == "__main__":
    run(int(sys.argv[1]) if len(sys.argv) > 1 else ITEMS)
