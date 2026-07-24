"""Wiring progress into your own display with CallbackProgress.

Run:  python examples/04_progress_callback.py

CallbackProgress calls ``callback(completed, total)`` after every completed
item — the hook for feeding a GUI, a log line every N items, a web socket,
or (shown here) a homemade milestone logger. No subclassing required; any
object with start/advance/finish also works as a reporter.
"""

import logging
import random
import time

from tanda import CallbackProgress, Pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("milestones")


def every_10_items(completed: int, total: int | None) -> None:
    if completed % 10 == 0 or completed == total:
        log.info("processed %d/%s items", completed, total if total else "?")


def work(n: int) -> int:
    time.sleep(random.uniform(0.01, 0.03))
    return n * n


if __name__ == "__main__":
    with Pool(workers=8) as pool:
        squares = pool.map(range(50), work, progress=CallbackProgress(every_10_items))
    print(f"done: {len(squares)} results")
