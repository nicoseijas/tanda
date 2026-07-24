"""Basic progress bar over a sized workload.

Run:  python examples/01_progress_basic.py

Each "download" sleeps 30-80 ms to simulate I/O latency. With a sized input
the bar shows percentage, counts, rate, and ETA:

    Processing ████████████░░░░░░░░  60%  30/50  21.4 items/s  ETA 0.9s
"""

import random
import time

from tanda import Pool

files = [f"report-{n:03}.pdf" for n in range(50)]


def download(name: str) -> int:
    time.sleep(random.uniform(0.03, 0.08))  # pretend this is a network call
    return len(name)


if __name__ == "__main__":
    with Pool(workers=8) as pool:
        sizes = pool.map(files, download, progress=True)
    print(f"downloaded {len(sizes)} files, {sum(sizes)} bytes total")
