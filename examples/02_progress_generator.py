"""Progress over an unsized (generator) input.

Run:  python examples/02_progress_generator.py

The generator is consumed lazily — tanda never materializes it to count it,
so there is no percentage or ETA, but count, rate, and elapsed time still
render:

    Processing 1,213 items  388.9 items/s  3.1s

This is the shape that scales to millions of items in constant memory.
"""

import random
import time
from collections.abc import Iterator

from tanda import Pool


def crawl_queue() -> Iterator[str]:
    """Pretend we discover URLs as we go — total unknown up front."""
    for n in range(300):
        yield f"https://example.com/page/{n}"


def fetch(url: str) -> int:
    time.sleep(random.uniform(0.005, 0.02))
    return hash(url) % 1000


if __name__ == "__main__":
    with Pool(workers=16) as pool:
        checksums = pool.map(crawl_queue(), fetch, progress=True)
    print(f"fetched {len(checksums)} pages")
