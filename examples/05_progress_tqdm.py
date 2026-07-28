"""Using a tqdm bar instead of the built-in one.

Run:  pip install "tanda[tqdm]"
      python examples/05_progress_tqdm.py

TqdmProgress forwards tanda's progress to tqdm, so the bar looks like every
other tqdm bar in your program. Constructor keywords go straight to
``tqdm.tqdm`` — except ``total``, which comes from the batch (and is None
for generator input, where tqdm shows a count and a rate instead).

The counting rule is tanda's, not tqdm's: the bar advances once per item,
even when that item took three attempts.
"""

import random
import time

from tanda import Pool, RetryPolicy, TqdmProgress


def flaky_upload(n: int) -> int:
    time.sleep(random.uniform(0.01, 0.04))
    if random.random() < 0.3:
        raise ConnectionError(f"transient failure on {n}")
    return n


if __name__ == "__main__":
    with Pool(workers=8) as pool:
        uploaded = pool.map(
            range(60),
            flaky_upload,
            progress=TqdmProgress(desc="Uploading", unit="file"),
            retry=RetryPolicy(
                max_attempts=5, retry_on=(ConnectionError,), backoff=0.05
            ),
        )
    print(f"done: {len(uploaded)} uploaded")
