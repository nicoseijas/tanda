"""Progress with flaky tasks and retries.

Run:  python examples/03_progress_with_retries.py

Roughly 30% of calls fail transiently and are retried (up to 3 attempts with
a short fixed backoff). Watch the bar: it advances once per ITEM, never per
attempt — a task that fails twice and then succeeds still moves the bar by
exactly one step. The collect-mode summary at the end shows how many
attempts each stubborn item needed.
"""

import random
import time

from tanda import Pool, RetryPolicy

items = [f"job-{n:02}" for n in range(30)]


def unreliable(job: str) -> str:
    time.sleep(random.uniform(0.02, 0.05))
    if random.random() < 0.30:  # transient failure
        raise ConnectionError(f"{job}: connection reset")
    return job.upper()


if __name__ == "__main__":
    with Pool(workers=6) as pool:
        batch = pool.map(
            items,
            unreliable,
            retry=RetryPolicy(
                max_attempts=3, retry_on=(ConnectionError,), backoff=0.05
            ),
            progress=True,
            error_policy="collect",
        )

    retried = [r for r in batch.successful if r.attempts > 1]
    print(f"succeeded: {len(batch.successful)}  failed for good: {len(batch.failed)}")
    for r in retried:
        print(f"  {r.item}: needed {r.attempts} attempts ({r.elapsed:.2f}s inside fn)")
    for f in batch.failed:
        print(f"  {f.item}: gave up after {f.attempts} attempts ({f.exception!r})")
