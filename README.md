# tanda

A small execution layer over `concurrent.futures.ThreadPoolExecutor` for
running many I/O-bound jobs at once — with bounded memory, progress, retries,
timeouts, cancellation, and a Ctrl+C that actually works.

*Tanda* is Spanish for a batch, a round of work.

```python
from tanda import Pool

with Pool() as pool:
    results = pool.map(files, process)
```

## Why

`ThreadPoolExecutor` gives you `submit()` and `map()`, and stops there.
Everything a real batch job needs on top — a progress line, retrying transient
failures, per-task timeouts, cancelling cleanly on Ctrl+C, not materializing a
10-million-item generator into a list of futures — gets reimplemented in every
script, slightly differently and slightly wrong each time.

tanda is not a prettier wrapper around `submit()`. It resolves the full
lifecycle of a concurrent batch, consistently:

```text
submission → concurrency → progress → retries → timeout
          → cancellation → errors → shutdown
```

The basic case needs zero configuration. Everything else is opt-in:

```python
from tanda import Pool, RetryPolicy

with Pool(workers=16) as pool:
    results = pool.map(
        files,
        process,
        progress=True,
        retry=RetryPolicy(max_attempts=3, retry_on=(OSError,)),
        task_timeout=30,
    )
```

## What you get

- **Ordered results by default.** `pool.map(["a", "b", "c"], fn)` returns
  results in input order, regardless of completion order. Use
  `imap_unordered()` to consume results as they finish.
- **Bounded submission.** Input iterables are consumed lazily; only a limited
  window of tasks (roughly `workers × 4`) is in flight at any time. A
  100-million-item generator runs in constant memory instead of creating
  100 million `Future` objects up front.
- **Progress that counts items, not attempts.** A task that fails twice and
  succeeds on the third try advances progress by 1. Unsized iterables still
  get a count, rate, and elapsed time — the iterable is never materialized
  just to obtain a `len()`.
- **Explicit retries.** `RetryPolicy(max_attempts=3, retry_on=(OSError,))` —
  attempt count instead of an ambiguous `retries=`, retry only for the
  exception types you list, with fixed or exponential backoff.
- **Honest timeouts.** `task_timeout` marks a task as timed out and applies
  your retry/error policy, but Python cannot kill a running thread — the
  underlying call may keep executing. tanda documents this instead of
  pretending otherwise (see [When not to use tanda](#when-not-to-use-tanda)).
- **Cancellation and Ctrl+C.** Pending tasks are cancelled, running tasks are
  asked to stop, the executor shuts down, and `KeyboardInterrupt` is
  re-raised — no hung process, no wall of stack traces.
- **A shutdown you can bound.** Leaving the `with` block cancels queued work
  and waits for what is still running. `Pool(shutdown_timeout=30)` caps that
  wait and raises `ShutdownTimeout` naming the stuck workers, instead of
  hanging on a task that never returns.
- **Errors that fail loudly.** By default the first definitive failure raises
  a `TaskError` carrying the item, its index, the underlying exception, the
  attempt count, and elapsed time. `error_policy="collect"` instead returns a
  `BatchResult` with successes and failures separated — never a mixed list of
  results and exceptions.

## When not to use tanda

- **CPU-bound pure-Python work.** Threads share the GIL; a thread pool will
  not speed up numeric loops, compression, or image transforms written in
  Python. Use processes — unless your library releases the GIL (NumPy, most
  I/O libraries).
- **Hard timeouts.** If a task must be forcibly killed at the deadline,
  threads are the wrong tool; use subprocesses.
- **Task orchestration.** Scheduling, DAGs, priorities, rate limiting,
  distributed execution, and persistent queues are explicit
  non-goals — that's Celery's job, not tanda's.

## Installation

Not published to PyPI yet — see [Status](#status).

```bash
pip install tanda
```

## Design

The design decisions — API surface, retry and timeout semantics, backpressure,
the task state machine — are written down before the code:

- [GUIDELINES.md](GUIDELINES.md) — design principles and API decisions
- [ARCHITECTURE.md](ARCHITECTURE.md) — module layout, task state machine,
  threading model
- [CONTRIBUTING.md](CONTRIBUTING.md) — testing and benchmark requirements
- [examples/](examples/) — runnable scripts (progress bars, retries,
  callbacks)

## Status

Pre-release, not on PyPI. The batch lifecycle described above is implemented
and tested: bounded submission, `map()` and `imap_unordered()`, retries,
`task_timeout` and `overall_timeout`, cooperative cancellation, error
policies, progress reporting, and graceful shutdown. Everything in the
examples runs today.

Before a first release: an adversarial deterministic test suite, a benchmark
suite with a published overhead budget, and packaging. `imap()` (ordered
streaming) and a job-handle API are planned after that — see the
[issues](https://github.com/nicoseijas/tanda/issues).

The surface may still change until `0.1.0` is tagged.

## License

[MIT](LICENSE)
