# Guidelines

Design principles and API decisions for tanda. This document is normative:
when an implementation choice conflicts with it, either the code or this
document must change — not silently drift apart.

The central rule:

> **tanda is not a pretty wrapper over `ThreadPoolExecutor`; it resolves the
> complete lifecycle of running many concurrent jobs, correctly.**

## Principles

1. **Simple by default.** The common case fits in one line, with zero
   configuration.
2. **Bounded by default.** Never create an unlimited number of futures.
3. **No fake cancellation.** Never claim a thread was cancelled while it is
   still executing.
4. **Timeouts are explicit.** Task, overall, and shutdown timeouts are
   different concepts with different names.
5. **Retries require semantics.** Document idempotency requirements and which
   errors are retryable.
6. **User code outside locks.** Never invoke a callback while holding an
   internal lock.
7. **Streaming first.** Accept generators without materializing them.
8. **Standard-library first.** Build on `concurrent.futures`, not against it.
9. **Fail loudly.** Never swallow exceptions for convenience.
10. **Benchmark honestly.** Measure overhead and publish the losses too.

## Philosophy

### Simple things stay simple

```python
with Pool() as pool:
    results = pool.map(files, process)
```

Advanced behavior is opt-in, on the pool or per call:

```python
with Pool(workers=16) as pool:
    results = pool.map(
        files,
        process,
        progress=True,
        retry=RetryPolicy(max_attempts=3, retry_on=(OSError,)),
        task_timeout=30,
    )
```

Per-call options override pool defaults.

### Thin abstraction, explicit semantics

Do not reinvent a scheduler. Use the standard primitives —
`ThreadPoolExecutor`, `Future`, `wait`, `as_completed`, `Event` — and add
value by orchestrating them correctly, not by hiding them.

### Correctness before convenience

Never ship a feature whose semantics are ambiguous. This applies especially to
timeouts, cancellation, retries, ordering, and exception propagation. The
documentation states exactly what happens in each case.

## Ordering

`map()` preserves input order. Given `["a", "b", "c"]`, results correspond to
`a, b, c` even if internal completion order was `c, a, b`. That is the natural
expectation for a function named `map`.

For workloads that need results as they complete, provide a separate API
rather than a flag on `map()`:

```python
for result in pool.imap_unordered(items, process):
    consume(result)
```

Planned surface: `map()`, `imap()`, `imap_unordered()` — three functions with
clear contracts, not one function with mode flags.

## Bounded submission and backpressure

This is mandatory, not a feature. Never do:

```python
futures = [executor.submit(fn, item) for item in million_items]
```

tanda keeps a limited window of tasks in flight — `max_pending`, defaulting to
`workers × 4` — and submits the next item only when a slot frees up. Input
iterables are consumed lazily, so this works in constant memory:

```python
pool.map(generate_100_million_files(), process)
```

## Progress

Progress is an *observation* of the workload; it never controls execution.

- The unit is the **item**, not the attempt. A task that fails twice and
  succeeds on the third try advances progress by exactly 1.
- Unsized iterables are supported: show count, rate, and elapsed time; show
  percentage and ETA only when a total is known. Never materialize an iterable
  to obtain `len()`.
- The core does not depend on `tqdm`. It defines a small reporter interface:

  ```python
  class ProgressReporter:
      def start(self, total): ...
      def advance(self, n=1): ...
      def finish(self): ...
  ```

  with `DefaultProgress`, `NullProgress`, and `CallbackProgress`
  implementations, and optional integration via `pip install tanda[tqdm]`.
- The parameter is `progress: bool | ProgressReporter` — `True` means
  `DefaultProgress()` (live `\r` bar on a TTY; only the final summary line
  on redirected streams, so CI logs stay clean; ASCII fallback when the
  stream can't encode block characters). `start()` fires eagerly at call
  time, `advance()` once per terminal item, `finish()` always — including
  on failure, timeout, and cancellation exits. Runnable demos live in
  `examples/`.

## Retries

- Use `max_attempts`, not `retries`. `retries=3` is ambiguous (3 or 4
  executions?); `max_attempts=3` is not.
- Not every error is retryable. Retry only the exception types the caller
  lists:

  ```python
  RetryPolicy(max_attempts=3, retry_on=(TimeoutError, ConnectionError))
  ```

  A predicate form (`retry_if=lambda exc: ...`) may come later; V1 ships
  `retry_on` only — one API, not two.
- Backoff in V1 is limited to `none`, `fixed`, and `exponential` (with
  optional jitter). tanda is not trying to become Tenacity. The frozen
  signature:

  ```python
  RetryPolicy(
      max_attempts=3,
      retry_on=(Exception,),
      backoff=0.0,               # base delay in seconds; 0 = no wait
      backoff_strategy="fixed",  # or "exponential": backoff * 2**(N-1)
      jitter=False,              # full jitter: uniform(0, computed delay)
  )
  ```

  Backoff waits happen in the coordinator (`wait()` with a timeout — no
  helper threads); items waiting out a backoff count against `max_pending`,
  and `elapsed` accumulates execution time across attempts, excluding
  backoff waits.

### Idempotency

Retrying is only safe when the operation is idempotent or the caller provides
idempotency guarantees. `pool.map(orders, charge_credit_card, retry=...)` can
charge a card multiple times. The documentation must say this plainly and
never present retries as universally safe.

## Timeouts

Three distinct concepts, three distinct names — never a bare `timeout`:

| Name | Scope |
|---|---|
| `task_timeout` | maximum time per *execution* of an item — the clock starts when the worker begins it; queue time never counts |
| `overall_timeout` | maximum time for the entire `map()` call |
| shutdown timeout | how long `__exit__` waits for running tasks |

### Be brutally honest about what a timeout means

`Future.cancel()` cannot stop a thread that is already running. Therefore
`task_timeout=5` does **not** mean "Python kills the function at second 5". It
means: from the caller's perspective the task is considered timed out and the
configured policy (retry, fail, collect) applies — but the underlying call may
keep executing in its worker thread.

Callers who need hard kills need processes or subprocesses, not threads. This
limitation is documented prominently, never hidden.

## Cancellation

Two levels, never conflated:

1. **Not-yet-started tasks** can genuinely be cancelled — their futures are
   still pending.
2. **Running tasks** can only be cancelled cooperatively. The task function
   may opt in to receiving a cancellation token and check it; tanda never
   forces every function to accept one.

State names reflect this: a pending task becomes `CANCELLED`; a running task
becomes `CANCELLATION_REQUESTED`. They are not the same state.

The frozen V1 API — the caller owns the token and the task function opts in
by closing over it, no signature magic:

```python
cancel = Cancellation()

def process(item):
    for chunk in chunks(item):
        cancel.raise_if_requested()   # cooperative checkpoint (opt-in)
        handle(chunk)

with Pool() as pool:
    pool.map(items, process, cancel=cancel)
# From any thread: cancel.request() -> map() raises Cancelled, pending
# tasks are cancelled, running tasks are asked and their late results
# dropped. Tokens are one-way; requesting is idempotent. Cancellation
# interrupts retry backoffs, and observation latency while the coordinator
# is blocked is bounded by a 0.5s wait slice.
```

`Cancelled` is deliberately an `Exception` (unlike
`concurrent.futures.CancelledError`, a `BaseException`): a requested
cancellation is an expected outcome, not a control-flow escape.

### Ctrl+C

`KeyboardInterrupt` must work correctly — this looks trivial and is routinely
broken. The policy:

```text
KeyboardInterrupt
→ signal cancellation
→ cancel pending tasks
→ shut down the executor
→ re-raise KeyboardInterrupt
```

The user sees "Cancelling N pending tasks... waiting for M running tasks", not
a hung process or a wall of stack traces.

## Error handling

Errors are never hidden. When an item fails definitively, `map()` raises
`TaskError`, which carries: the item, its index, the underlying exception, the
attempt count, and elapsed time.

Two explicit policies:

- `error_policy="raise"` (default) — first definitive failure cancels pending
  work and raises.
- `error_policy="collect"` — run everything; return a
  `BatchResult(successful=[...], failed=[...])`.

Never return a mixed list of results and exceptions
(`[result, exception, result, None]`).

### Structured results

For advanced use, `TaskResult(item, value, attempts, elapsed)` and
`TaskFailure(item, exception, attempts, elapsed)` expose per-task metadata.
The simple `map()` keeps returning a plain `list[T]` — basic users are never
forced to unwrap.

## Lifecycle

The context manager is the primary path:

```python
with Pool() as pool:
    ...
```

On exit: stop accepting submissions, cancel according to policy, wait
according to policy, release resources. An explicit `pool.close()` is
supported but secondary.

`shutdown_timeout` is the third timeout, and it is not a task timeout: it
bounds how long `close()` waits for workers still inside `fn` — including
executions already declared timed out, which keep running because Python
cannot kill a thread.

```python
with Pool(shutdown_timeout=30) as pool:   # or pool.close(timeout=30)
    ...
```

Default `None`: wait indefinitely, like `ThreadPoolExecutor.shutdown()`. A
finite default would be false comfort — giving up does not stop anything, and
pool threads are not daemons, so an abandoned worker still holds the process
open at exit. What a bounded shutdown buys is a `ShutdownTimeout` naming the
leak instead of a silent hang. `timeout=0` gives up immediately; an explicit
`timeout=None` overrides a finite pool default.

Exiting the `with` block never lets a shutdown complaint replace the
exception that ended the block: if the body raised, a `ShutdownTimeout` is
logged as a warning instead of propagating.

### One coordinator per pool

A pool belongs to the thread that drives it, and the rule is enforced, not
just documented. A second thread entering `map()`/`imap_unordered()`, or
calling `close()` while a batch runs, raises `RuntimeError`. So does a task
function calling back into its own pool — it runs on a worker thread, so the
same check catches it.

The alternative is worse than an error: concurrent runs would each get their
own `max_pending` window (the pool-wide bound would silently stop holding),
and a cross-thread `close()` can leave the coordinator waiting on futures the
shutdown drained, which `concurrent.futures.wait()` never reports as done —
a permanent hang. Same-thread nesting stays legal; it cannot deadlock.

## No unnecessary threads

No dedicated threads for progress, retries, scheduling, or timeouts if they
can be avoided. The architecture is a caller/coordinator driving a single
`ThreadPoolExecutor`; progress, retry, and orchestration live in the
coordinator. Fewer threads means fewer states to reason about. See
[ARCHITECTURE.md](ARCHITECTURE.md).

## Threads are for I/O

Document explicitly: a thread pool helps I/O-bound workloads (HTTP,
filesystem, databases, cloud APIs, subprocess orchestration). It does not help
CPU-bound pure-Python work (numeric loops, compression, image transforms)
because of the GIL — unless the library in use releases it. CPU-bound work
belongs in a process pool, which is out of tanda's scope.

## Naming

Avoid calling everything `Task` — it collides with `asyncio.Task`. Preferred
vocabulary: `Pool`, `Job`, `WorkItem`, `BatchResult`, `RetryPolicy`,
`Cancellation`.

## V1 surface

```python
Pool(workers=None, max_pending=None)

pool.map(items, fn, *, progress=False, retry=None,
         task_timeout=None, overall_timeout=None)
pool.imap_unordered(...)

RetryPolicy(max_attempts=3, retry_on=(Exception,), backoff=0.0,
            backoff_strategy="fixed", jitter=False)
```

Nothing else initially. The MVP must deliver, internally: bounded pending
futures, ordered results, progress, retries, cooperative cancellation, honest
task-timeout semantics, working Ctrl+C, and graceful shutdown.

### Non-goals

Out of scope, deliberately: cron/scheduling, distributed execution,
multiprocessing, async/await integration, dependency graphs, priority queues,
rate limiting, circuit breakers, persistent queues, task DAGs. Adding those
turns tanda into a bad Celery instead of a good execution layer.

### Later (maybe)

- A `Job` handle API (`job = pool.start(...)`, iterate, `job.cancel()`,
  `job.stats`).
- Observability: `stats.submitted / running / completed / failed / retried /
  cancelled / timed_out / elapsed / rate`. Metrics must be cheap and opt-in if
  they touch hot paths — the goal is making visible what normally happens
  hidden.

## Performance budget

Without progress/retry/timeout enabled, the wrapper should add no more than a
fixed overhead percentage versus raw `ThreadPoolExecutor` on sufficiently
large workloads. The percentage is not chosen in advance: measure first, then
freeze the target. Benchmark methodology and honesty rules are in
[CONTRIBUTING.md](CONTRIBUTING.md).
