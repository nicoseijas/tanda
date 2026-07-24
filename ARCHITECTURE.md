# Architecture

Internal structure for tanda. Read [GUIDELINES.md](GUIDELINES.md) first — this
document describes *how* the code is organized to satisfy those decisions.

## Layout

```text
src/
└── tanda/
    ├── pool.py           # Pool — public API
    ├── retry.py          # RetryPolicy — attempts + backoff
    ├── progress.py       # ProgressReporter and implementations
    ├── cancellation.py   # global cancellation state
    ├── results.py        # TaskResult / TaskFailure / BatchResult
    ├── exceptions.py     # stable public errors (TaskError, ...)
    └── _scheduler.py     # bounded submission, future lifecycle
```

Responsibilities:

| Piece | Owns |
|---|---|
| `Pool` | public API, defaults, context-manager lifecycle |
| `_scheduler` | bounded submission window, future lifecycle, result ordering |
| `RetryPolicy` | attempt counting, backoff computation |
| `Cancellation` | shared cancellation state, cooperative tokens |
| `ProgressReporter` | decoupled reporting; never controls execution |
| `results` | per-task metadata |
| `exceptions` | stable public error types |

## Threading model

```text
caller / coordinator
        │
        ▼
ThreadPoolExecutor
        │
   worker threads
```

- The **coordinator** (the thread that called `map()`) owns orchestration:
  the futures map, the retry queue, progress state, and the submission
  window. No dedicated threads for progress, retries, scheduling, or
  timeouts.
- **Workers** own exactly one thing: executing the user's function. They do
  not mutate shared coordinator structures beyond signalling completion
  through their `Future`.

### Thread-safety rules

- Every piece of shared state has a single clear owner. No global locks on
  hot paths.
- **User code never runs under an internal lock.** This includes progress
  callbacks, completion callbacks, retry callbacks, and error handlers. The
  pattern is always:

  ```python
  with lock:
      update_internal_state()

  callback()   # outside the lock
  ```

  Running callbacks under a lock invites deadlocks, reentrancy bugs, and
  unbounded stalls — a callback may block, raise, or call back into the pool.

## Task state machine

Define the legal transitions before writing the scheduler; the scheduler
implements this diagram and nothing else.

```text
PENDING
   ↓
RUNNING
   ├── SUCCESS
   ├── RETRY_WAIT ──→ PENDING
   ├── FAILED
   ├── TIMED_OUT
   └── CANCELLATION_REQUESTED
```

Rules the diagram encodes:

- `SUCCESS → RETRY` is illegal. Completed work is never re-executed.
- A `PENDING` task can move to `CANCELLED`; a `RUNNING` task cannot — threads
  cannot be killed, so the honest state is `CANCELLATION_REQUESTED` and the
  transition out of it depends on the task noticing (cooperative) or
  finishing on its own.
- `RETRY_WAIT` returns the task to `PENDING`; it re-enters the same bounded
  submission window as everything else. Progress does not advance on this
  path — only terminal states advance progress.
- Terminal states: `SUCCESS`, `FAILED`, `TIMED_OUT`, `CANCELLED`.

## Bounded submission

The scheduler maintains an in-flight window (`max_pending`, default
`workers × 4`). The input iterable is pulled lazily: one completion frees one
slot, which pulls one more item. Consequences:

- memory is O(window), not O(input size);
- infinite or very large generators are safe;
- backpressure is inherent — a slow consumer of `imap_unordered()` slows
  submission instead of accumulating results.

## Timeout handling

`task_timeout` is enforced from the coordinator's perspective (the future's
result is awaited with a deadline), not by killing threads — Python cannot do
that. A timed-out task transitions to `TIMED_OUT` (or `RETRY_WAIT` if the
policy retries timeouts) while its worker thread may still be executing. The
scheduler must account for this: a "leaked" running task still occupies a
worker until its function returns, and shutdown must decide whether to wait
for it (shutdown timeout) or abandon it.
