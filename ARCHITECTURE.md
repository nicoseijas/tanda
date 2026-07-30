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
    ├── tqdm_progress.py  # optional tqdm adapter (imports tqdm lazily)
    ├── cancellation.py   # global cancellation state
    ├── results.py        # TaskResult / TaskFailure / BatchResult
    ├── exceptions.py     # stable public errors (TaskError, ...)
    ├── _scheduler.py     # bounded submission, future lifecycle
    └── _states.py        # task state machine (states + legal transitions)
```

Responsibilities:

| Piece | Owns |
|---|---|
| `Pool` | public API, defaults, context-manager lifecycle, shutdown and coordinator ownership |
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
- backpressure is inherent — a slow consumer of `imap()` or
  `imap_unordered()` slows submission instead of accumulating results.

`imap()` reorders the completion stream back to input order in the `Pool`
layer: out-of-order completions are held in a buffer until their index's
turn. The window still bounds futures and submissions, but the buffer holds
finished results and is not window-bounded — a slow head item lets it grow
up to O(completed). The scheduler is unaware of this; ordering is purely a
consumer-side concern.

## Timeout handling

`task_timeout` is enforced by the coordinator, not by killing threads —
Python cannot do that. The clock is per *execution*: the worker stamps
`started_at` when it begins an item, and queue time never counts.

Timeouts break the sequential ownership hand-off: the coordinator may
finalize an item the worker still holds. Every finalizing transition is
therefore a compare-and-set under the item's own lock — whoever finalizes
first wins, and the loser's outcome is dropped (a timed-out execution's late
result goes nowhere). This is the same mechanism cooperative cancellation
(#6) needs.

A timed-out task's future moves to an *abandoned* set: it keeps occupying a
window slot (the worker is genuinely busy) until the function returns, at
which point the slot frees. If the retry policy matches `TimeoutError`, the
item is *cloned* into `RETRY_WAIT` — the original stays with the abandoned
execution, which can no longer touch the clone. `overall_timeout` is a
single coordinator deadline that raises under every error policy. An
explicit cancellation request takes precedence over the overall deadline
when both are expired at the same check.

## Shutdown

`ThreadPoolExecutor.shutdown(wait=True)` takes no timeout, so a bounded
`close()` is assembled from two steps: `shutdown(wait=False,
cancel_futures=True)` drains the queue immediately, then the coordinator
waits on the *leaked* futures itself.

Leaked futures are the only ones worth waiting on: after a run ends, the
scheduler's teardown hands `Pool` the futures whose workers are still inside
`fn` — abandoned (timed-out) executions, plus anything left running by a
cancelled or failed batch. Cancelled futures are deliberately excluded. A
future the queue drain removed sits in plain `CANCELLED`, never
`CANCELLED_AND_NOTIFIED`, and `concurrent.futures.wait()` never reports those
as done — waiting on one is the hang described under the scheduler.

Ownership is enforced in `Pool`, not the scheduler: a small lock guards the
identity of the thread currently running a batch. Only that thread may call
`close()`, which is what makes `shutdown(cancel_futures=True)` safe — no
`run()` loop can be waiting on the futures being drained. The claim is taken
on `map()` entry, and on the *first resumption* of an `imap()` or
`imap_unordered()` stream (never at call time — a stream that is created
and never iterated must not leave the pool looking busy). It spans the
stream's suspensions, so another thread cannot slip a batch in between two
yields.

Every coordinator wait is capped at a 0.5 s slice — unconditionally, not
only when a `Cancellation` token is in use. This bounds how long a
cancellation request (or a Ctrl+C on Windows, where a blocked wait cannot
be interrupted mid-call) can go unnoticed, at the cost of at most two
wakeups per second on an otherwise idle coordinator. Each wakeup re-checks
deadlines against real timestamps, so the cap never changes *when* things
happen — only how promptly they are observed.
