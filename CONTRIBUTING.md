# Contributing

tanda is a concurrency library: the bar for merging is not "the tests pass"
but "the semantics are unambiguous and adversarially tested". Read
[GUIDELINES.md](GUIDELINES.md) before proposing API changes — it is
normative.

## Ground rules

- A feature with ambiguous semantics does not ship. Timeouts, cancellation,
  retries, ordering, and exception propagation must have their behavior
  written down exactly.
- Public API additions need a matching update to GUIDELINES.md in the same
  pull request.
- Stay inside the V1 scope and non-goals listed in GUIDELINES.md. Proposals
  for scheduling, DAGs, rate limiting, etc. will be declined.

## Testing

Concurrency needs adversarial tests, not happy-path tests. The required
coverage areas:

**Basics**

- `map` preserves input order; `imap_unordered` yields in completion order
- empty iterable, single item, generator input
- an exception in the worker function propagates as specified

**Retry**

- fails once then succeeds; always fails; exact attempt counts
- only listed exception types are retried
- backoff waits are interruptible by cancellation

**Timeout**

- task completes before / exceeds `task_timeout`
- timeout combined with retry; `overall_timeout`
- a running task is *not* force-killed — assert the honest semantics

**Cancellation**

- cancel pending tasks; cancel during execution; cancel during retry backoff
- cancellation is idempotent
- Ctrl+C: pending cancelled, executor shut down, `KeyboardInterrupt`
  re-raised

**Concurrency stress**

- 1 worker and 100 workers; 10k jobs
- slow producer, slow consumer
- races: completion vs. shutdown, exception vs. cancellation

**Reentrancy**

- callbacks that call back into the pool (this is where lock-held-callback
  deadlocks surface)

### Determinism

No sleep-and-hope tests:

```python
# flaky — will not be merged
time.sleep(0.1)
assert ...
```

Use explicit synchronization (`Event`, `Barrier`, `Condition`) so the test
controls exactly when each transition happens:

```python
started = threading.Event()
release = threading.Event()

def worker():
    started.set()
    release.wait()
```

## Benchmarks

Benchmark real scenarios, not just overhead:

1. **Executor overhead** — raw `ThreadPoolExecutor` vs. tanda on no-op tasks.
   This quantifies the cost of the abstraction.
2. **I/O simulation** — 1000 tasks with 5–20 ms latency.
3. **Retries** — 10% transient failure rate.
4. **Large iterable** — 1M items, bounded submission vs. submit-all,
   comparing peak memory.

### Benchmark honesty

Publish the numbers where tanda loses. An overhead result like

```text
No-op tasks
ThreadPoolExecutor.map   120 ms
tanda                    155 ms   (+29%)
```

is a valid, publishable result — tanda does not exist to win a no-op
benchmark. It exists for results like

```text
1M input items
submit-all      peak memory 1.8 GB
tanda bounded   peak memory 14 MB
```

Never cherry-pick. The performance budget (maximum acceptable overhead) is
frozen only after measuring — see GUIDELINES.md.

## Commits and pull requests

- Conventional commit format: `feat:`, `fix:`, `refactor:`, `docs:`,
  `test:`, `chore:`, `perf:`, `ci:`.
- A PR description states what changed and how it was verified. If a
  benchmark is affected, include before/after numbers.
