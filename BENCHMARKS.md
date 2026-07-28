# Benchmarks

Measured numbers for tanda, including the ones where it loses. Methodology
and the honesty rules live in [CONTRIBUTING.md](CONTRIBUTING.md); the
scenarios themselves are in [benchmarks/](benchmarks/).

```bash
PYTHONPATH=src python benchmarks/run_all.py           # everything
PYTHONPATH=src python benchmarks/run_all.py --quick   # skip the 1M-item run
PYTHONPATH=src python benchmarks/bench_overhead.py    # one scenario
```

Each timing is the best of five runs after a discarded warm-up; the median is
printed next to it so a noisy machine is visible rather than hidden. Memory is
peak Python-heap allocation measured with `tracemalloc`.

## Environment

The numbers below come from one machine and are not a promise about yours:

```text
python 3.13.3 | Windows AMD64 | cpython
```

## No-op tasks — the abstraction's price

```text
No-op tasks (50,000 items, 8 workers)
  ThreadPoolExecutor.map     278.2 ms  (median 299.8 ms)
  tanda Pool.map             398.7 ms  (median 499.2 ms)   (+43%)
  tanda imap_unordered       487.2 ms  (median 505.3 ms)   (+75%)
```

tanda loses this one, by design and by construction. With nothing to wait for,
every microsecond of coordination is overhead, and the coordinator does
strictly more per item than `executor.map`: a `WorkItem` with a lock, a state
machine, a completion wait, an ordered collection step. Roughly 2.4 µs per
item for `map`, 4.2 µs for `imap_unordered`.

That is the cost of the lifecycle. It is worth paying when a task waits on a
network, and not worth paying when it does not — which is the same thing as
saying tanda is for I/O-bound work.

## Simulated I/O — the workload tanda is for

```text
Simulated I/O (1,000 tasks, 5-20 ms each, 32 workers)
  ThreadPoolExecutor.map     593.4 ms
  tanda Pool.map             593.8 ms   (+0%)
```

Microseconds of coordination against milliseconds of waiting: the overhead
disappears into the noise. Both implementations sit on the ideal time,
`sum(latencies) / workers`.

## Retries — mechanism, and where backoff runs

```text
Retries, no backoff (2,000 items, 10% transient failures, 16 workers)
  hand-rolled retry loop      13.4 ms
  tanda RetryPolicy           25.4 ms   (+89%)

Retries, 10 ms backoff (same workload)
  hand-rolled, sleep in worker      136.9 ms
  tanda, backoff in coordinator      56.7 ms   (-59%)
```

Two different results, and both are honest. Re-running a failed item costs
more in tanda than in an inline `for attempt in ...` loop, because the
resubmission goes back through the bounded window instead of staying on the
worker that already holds the item.

The moment a backoff is involved, that inversion pays for itself: the
hand-rolled version sleeps *inside* a pool thread, so 200 retrying items hold
16 workers hostage for 10 ms each while other work queues behind them. tanda
parks the item in the coordinator and frees the worker immediately.

## Large input — the reason bounded submission exists

```text
Peak memory over 1,000,000 input items
  submit-all     peak   1715.7 MB
  tanda bounded  peak      0.2 MB   (10,803x less)
```

Both sides run the same no-op over the same million items and discard results
as they arrive; the only difference is how many `Future` objects are alive at
once — one per input item versus at most `max_pending` (32 here). This is not
a micro-optimization: submit-all does not get slower on a bigger input, it
gets impossible.

## Performance budget

Frozen after measuring, per [GUIDELINES.md](GUIDELINES.md):

| Scenario | Budget (best-of-five vs raw `ThreadPoolExecutor`) |
|---|---|
| No-op `map()` | ≤ +60% |
| No-op `imap_unordered()` | ≤ +100% |
| I/O-bound workload | ≤ +5% |

Exceeding a budget is a `perf` bug, not a new baseline. A change that trades
overhead for correctness may raise a number — but it raises it in this table,
in the same pull request, with the reason written down.
