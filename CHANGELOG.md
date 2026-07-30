# Changelog

Notable changes, newest first. Versions follow
[semantic versioning](https://semver.org); anything under `0.x` may still
change its surface, and this file says so when it does.

## 0.2.0 — 2026-07-30

### Added

- `Pool.imap()` — the streaming counterpart of `map()`: yields results in
  input order as they become ready, without waiting for the whole batch.
  Out-of-order completions are buffered until their turn; futures stay
  bounded by `max_pending`, but one slow head item can grow that result
  buffer up to O(completed) — `imap_unordered()` remains the
  constant-memory stream. Error semantics match `map()`: the first
  definitive failure cancels pending work and raises immediately. This
  completes the three-function surface (`map`/`imap`/`imap_unordered`)
  planned in GUIDELINES.md.

## 0.1.0 — 2026-07-28

First release. Everything below is what it ships with.

### Added

- `Pool` with `map()` (ordered results) and `imap_unordered()` (completion
  order), over a bounded submission window that keeps memory O(window)
  instead of O(input).
- `RetryPolicy` with `max_attempts`, selective `retry_on`, fixed or
  exponential backoff, and optional full jitter. Backoff waits happen in the
  coordinator, so a retrying item never holds a worker thread.
- `task_timeout`, `overall_timeout`, and `shutdown_timeout` — three named
  concepts, with documented honest semantics: a timed-out execution is
  abandoned, never killed.
- Cooperative cancellation through a caller-owned `Cancellation` token, plus
  a `KeyboardInterrupt` path that cancels pending work and re-raises.
- Error policies: `TaskError`/`TaskTimeout` on the first definitive failure,
  or a `BatchResult` of successes and failures under
  `error_policy="collect"`.
- Progress reporting: `DefaultProgress`, `NullProgress`, `CallbackProgress`,
  and the optional `TqdmProgress` (`pip install tanda[tqdm]`) behind a
  three-method reporter protocol.
- Adversarial test suite covering 10k-item batches, the timeout/completion
  and error/cancellation races, backpressure, and reentrancy.
- Benchmark suite and a frozen overhead budget — see
  [BENCHMARKS.md](BENCHMARKS.md).
- Packaging: typed distribution (`py.typed`), `[tqdm]` and `[dev]` extras,
  and CI across Python 3.10–3.13 on Linux and Windows.
