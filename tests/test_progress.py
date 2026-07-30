"""Progress: item-based counting, reporter contract, rendering."""

import io
import threading

import pytest

from tanda import (
    CallbackProgress,
    DefaultProgress,
    NullProgress,
    Pool,
    RetryPolicy,
    TaskError,
)

WAIT = 5.0


class Recorder:
    """Minimal reporter that records the exact call sequence."""

    def __init__(self):
        self.events = []

    def start(self, total):
        self.events.append(("start", total))

    def advance(self, n=1):
        self.events.append(("advance", n))

    def finish(self):
        self.events.append(("finish",))

    @property
    def advances(self):
        return sum(n for kind, *rest in self.events if kind == "advance" for n in rest)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# --- wiring: map/imap drive the reporter -------------------------------------


def test_map_reports_total_advances_and_finish():
    reporter = Recorder()
    with Pool(workers=2) as pool:
        pool.map([10, 20, 30], lambda x: x, progress=reporter)

    assert reporter.events[0] == ("start", 3)
    assert reporter.events[-1] == ("finish",)
    assert reporter.advances == 3


def test_unsized_input_reports_total_none_without_materializing():
    reporter = Recorder()
    pulled = []

    def items():
        for i in range(5):
            pulled.append(i)
            yield i

    with Pool(workers=2) as pool:
        source = items()
        assert pulled == []  # nothing consumed before map()
        pool.map(source, lambda x: x, progress=reporter)

    assert reporter.events[0] == ("start", None)
    assert reporter.advances == 5


def test_retries_advance_once_per_item_not_per_attempt():
    lock = threading.Lock()
    calls = {}

    def flaky(item):
        with lock:
            calls[item] = calls.get(item, 0) + 1
        if calls[item] == 1:
            raise ValueError("first attempt fails")
        return item

    reporter = Recorder()
    with Pool(workers=2) as pool:
        pool.map(
            range(4),
            flaky,
            retry=RetryPolicy(max_attempts=2),
            progress=reporter,
        )

    assert sum(calls.values()) == 8  # every item ran twice...
    assert reporter.advances == 4  # ...but progress counts items, not attempts


def test_failed_item_advances_and_finish_runs_on_raise():
    reporter = Recorder()
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError):
            pool.map([1], lambda x: 1 / 0, progress=reporter)

    assert reporter.advances == 1  # the item completed — by failing
    assert reporter.events[-1] == ("finish",)


def test_imap_unordered_advances_as_items_complete():
    reporter = Recorder()
    with Pool(workers=2) as pool:
        stream = pool.imap_unordered([1, 2], lambda x: x, progress=reporter)
        assert reporter.events[0] == ("start", 2)
        results = sorted(stream)
    assert results == [1, 2]
    assert reporter.advances == 2
    assert reporter.events[-1] == ("finish",)


def test_imap_advances_on_completion_not_on_yield():
    gate = threading.Event()

    class GateOnAdvance(Recorder):
        def advance(self, n=1):
            super().advance(n)
            gate.set()

    reporter = GateOnAdvance()

    def fn(item):
        if item == "slow":
            # Only "fast"'s completion being observed — an advance firing
            # while nothing has been yielded yet — can open this gate. If
            # advance fired on yield instead, "slow" (the head item) could
            # never be yielded and this wait would fail the test.
            assert gate.wait(WAIT)
        return item

    with Pool(workers=2) as pool:
        stream = pool.imap(["slow", "fast"], fn, progress=reporter)
        assert reporter.events[0] == ("start", 2)
        assert list(stream) == ["slow", "fast"]
    assert reporter.advances == 2
    assert reporter.events[-1] == ("finish",)


def test_collect_mode_drives_progress_too():
    reporter = Recorder()
    with Pool(workers=2) as pool:
        pool.map(
            range(6),
            lambda x: x,
            progress=reporter,
            error_policy="collect",
        )
    assert reporter.advances == 6


def test_progress_true_uses_default_reporter_final_line():
    buf = io.StringIO()  # not a TTY: only the final summary is written
    with Pool(workers=2) as pool:
        pool.map([1, 2], lambda x: x, progress=DefaultProgress(stream=buf))
    out = buf.getvalue()
    assert "2/2" in out
    assert out.endswith("\n")


def test_invalid_progress_object_raises_type_error():
    with Pool(workers=2) as pool:
        with pytest.raises(TypeError, match="progress"):
            pool.map([1], lambda x: x, progress=object())


def test_null_progress_is_silent():
    reporter = NullProgress()
    reporter.start(10)
    reporter.advance()
    reporter.finish()  # nothing to assert beyond "does not blow up"


# --- CallbackProgress --------------------------------------------------------


def test_callback_progress_reports_completed_and_total():
    seen = []
    reporter = CallbackProgress(
        lambda completed, total: seen.append((completed, total))
    )
    with Pool(workers=2) as pool:
        pool.map([1, 2, 3], lambda x: x, progress=reporter)

    assert seen[-1] == (3, 3)
    assert [c for c, _ in seen] == sorted(c for c, _ in seen)  # monotonic
    # finish() must not re-deliver what the last advance already reported.
    assert len(seen) == len(set(enumerate(seen)))  # trivially true, keep pairs
    # no consecutive duplicates
    assert all(a != b for a, b in zip(seen, seen[1:], strict=False))


def test_reporter_exception_does_not_mask_the_task_outcome(caplog):
    class ExplodingReporter(Recorder):
        def advance(self, n=1):
            raise RuntimeError("broken reporter")

    reporter = ExplodingReporter()
    with Pool(workers=2) as pool:
        with caplog.at_level("WARNING", logger="tanda"):
            # The task outcome — success here — survives the broken observer.
            assert pool.map([1, 2], lambda x: x + 1, progress=reporter) == [2, 3]
    assert "reporter raised" in caplog.text


def test_reporter_exception_does_not_replace_a_task_error():
    class ExplodingReporter(Recorder):
        def advance(self, n=1):
            raise RuntimeError("broken reporter")

    with Pool(workers=2) as pool:
        with pytest.raises(TaskError):  # never RuntimeError from the reporter
            pool.map([1], lambda x: 1 / 0, progress=ExplodingReporter())


def test_unconsumed_imap_stream_starts_but_never_finishes_reporter():
    # Documented asymmetry: start() is eager at call time; finish() lives in
    # the stream's cleanup, which a never-iterated generator never runs.
    reporter = Recorder()
    with Pool(workers=2) as pool:
        pool.imap_unordered([1, 2], lambda x: x, progress=reporter)
    assert reporter.events == [("start", 2)]


# --- DefaultProgress rendering (fake clock, deterministic) -------------------


def test_default_progress_sized_rendering():
    buf = io.StringIO()
    clock = FakeClock()
    reporter = DefaultProgress(stream=buf, live=True, min_interval=0.0, clock=clock)
    reporter.start(4)
    clock.t = 2.0
    reporter.advance()
    reporter.advance()
    frames = buf.getvalue().split("\r")
    assert "50%" in frames[-1]
    assert "2/4" in frames[-1]
    assert "1.0 items/s" in frames[-1]
    assert "ETA 2.0s" in frames[-1]

    clock.t = 4.0
    reporter.advance(2)
    reporter.finish()
    final = buf.getvalue().split("\r")[-1]
    assert "100%" in final
    assert "4/4" in final
    assert "elapsed 4.0s" in final
    assert final.endswith("\n")


def test_default_progress_unsized_rendering_has_no_percent_or_eta():
    buf = io.StringIO()
    clock = FakeClock()
    reporter = DefaultProgress(stream=buf, live=True, min_interval=0.0, clock=clock)
    reporter.start(None)
    clock.t = 1.0
    reporter.advance(1500)
    line = buf.getvalue().split("\r")[-1]
    assert "1,500 items" in line
    assert "1,500.0 items/s" in line
    assert "%" not in line
    assert "ETA" not in line


def test_default_progress_non_tty_writes_only_final_line():
    buf = io.StringIO()
    reporter = DefaultProgress(stream=buf, min_interval=0.0)
    reporter.start(2)
    reporter.advance()
    assert buf.getvalue() == ""  # silent while running
    reporter.advance()
    reporter.finish()
    assert buf.getvalue().count("\n") == 1
    assert "2/2" in buf.getvalue()


def test_default_progress_empty_input_renders_cleanly():
    buf = io.StringIO()
    reporter = DefaultProgress(stream=buf, live=True, min_interval=0.0)
    reporter.start(0)
    reporter.finish()
    assert "0/0" in buf.getvalue()  # no ZeroDivisionError
