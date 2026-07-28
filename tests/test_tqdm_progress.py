"""TqdmProgress: the optional tqdm adapter.

Most of these run against a stub bar rather than the real tqdm — the adapter's
contract is which calls it makes, and stubbing keeps the suite meaningful on
an environment without the optional dependency. One smoke test drives the real
library when it is installed.
"""

import io
import os
import subprocess
import sys
import types

import pytest

from tanda import Pool, TaskError, TqdmProgress


class _StubBar:
    instances: list["_StubBar"] = []

    def __init__(self, total=None, **kwargs):
        self.total = total
        self.kwargs = kwargs
        self.updates = []
        self.closed = 0
        _StubBar.instances.append(self)

    def update(self, n=1):
        self.updates.append(n)

    def close(self):
        self.closed += 1


@pytest.fixture
def stub_tqdm(monkeypatch):
    """Make ``from tqdm import tqdm`` resolve to _StubBar."""
    _StubBar.instances = []
    module = types.ModuleType("tqdm")
    module.tqdm = _StubBar
    monkeypatch.setitem(sys.modules, "tqdm", module)
    return _StubBar


# --- construction ------------------------------------------------------------


def test_importing_tanda_does_not_import_tqdm():
    # The whole point of the lazy import: `import tanda` must stay free of
    # optional dependencies even when they are installed.
    code = "import sys, tanda; print('tqdm' in sys.modules)"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    assert out.stdout.strip() == "False"


def test_total_keyword_is_rejected(stub_tqdm):
    with pytest.raises(TypeError, match="supplied by the batch"):
        TqdmProgress(total=10)


def test_missing_tqdm_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "tqdm", None)  # forces ImportError
    with pytest.raises(ImportError, match=r"tanda\[tqdm\]"):
        TqdmProgress()


# --- reporter protocol -------------------------------------------------------


def test_start_creates_a_bar_with_the_batch_total(stub_tqdm):
    reporter = TqdmProgress(desc="Uploading", unit="file")
    reporter.start(10)
    bar = stub_tqdm.instances[-1]
    assert bar.total == 10
    assert bar.kwargs == {"desc": "Uploading", "unit": "file"}


def test_unsized_input_passes_total_none(stub_tqdm):
    reporter = TqdmProgress()
    reporter.start(None)
    assert stub_tqdm.instances[-1].total is None


def test_advance_updates_and_finish_closes(stub_tqdm):
    reporter = TqdmProgress()
    reporter.start(3)
    reporter.advance()
    reporter.advance(2)
    reporter.finish()
    bar = stub_tqdm.instances[-1]
    assert bar.updates == [1, 2]
    assert bar.closed == 1


def test_advance_without_start_is_a_no_op(stub_tqdm):
    TqdmProgress().advance()  # never raises; no bar exists yet
    assert stub_tqdm.instances == []


def test_finish_is_idempotent(stub_tqdm):
    reporter = TqdmProgress()
    reporter.start(1)
    reporter.finish()
    reporter.finish()
    assert stub_tqdm.instances[-1].closed == 1


def test_reuse_closes_the_previous_bar(stub_tqdm):
    reporter = TqdmProgress()
    reporter.start(1)
    reporter.start(2)
    first, second = stub_tqdm.instances
    assert first.closed == 1
    assert second.total == 2


# --- through the pool --------------------------------------------------------


def test_pool_drives_the_bar_once_per_item(stub_tqdm):
    reporter = TqdmProgress()
    with Pool(workers=2) as pool:
        assert pool.map(range(5), lambda x: x, progress=reporter) == list(
            range(5)
        )
    bar = stub_tqdm.instances[-1]
    assert bar.total == 5
    assert bar.updates == [1] * 5
    assert bar.closed == 1


def test_bar_is_closed_when_the_batch_fails(stub_tqdm):
    reporter = TqdmProgress()
    with Pool(workers=2) as pool:
        with pytest.raises(TaskError):
            pool.map([1], lambda x: 1 / 0, progress=reporter)
    assert stub_tqdm.instances[-1].closed == 1


def test_real_tqdm_smoke():
    pytest.importorskip("tqdm")
    sink = io.StringIO()
    reporter = TqdmProgress(file=sink)
    with Pool(workers=2) as pool:
        assert pool.map(range(3), lambda x: x, progress=reporter) == [0, 1, 2]
    assert "3/3" in sink.getvalue()
