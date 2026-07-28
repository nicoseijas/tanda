"""Optional tqdm-backed progress reporter.

Kept in its own module so importing ``tanda`` never imports tqdm: the class
is re-exported from the package, but the third-party import happens when a
``TqdmProgress`` is constructed, not at import time. Install the dependency
with ``pip install tanda[tqdm]``.
"""

from __future__ import annotations

from typing import Any


class TqdmProgress:
    """Reports progress through a tqdm bar.

        with Pool() as pool:
            pool.map(files, process, progress=TqdmProgress())

    Constructor keywords are forwarded to ``tqdm.tqdm``, so the usual
    knobs work — ``TqdmProgress(desc="Uploading", unit="file",
    leave=False)``. ``total`` is not accepted: it comes from the batch, and
    is ``None`` for unsized input (tqdm then shows a count and a rate,
    matching :class:`DefaultProgress`).

    Reusing one instance across batches is fine: each ``start()`` closes the
    previous bar and opens a fresh one.
    """

    def __init__(self, **tqdm_kwargs: Any) -> None:
        if "total" in tqdm_kwargs:
            raise TypeError(
                "total is supplied by the batch, not by the reporter; "
                "tanda passes it to start()"
            )
        try:
            from tqdm import tqdm
        except ImportError as exc:  # pragma: no cover - depends on the env
            raise ImportError(
                "TqdmProgress requires tqdm: pip install tanda[tqdm]"
            ) from exc
        self._tqdm = tqdm
        self._kwargs = tqdm_kwargs
        self._bar: Any | None = None

    def start(self, total: int | None) -> None:
        self.finish()  # a previous batch's bar must not stay open
        self._bar = self._tqdm(total=total, **self._kwargs)

    def advance(self, n: int = 1) -> None:
        if self._bar is not None:
            self._bar.update(n)

    def finish(self) -> None:
        # finish() is called on every exit path, including ones where start()
        # never ran (and it may be called twice); closing once is enough.
        bar, self._bar = self._bar, None
        if bar is not None:
            bar.close()
