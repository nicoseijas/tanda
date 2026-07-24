"""Stable public errors."""

from __future__ import annotations

from typing import Any


class TaskError(Exception):
    """An item failed definitively under ``error_policy="raise"``.

    Carries everything needed to diagnose or reprocess the failure without
    re-deriving it from logs: the input item, its position, the underlying
    exception (also chained as ``__cause__``), how many attempts ran, and the
    elapsed execution time in seconds.
    """

    def __init__(
        self,
        item: Any,
        index: int,
        exception: BaseException,
        attempts: int,
        elapsed: float,
    ) -> None:
        self.item = item
        self.index = index
        self.exception = exception
        self.attempts = attempts
        self.elapsed = elapsed
        super().__init__(
            f"item {index} failed after {attempts} attempt(s) in "
            f"{elapsed:.3f}s: {exception!r}"
        )
