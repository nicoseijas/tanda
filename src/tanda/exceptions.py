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

    _VERB = "failed"

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
            f"item {index} {self._VERB} after {attempts} attempt(s) in "
            f"{elapsed:.3f}s: {exception!r}"
        )


class TaskTimeout(TaskError):
    """An item exceeded ``task_timeout``.

    Honest semantics: Python cannot kill a running thread. The underlying
    call may still be executing when this is raised; its eventual result is
    discarded, and the occupied worker only frees up when the call returns.
    ``exception`` holds the ``TimeoutError`` that describes the deadline.
    """

    _VERB = "timed out"


class Cancelled(Exception):
    """The run was cancelled via a :class:`~tanda.Cancellation` token.

    Raised by ``map()``/``imap_unordered()`` when their token is requested,
    and by ``Cancellation.raise_if_requested()`` inside cooperative task
    functions. Deliberately an ``Exception`` (unlike
    ``concurrent.futures.CancelledError``): a requested cancellation is an
    expected outcome, not a control-flow signal to bypass handlers.
    """


class OverallTimeout(Exception):
    """The whole ``map()``/``imap_unordered()`` call exceeded
    ``overall_timeout``. Pending tasks are cancelled; running tasks finish in
    the background. Raised under every error policy — a partial batch is
    never silently returned."""
