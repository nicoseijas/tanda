"""Structured per-task results and the collect-mode batch envelope.

Plain ``map()`` keeps returning ``list[T]``; these types only appear when the
caller opts into ``error_policy="collect"`` or (later) advanced APIs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class TaskResult(Generic[T, R]):
    """A successfully completed item plus its execution metadata."""

    item: T
    index: int
    value: R
    attempts: int
    elapsed: float


@dataclass(frozen=True, slots=True)
class TaskFailure(Generic[T]):
    """A definitively failed item plus its execution metadata."""

    item: T
    index: int
    exception: BaseException
    attempts: int
    elapsed: float


@dataclass(frozen=True, slots=True)
class BatchResult(Generic[T, R]):
    """Collect-mode outcome: successes and failures separated, never a mixed
    list of results and exceptions. Tuples (genuinely immutable, not just a
    frozen container around mutable lists), ordered by input index."""

    successful: tuple[TaskResult[T, R], ...]
    failed: tuple[TaskFailure[T], ...]
