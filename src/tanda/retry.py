"""RetryPolicy — attempt counting and backoff.

Retrying is only safe when the operation is idempotent or the caller
provides idempotency guarantees: ``pool.map(orders, charge_credit_card,
retry=...)`` can charge a card multiple times. tanda re-runs the task
function verbatim; deduplication is the caller's responsibility.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Literal

BackoffStrategy = Literal["fixed", "exponential"]
_BACKOFF_STRATEGIES = ("fixed", "exponential")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How failed tasks are re-executed.

    ``max_attempts`` counts total executions — ``max_attempts=3`` means one
    initial run plus at most two retries; there is deliberately no ambiguous
    ``retries=`` parameter. Only exceptions matching ``retry_on`` (by
    ``isinstance``, so subclasses count) are retried; anything else fails
    immediately. BaseExceptions like KeyboardInterrupt are never retryable
    and are rejected at construction.

    ``backoff`` is the base delay in seconds between attempts. With
    ``backoff_strategy="fixed"`` every wait is ``backoff``; with
    ``"exponential"`` the wait after the Nth execution is
    ``backoff * 2**(N-1)``, growing without cap. ``jitter=True`` applies
    full jitter: each wait becomes ``uniform(0, computed_delay)``.
    """

    max_attempts: int = 3
    retry_on: tuple[type[Exception], ...] = (Exception,)
    backoff: float = 0.0
    backoff_strategy: BackoffStrategy = "fixed"
    jitter: bool = False

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.backoff < 0:
            raise ValueError(f"backoff must be >= 0, got {self.backoff}")
        if self.backoff_strategy not in _BACKOFF_STRATEGIES:
            raise ValueError(
                f"backoff_strategy must be one of {_BACKOFF_STRATEGIES}, "
                f"got {self.backoff_strategy!r}"
            )
        if not self.retry_on:
            raise ValueError("retry_on must name at least one exception type")
        for exc_type in self.retry_on:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise ValueError(
                    "retry_on entries must be Exception subclasses "
                    f"(BaseException is never retryable), got {exc_type!r}"
                )

    def should_retry(self, exception: BaseException, attempts: int) -> bool:
        """Whether a task that has executed ``attempts`` times and just
        failed with ``exception`` should run again."""
        return attempts < self.max_attempts and isinstance(exception, self.retry_on)

    def delay_for(self, attempts: int) -> float:
        """Seconds to wait before the next execution, after ``attempts``
        completed executions (1-based)."""
        if self.backoff == 0:
            return 0.0
        if self.backoff_strategy == "exponential":
            delay = self.backoff * (2 ** (attempts - 1))
        else:
            delay = self.backoff
        if self.jitter:
            delay = random.uniform(0.0, delay)
        return delay
