"""Task state machine.

The scheduler implements exactly the diagram in ARCHITECTURE.md; any
transition outside ``LEGAL_TRANSITIONS`` is a bug in tanda, not a user error.

Two deliberate asymmetries encode the honesty rules from GUIDELINES.md:

- ``SUCCESS`` has no outgoing transitions: completed work is never re-run.
- ``RUNNING`` cannot reach ``CANCELLED`` directly: a thread cannot be killed,
  so cancelling running work only ever *requests* it
  (``CANCELLATION_REQUESTED``).
"""

from __future__ import annotations

import enum


class TaskState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    CANCELLATION_REQUESTED = "cancellation_requested"
    SUCCESS = "success"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {
        TaskState.SUCCESS,
        TaskState.FAILED,
        TaskState.TIMED_OUT,
        TaskState.CANCELLED,
    }
)

LEGAL_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCESS,
            TaskState.RETRY_WAIT,
            TaskState.FAILED,
            TaskState.TIMED_OUT,
            TaskState.CANCELLATION_REQUESTED,
        }
    ),
    TaskState.RETRY_WAIT: frozenset({TaskState.PENDING, TaskState.CANCELLED}),
    TaskState.CANCELLATION_REQUESTED: frozenset(
        {
            TaskState.SUCCESS,
            TaskState.FAILED,
            TaskState.TIMED_OUT,
            TaskState.CANCELLED,
        }
    ),
    TaskState.SUCCESS: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.TIMED_OUT: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


class InvalidStateTransition(RuntimeError):
    """An internal invariant was violated; never caused by user input."""


def check_transition(current: TaskState, new: TaskState) -> None:
    if new not in LEGAL_TRANSITIONS[current]:
        raise InvalidStateTransition(f"illegal transition: {current} -> {new}")
