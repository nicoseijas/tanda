"""Task state machine: the scheduler implements this diagram and nothing else.

See ARCHITECTURE.md "Task state machine".
"""

import pytest

from tanda._states import (
    LEGAL_TRANSITIONS,
    TERMINAL_STATES,
    InvalidStateTransition,
    TaskState,
    check_transition,
)


def test_every_state_has_a_transition_entry():
    assert set(LEGAL_TRANSITIONS) == set(TaskState)


def test_terminal_states_have_no_outgoing_transitions():
    assert TERMINAL_STATES == {
        TaskState.SUCCESS,
        TaskState.FAILED,
        TaskState.TIMED_OUT,
        TaskState.CANCELLED,
    }
    for state in TERMINAL_STATES:
        assert LEGAL_TRANSITIONS[state] == frozenset()


@pytest.mark.parametrize(
    ("current", "new"),
    [
        (TaskState.PENDING, TaskState.RUNNING),
        (TaskState.PENDING, TaskState.CANCELLED),
        (TaskState.RUNNING, TaskState.SUCCESS),
        (TaskState.RUNNING, TaskState.RETRY_WAIT),
        (TaskState.RUNNING, TaskState.FAILED),
        (TaskState.RUNNING, TaskState.TIMED_OUT),
        (TaskState.RUNNING, TaskState.CANCELLATION_REQUESTED),
        (TaskState.RETRY_WAIT, TaskState.PENDING),
        (TaskState.RETRY_WAIT, TaskState.CANCELLED),
        (TaskState.CANCELLATION_REQUESTED, TaskState.SUCCESS),
        (TaskState.CANCELLATION_REQUESTED, TaskState.FAILED),
        (TaskState.CANCELLATION_REQUESTED, TaskState.TIMED_OUT),
        (TaskState.CANCELLATION_REQUESTED, TaskState.CANCELLED),
    ],
)
def test_legal_transitions_pass(current, new):
    check_transition(current, new)


@pytest.mark.parametrize(
    ("current", "new"),
    [
        # Completed work is never re-executed (ARCHITECTURE.md).
        (TaskState.SUCCESS, TaskState.RETRY_WAIT),
        (TaskState.SUCCESS, TaskState.RUNNING),
        (TaskState.SUCCESS, TaskState.PENDING),
        # A task cannot complete without having run.
        (TaskState.PENDING, TaskState.SUCCESS),
        (TaskState.PENDING, TaskState.FAILED),
        # Terminal means terminal.
        (TaskState.CANCELLED, TaskState.PENDING),
        (TaskState.FAILED, TaskState.RUNNING),
        (TaskState.TIMED_OUT, TaskState.RUNNING),
        # Running tasks go back through RETRY_WAIT, never straight to PENDING.
        (TaskState.RUNNING, TaskState.PENDING),
        # Cancellation of a running task is a request, not a fact.
        (TaskState.RUNNING, TaskState.CANCELLED),
    ],
)
def test_illegal_transitions_raise(current, new):
    with pytest.raises(InvalidStateTransition):
        check_transition(current, new)


def test_self_transition_is_illegal():
    for state in TaskState:
        with pytest.raises(InvalidStateTransition):
            check_transition(state, state)
