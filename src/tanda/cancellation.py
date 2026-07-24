"""Cooperative cancellation token.

Cancellation has two honest levels (see GUIDELINES.md): tasks that have not
started are genuinely cancelled; running tasks can only be *asked* to stop.
The asking works without any special task-function signature — the caller
creates the token and the function closes over it:

    cancel = Cancellation()

    def process(item):
        for chunk in chunks(item):
            cancel.raise_if_requested()   # cooperative opt-in
            handle(chunk)

    with Pool() as pool:
        pool.map(items, process, cancel=cancel)   # raises Cancelled if
        # another thread calls cancel.request()

A token is one-way: once requested it stays requested, including across
calls that share it.
"""

from __future__ import annotations

import threading

from tanda.exceptions import Cancelled


class Cancellation:
    """Thread-safe, one-way cancellation flag shared between the caller,
    the pool coordinator, and (optionally) the task functions."""

    __slots__ = ("_event",)

    def __init__(self) -> None:
        self._event = threading.Event()

    def request(self) -> None:
        """Request cancellation. Idempotent; callable from any thread."""
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """Block until cancellation is requested (or ``timeout`` elapses).
        For task functions that park while waiting to be told to stop."""
        return self._event.wait(timeout)

    def raise_if_requested(self) -> None:
        """Raise :class:`Cancelled` if cancellation has been requested.
        The checkpoint call for cooperative task functions."""
        if self._event.is_set():
            raise Cancelled("cancellation requested")
