"""Progress reporting — decoupled from execution, no tqdm dependency.

Progress is an *observation* of the workload; it never controls execution.
The unit is the item, not the attempt: a task that fails twice and succeeds
on the third try advances progress by exactly 1 (guaranteed by construction —
the scheduler only surfaces terminal items).

Any object with ``start(total)`` / ``advance(n=1)`` / ``finish()`` works as a
reporter; ``total`` is ``None`` for unsized inputs, which are never
materialized just to count them.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Protocol, TextIO, runtime_checkable


@runtime_checkable
class ProgressReporter(Protocol):
    def start(self, total: int | None) -> None: ...

    def advance(self, n: int = 1) -> None: ...

    def finish(self) -> None: ...


class NullProgress:
    """A reporter that does nothing."""

    def start(self, total: int | None) -> None:
        pass

    def advance(self, n: int = 1) -> None:
        pass

    def finish(self) -> None:
        pass


class CallbackProgress:
    """Invokes ``callback(completed, total)`` on every advance and on finish.

    The hook for wiring tanda's progress into an existing display (GUI,
    logging, a web socket) without subclassing anything.
    """

    def __init__(self, callback: Callable[[int, int | None], None]) -> None:
        self._callback = callback
        self._completed = 0
        self._total: int | None = None
        self._last_delivered: tuple[int, int | None] | None = None

    def start(self, total: int | None) -> None:
        self._completed = 0
        self._total = total
        self._last_delivered = None

    def advance(self, n: int = 1) -> None:
        self._completed += n
        self._deliver()

    def finish(self) -> None:
        # Deliver the final tally only if the last advance didn't already —
        # consumers never see the same (completed, total) twice in a row.
        self._deliver()

    def _deliver(self) -> None:
        state = (self._completed, self._total)
        if state == self._last_delivered:
            return
        self._last_delivered = state
        self._callback(*state)


class DefaultProgress:
    """Terminal progress line on stderr.

    With a known total::

        Processing ██████████░░░░░░░░░░  50%  5/10  12.3 items/s  ETA 0.4s

    Without one (generator input)::

        Processing 12,482 items  840.2 items/s  14.8s

    Live ``\\r`` re-rendering happens only on a TTY (or with ``live=True``);
    on captured/redirected streams only the final summary line is written,
    so CI logs are not flooded. Rendering is throttled to ``min_interval``
    seconds; the finish line always renders.
    """

    _BAR_WIDTH = 20

    def __init__(
        self,
        stream: TextIO | None = None,
        *,
        label: str = "Processing",
        min_interval: float = 0.1,
        live: bool | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._label = label
        self._min_interval = min_interval
        self._clock = clock
        if live is None:
            isatty = getattr(self._stream, "isatty", None)
            live = bool(isatty and isatty())
        self._live = live
        # Block characters crash on streams that can't encode them (e.g. a
        # cp1252-redirected stderr on Windows) — fall back to ASCII there.
        encoding = getattr(self._stream, "encoding", None)
        try:
            "█░".encode(encoding or "ascii")
            self._blocks = ("█", "░")
        except (UnicodeEncodeError, LookupError):
            self._blocks = ("#", "-")
        self._total: int | None = None
        self._completed = 0
        self._started_at = 0.0
        self._last_render = float("-inf")

    def start(self, total: int | None) -> None:
        self._total = total
        self._completed = 0
        self._started_at = self._clock()
        self._last_render = float("-inf")
        if self._live:
            self._render()

    def advance(self, n: int = 1) -> None:
        self._completed += n
        if self._live and self._clock() - self._last_render >= self._min_interval:
            self._render()

    def finish(self) -> None:
        self._render(final=True)

    def _render(self, final: bool = False) -> None:
        elapsed = self._clock() - self._started_at
        rate = self._completed / elapsed if elapsed > 0 else 0.0
        if self._total is None:
            line = (
                f"{self._label} {self._completed:,} items  "
                f"{rate:,.1f} items/s  {elapsed:,.1f}s"
            )
        else:
            fraction = self._completed / self._total if self._total else 1.0
            filled = round(self._BAR_WIDTH * min(fraction, 1.0))
            full_char, empty_char = self._blocks
            bar = full_char * filled + empty_char * (self._BAR_WIDTH - filled)
            line = (
                f"{self._label} {bar} {min(fraction, 1.0):4.0%}  "
                f"{self._completed:,}/{self._total:,}  {rate:,.1f} items/s"
            )
            if final:
                line += f"  elapsed {elapsed:,.1f}s"
            else:
                remaining = max(self._total - self._completed, 0)
                eta = f"{remaining / rate:,.1f}s" if rate > 0 else "?"
                line += f"  ETA {eta}"
        if final:
            prefix = "\r" if self._live else ""
            self._stream.write(f"{prefix}{line}\n")
        else:
            self._stream.write(f"\r{line}")
        self._stream.flush()
        self._last_render = self._clock()
