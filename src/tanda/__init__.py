"""tanda — a small execution layer over ThreadPoolExecutor.

Everything under a leading underscore is internal and may change without
notice.
"""

from tanda.cancellation import Cancellation
from tanda.exceptions import (
    Cancelled,
    OverallTimeout,
    ShutdownTimeout,
    TaskError,
    TaskTimeout,
)
from tanda.pool import Pool
from tanda.progress import (
    CallbackProgress,
    DefaultProgress,
    NullProgress,
    ProgressReporter,
)
from tanda.results import BatchResult, TaskFailure, TaskResult
from tanda.retry import RetryPolicy

# Safe to import unconditionally: the tqdm import lives inside the class's
# constructor, so importing tanda never requires the optional dependency.
from tanda.tqdm_progress import TqdmProgress

__version__ = "0.1.0.dev0"

__all__ = [
    "BatchResult",
    "CallbackProgress",
    "DefaultProgress",
    "NullProgress",
    "ProgressReporter",
    "Cancellation",
    "Cancelled",
    "OverallTimeout",
    "Pool",
    "RetryPolicy",
    "ShutdownTimeout",
    "TaskError",
    "TaskFailure",
    "TaskResult",
    "TaskTimeout",
    "TqdmProgress",
]
