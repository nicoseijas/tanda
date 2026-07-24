"""tanda — a small execution layer over ThreadPoolExecutor.

Everything under a leading underscore is internal and may change without
notice.
"""

from tanda.cancellation import Cancellation
from tanda.exceptions import Cancelled, OverallTimeout, TaskError, TaskTimeout
from tanda.pool import Pool
from tanda.results import BatchResult, TaskFailure, TaskResult
from tanda.retry import RetryPolicy

__version__ = "0.1.0.dev0"

__all__ = [
    "BatchResult",
    "Cancellation",
    "Cancelled",
    "OverallTimeout",
    "Pool",
    "RetryPolicy",
    "TaskError",
    "TaskFailure",
    "TaskResult",
    "TaskTimeout",
]
