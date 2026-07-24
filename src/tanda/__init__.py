"""tanda — a small execution layer over ThreadPoolExecutor.

Everything under a leading underscore is internal and may change without
notice.
"""

from tanda.exceptions import TaskError
from tanda.pool import Pool
from tanda.results import BatchResult, TaskFailure, TaskResult

__version__ = "0.1.0.dev0"

__all__ = ["BatchResult", "Pool", "TaskError", "TaskFailure", "TaskResult"]
