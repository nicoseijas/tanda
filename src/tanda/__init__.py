"""tanda — a small execution layer over ThreadPoolExecutor.

Everything under a leading underscore is internal and may change without
notice.
"""

from tanda.pool import Pool

__version__ = "0.1.0.dev0"

__all__ = ["Pool"]
