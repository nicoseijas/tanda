"""Run every benchmark scenario and print the full report.

    python benchmarks/run_all.py            # all four scenarios
    python benchmarks/run_all.py --quick    # skip the 1M-item memory run

The output is what goes into BENCHMARKS.md, losses included.
"""

from __future__ import annotations

import sys

import bench_io
import bench_memory
import bench_overhead
import bench_retries

from _harness import print_environment


def main(argv: list[str]) -> None:
    quick = "--quick" in argv
    print_environment()
    bench_overhead.run()
    bench_io.run()
    bench_retries.run()
    if quick:
        print("\nSkipped the memory scenario (--quick).")
    else:
        bench_memory.run()


if __name__ == "__main__":
    main(sys.argv[1:])
