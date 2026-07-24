# Examples

Runnable scripts, no dependencies beyond tanda itself. From the repository
root:

```bash
pip install -e .
python examples/01_progress_basic.py
```

| Script | Shows |
|---|---|
| `01_progress_basic.py` | The zero-config bar: percentage, counts, rate, ETA over a sized workload |
| `02_progress_generator.py` | Unsized (generator) input — count/rate/elapsed, no materialization |
| `03_progress_with_retries.py` | The bar advances per item, never per attempt; per-item attempt counts via collect mode |
| `04_progress_callback.py` | `CallbackProgress` feeding progress into your own logging/display |

The live `\r`-rendered bar only appears on a real terminal; when output is
redirected (CI logs, pipes) tanda writes just the final summary line.
