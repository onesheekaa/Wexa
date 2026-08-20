"""Timing + percentile helpers used by every workload in runner.py."""
import statistics
import time
from contextlib import contextmanager


class Timer:
    def __init__(self):
        self.samples_ms = []

    @contextmanager
    def measure(self):
        start = time.perf_counter()
        yield
        self.samples_ms.append((time.perf_counter() - start) * 1000)

    def percentiles(self) -> dict:
        if not self.samples_ms:
            return {"p50_ms": None, "p95_ms": None, "n": 0}
        data = sorted(self.samples_ms)
        return {
            "p50_ms": round(statistics.median(data), 3),
            "p95_ms": round(_percentile(data, 95), 3),
            "min_ms": round(data[0], 3),
            "max_ms": round(data[-1], 3),
            "n": len(data),
        }


def _percentile(sorted_data, pct):
    if len(sorted_data) == 1:
        return sorted_data[0]
    k = (len(sorted_data) - 1) * (pct / 100)
    f, c = int(k), min(int(k) + 1, len(sorted_data) - 1)
    if f == c:
        return sorted_data[f]
    return sorted_data[f] + (sorted_data[c] - sorted_data[f]) * (k - f)
