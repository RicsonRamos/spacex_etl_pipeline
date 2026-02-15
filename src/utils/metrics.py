import time 
from contextlib import contextmanager

class StepTimer:

    def __init__(self):
        self.metrics = {}

    @contextmanager
    def measure(self, step_name: str):
        start = time.perf_counter()
        yield
        elapsed = round(time.perf_counter() - start, 4)
        self.metrics[step_name] = elapsed

    def total_time(self) -> float:
        return round(sum(self.metrics.values()), 4)

    def throughput(self, records: int) -> float:  # ← ADD self
        total = self.total_time()
        if total == 0:
            return 0.0
        return round(records / total, 2)