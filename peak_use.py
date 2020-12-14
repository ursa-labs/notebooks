import pandas as pd
from pandas.util.testing import rands

import pyarrow as pa
import pyarrow.parquet as pq

import gc

GB = 1 << 30

class memory_use:

    def __init__(self):
        self.start_use = pa.total_allocated_bytes()
        self.pool = pa.default_memory_pool()
        self.start_peak_use = self.pool.max_memory()

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        gc.collect()
        print("Change in memory use: {}"
              .format((pa.total_allocated_bytes() - self.start_use) / GB))
        print("Change in peak use: {}"
              .format((self.pool.max_memory() - self.start_peak_use) / GB))


with memory_use():
    table = pq.read_table('/tmp/test.parquet')
