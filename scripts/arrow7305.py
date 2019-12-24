import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

import gc
import psutil


PROC = psutil.Process()


def get_rss():
    return PROC.memory_info().rss


def print_rss():
    print(f"RSS: {get_rss()}")


RSS_TELEMETRY = []


class memory_use:

    def __init__(self):
        self.start_use = pa.total_allocated_bytes()
        self.start_rss = get_rss()
        self.pool = pa.default_memory_pool()
        self.start_peak_use = self.pool.max_memory()

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        gc.collect()
        rss = get_rss()
        print("RSS: {}, change: {}"
              .format(rss, rss - self.start_rss))
        RSS_TELEMETRY.append(rss)
        # print("Change in Arrow allocations: {}"
        #       .format(pa.total_allocated_bytes() - self.start_use))
        # print("Change in peak use: {}"
        #       .format(self.pool.max_memory() - self.start_peak_use))


def log_(msg):
    print(f"{msg} RSS: {get_rss()}")


path = '/home/wesm/Downloads/big.snappy.parquet'

CSV_PATH = '/home/wesm/Downloads/50mb.csv.gz'

pa.jemalloc_set_decay_ms(0)

log_("Starting")

for i in range(10):
    df = pd.read_csv(CSV_PATH)
    log_("Read CSV")

    df.to_parquet('out.parquet')
    log_("Wrote Parquet")

    time.sleep(1)
    log_(f"Waited 1 second")

    # for i in range(10):
    #     time.sleep(0.1)
    #     elapsed = "%.2f" % (0.1 * (i + 1))
    #     log_(f"{elapsed} seconds elapsed")


for i in range(10):
    time.sleep(1)
    log_(f"Waited 1 second")
