# flake8: noqa

import pyarrow.feather as feather
import pandas as pd
import json
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.util.testing import rands
import gc
import time


path = '/home/wesm/Downloads/Performance_2016Q4.txt'

def write_files():
    df = pd.read_csv(path, sep='|', header=None, low_memory=False)
    df.columns = ['f{}'.format(i) for i in range(len(df.columns))]

    t = (pa.Table.from_pandas(df, preserve_index=False)
         .replace_schema_metadata(None))
    pq.write_table(t, '2016Q4.parquet')
    feather.write_feather(df, '2016Q4.feather')


def get_timing(f, niter):
    start = time.clock_gettime(time.CLOCK_REALTIME)
    for i in range(niter):
        f()
    result = (time.clock_gettime(time.CLOCK_REALTIME) - start) / niter
    return result


NITER = 5

cases = [
    ('pyarrow.parquet', lambda: pq.read_table('2016Q4.parquet')),
    ('pyarrow.parquet-pandas', lambda: (pq.read_table('2016Q4.parquet')
                                        .to_pandas())),
    ('pyarrow.feather', lambda: feather.read_feather('2016Q4.feather'))
]

def bench():
    for name, f in cases:
        print((name, get_timing(f, NITER)))

bench()

# ('pyarrow.parquet', 1.5470361709594727)
# ('pyarrow.parquet-pandas', 2.925654172897339)
# ('pyarrow.feather', 1.6384665012359618)
