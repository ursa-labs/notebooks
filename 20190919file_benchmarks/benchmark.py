# flake8: noqa

import pyarrow.feather as feather
import pandas as pd
import json
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.util.testing import rands
import fastparquet as fp
import gc
import time

print(f"using {pa.cpu_count()} cpu cores")


def generate_files_for_csv(csv_path, base, sep=',', header=None):
    df = pd.read_csv(csv_path, sep=sep, header=header, low_memory=False)
    if header is None:
        df.columns = ['f{}'.format(i) for i in range(len(df.columns))]

    t = (pa.Table.from_pandas(df, preserve_index=False)
         .replace_schema_metadata(None))
    pq.write_table(t, '{}.parquet'.format(base))
    feather.write_feather(df, '{}.feather'.format(base))


def get_timing(f, niter):
    start = time.clock_gettime(time.CLOCK_REALTIME)
    for i in range(niter):
        f()
    result = (time.clock_gettime(time.CLOCK_REALTIME) - start) / niter
    return result


NITER = 5

def bench(info):
    parquet_path = 'data/{}.parquet'.format(info['base'])
    feather_path = 'data/{}.feather'.format(info['base'])

    cases = [
        ('pyarrow.parquet', 'arrow Table',
         lambda: pq.read_table(parquet_path)),
        ('pyarrow.parquet-pandas', 'pandas',
         lambda: (pq.read_table(parquet_path).to_pandas())),
        ('pyarrow.feather', 'pandas',
         lambda: feather.read_feather(feather_path)),
        # ('fastparquet', 'pandas', lambda: pd.read_parquet(parquet_path))
    ]

    results = []
    for name, output_type, f in cases:
        print(name)
        results.append((name, output_type, get_timing(f, NITER)))
    return pd.DataFrame.from_records(results, columns=['expr', 'output_type',
                                                       'mean'])


files = {
    'fanniemae': {
        'base': '2016Q4',
        'source': {
            'path': '2016Q4.txt',
            'sep': '|',
            'header': None
        }
    },
    'nyctaxi': {
        'base': 'yellow_tripdata_2010-01',
        'source': {
            'path': 'yellow_tripdata_2010-01.csv',
            'sep': ',',
            'header': 0
        }
    }
}


def write_files(files):
    for name, info in files.items():
        print(name)
        source = info['source']
        generate_files_for_csv(source['path'], info['base'], sep=source['sep'],
                               header=source['header'])


def run_benchmarks():
    all_results = []
    for name, info in files.items():
        file_results = bench(info)
        file_results['dataset'] = name
        all_results.append(file_results)

    all_results = pd.concat(all_results, ignore_index=True)
    print(all_results)
    all_results.to_csv('py_results.csv')


run_benchmarks()


# for i in range(5):
#     pq.read_table('yellow_tripdata_2010-01.parquet').to_pandas()


# write_files(files)

# ('pyarrow.parquet', 1.5470361709594727)
# ('pyarrow.parquet-pandas', 2.925654172897339)
# ('pyarrow.feather', 1.6384665012359618)
