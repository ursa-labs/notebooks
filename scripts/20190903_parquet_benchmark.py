import json
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.util.testing import rands
import gc
import time


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
              .format(pa.total_allocated_bytes() - self.start_use))
        print("Change in peak use: {}"
              .format(self.pool.max_memory() - self.start_peak_use))


def generate_strings(string_size, nunique, length, random_order=True):
    uniques = np.array([rands(string_size) for i in range(nunique)], dtype='O')
    if random_order:
        indices = np.random.randint(0, nunique, size=length).astype('i4')
        return uniques.take(indices)
    else:
        return uniques.repeat(length // nunique)


def generate_dict_strings(string_size, nunique, length, random_order=True):
    uniques = np.array([rands(string_size) for i in range(nunique)], dtype='O')
    if random_order:
        indices = np.random.randint(0, nunique, size=length).astype('i4')
    else:
        indices = np.arange(nunique).astype('i4').repeat(length // nunique)
    return pa.DictionaryArray.from_arrays(indices, uniques)


STRING_SIZE = 32
LENGTH = 3_000_000
NITER = 5


def generate_table(nunique, num_cols=10, random_order=True):
    data = generate_strings(STRING_SIZE, nunique, LENGTH,
                            random_order=random_order)
    return pa.Table.from_arrays([
        pa.array(data) for i in range(num_cols)
    ], names=['f{}'.format(i) for i in range(num_cols)])


def generate_dict_table(nunique, num_cols=10, random_order=True):
    data = generate_dict_strings(STRING_SIZE, nunique, LENGTH,
                                 random_order=random_order)
    return pa.Table.from_arrays([
        data for i in range(num_cols)
    ], names=['f{}'.format(i) for i in range(num_cols)])


def get_timing(f, niter):
    start = time.clock_gettime(time.CLOCK_REALTIME)
    gc.disable()
    for i in range(niter):
        f()
    result = (time.clock_gettime(time.CLOCK_REALTIME) - start) / niter
    gc.enable()
    gc.collect()
    return result


def write_table(t):
    out = pa.BufferOutputStream()
    pq.write_table(t, out)
    return out.getvalue()


def read_table(source):
    return pq.read_table(source)


def get_write_read_results(table, case_name):
    buf = write_table(table)
    results = [({'case': f'write-{case_name}'},
                get_timing(lambda: write_table(table), 1)),
               ({'case': f'read-{case_name}'},
                get_timing(lambda: read_table(buf), NITER)),
               ({'case': f'read-{case_name}-single-thread'},
                get_timing(lambda: pq.read_table(buf, use_threads=False),
                           NITER))]
    for item in results:
        print(item)
    return results


def get_cases(nunique):
    return {
        'dense-random': generate_table(nunique),
        'dense-sequential': generate_table(nunique, random_order=False),
        'dict-random': generate_dict_table(nunique),
        'dict-sequential': generate_dict_table(nunique, random_order=False)
    }


def run_benchmarks():
    results = {}

    nuniques = [1000, 100000]
    # nuniques = [100000]
    for nunique in nuniques:
        nunique_results = []

        cases = get_cases(nunique)
        for case_name, table in cases.items():
            print(case_name, nunique)
            nunique_results.extend(get_write_read_results(table, case_name))

        results[nunique] = nunique_results

    return results


# cases = get_cases(100000)

# buf = write_table(cases['dict-random'])
# with memory_use():
#     result = pq.read_table(buf)


print(json.dumps(run_benchmarks()))
