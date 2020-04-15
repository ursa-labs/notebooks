import os
import pandas as pd


def munge_results(kind='read'):
    pieces = []
    for num_threads in (1, 4, 8):
        expr_rename = {
            'R parquet_unc': 'parquet (UNC)',
            'R parquet_snappy': 'parquet (SNAPPY)',
            'R feather_unc': 'feather (UNC)',
            'R feather_lz4': 'feather (LZ4)',
            'R feather_zstd': 'feather (ZSTD)',
            'R fst_unc': 'fst (UNC)',
            'R fst_50': 'fst (c=50)',
            'pyarrow.parquet': 'parquet (SNAPPY)',
            'pyarrow.feather (UNC)': 'feather (UNC)',
            'pyarrow.feather (LZ4)': 'feather (LZ4)',
            'pyarrow.feather (ZSTD)': 'feather (ZSTD)',
        }

        r_results = pd.read_csv('r_{}_results_{}.csv'.format(kind,
                                                             num_threads))
        r_results = r_results[['expr', 'time', 'dataset']]
        r_results['output_type'] = "R data.frame"
        r_results['expr'] = 'R ' + r_results['expr']
        r_results['time'] /= 1e9
        r_results['nthreads'] = num_threads
        r_results['language'] = 'R'

        r_results.expr = r_results.expr.map(lambda x: expr_rename.get(x, x))

        py_results = pd.read_csv('py_{}_results_{}.csv'.format(kind,
                                                               num_threads))
        py_results = py_results[['expr', 'output_type', 'mean', 'dataset']]
        py_results['time'] = py_results.pop('mean')
        py_results['nthreads'] = num_threads
        py_results['language'] = 'Python'

        py_results.expr = py_results.expr.map(lambda x: expr_rename.get(x, x))

        renamings = {
            'pyarrow.Table': 'arrow Table',
        }

        py_results.output_type = py_results.output_type.map(
            lambda x: renamings.get(x, x))

        pieces.extend([r_results, py_results])
    return pd.concat(pieces, ignore_index=True, sort=False)


read_results = munge_results('read')
read_results.to_csv('all_read_results.csv', index=False)

write_results = munge_results('write')
write_results.to_csv('all_write_results.csv', index=False)


files = [('fanniemae', '2016Q4'),
         ('nyctaxi', 'yellow_tripdata_2010-01')]

cases = [
    ('feather (UNC)', '_uncompressed.feather'),
    ('feather (LZ4)', '_lz4.feather'),
    ('feather (ZSTD)', '_zstd.feather'),
    ('parquet (UNC)', '_uncompressed.parquet'),
    ('parquet (SNAPPY)', '_snappy.parquet'),
    ('fst (UNC)', '_0.fst'),
    ('fst (C=50)', '_50.fst')
]

file_sizes = []


for logical_name, file_base in files:
    for storage, ending in cases:
        full_path = f'{file_base}{ending}'
        size = os.stat(full_path).st_size
        result = (logical_name, storage, size / (1 << 20))
        print(result)
        file_sizes.append(result)

file_sizes = pd.DataFrame.from_records(
    file_sizes, columns=['dataset', 'file_type', 'size'])

file_sizes.to_csv('file_sizes.csv', index=False)
