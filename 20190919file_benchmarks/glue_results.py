import pandas as pd

r_results = pd.read_csv('r_results.csv')
r_results = r_results[['expr', 'time', 'dataset']]
r_results['output_type'] = "R data.frame"
r_results['expr'] = 'R ' + r_results['expr']

py_results = pd.read_csv('py_results.csv')
py_results = py_results[['expr', 'output_type', 'mean', 'dataset']]
py_results['time'] = py_results.pop('mean')

results = pd.concat([r_results, py_results], ignore_index=True)
