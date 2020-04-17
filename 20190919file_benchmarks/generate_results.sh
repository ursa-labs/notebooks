#!/bin/bash

# Make sure we're using performance CPU governor
sudo cpufreq-set -g performance

python benchmark.py

OMP_NUM_THREADS=1 Rscript benchmark.R
OMP_NUM_THREADS=4 Rscript benchmark.R

python glue_results.py
