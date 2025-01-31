#!/bin/bash

#SBATCH --job-name=polars_benchmark
#SBATCH --output=benchmark_%j.txt       # Output log
#SBATCH --error=benchmark_%j.err        # Error log
#SBATCH --nodes=1
#SBATCH --cpus-per-task=52
#SBATCH --ntasks=1
#SBATCH --mem=1024GB
#SBATCH --partition=HMEM
#SBATCH --time=10:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ant***@***.**

# Load Conda environment
source ~/.bashrc.conda3
conda activate polars


POLARS_MAX_THREADS=1 python benchmark.py
POLARS_MAX_THREADS=4 python benchmark.py
POLARS_MAX_THREADS=8 python benchmark.py
POLARS_MAX_THREADS=16 python benchmark.py
POLARS_MAX_THREADS=32 python benchmark.py
POLARS_MAX_THREADS=52 python benchmark.py
