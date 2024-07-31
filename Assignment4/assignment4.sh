#!/bin/bash
#SBATCH --job-name=assingment4   # name of job
#SBATCH --nodes=2                # node count
#SBATCH --ntasks=5               # total number of tasks
#SBATCH --cpus-per-task=1        # cpu-cores per task
#SBATCH --mem-per-cpu=1G         # memory per cpu-core
#SBATCH --time=01:00:00          # total run time limit (HH:MM:SS)
#SABTCH --output=/homes/aavanderleij/BDC_2024/Assignment4/slurm-%j.out

# load conda env
source /commons/conda/conda_load.sh

mpirun python assignment4.py "/homes/aavanderleij/BDC_2024/testFiles/subset.fastq" -o "test.csv"