#!/bin/bash

# set input file
input_file="/homes/aavanderleij/BDC_2024/testFiles/5reads.fastq"

# decode quality score of fastq file in parallel
awk 'NR % 4 == 0' "$input_file" | parallel -j 20  "python3 assignment3.py -d -l {}" | python3 assignment3.py -m

