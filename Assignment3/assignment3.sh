#!/bin/bash

# Set input file
input_file="/homes/aavanderleij/BDC_2024/testFiles/subset.fastq"

# Create a temporary directory for storing intermediate results
temp_dir=$(mktemp -d)

# Extract every 4th line and split the file into smaller chunks
awk 'NR % 4 == 0' "$input_file" | split -l 1000 - "$temp_dir/split_"

# Process each split file in parallel
find "$temp_dir" -type f -name 'split_*' | parallel -j 20 --bar "cat {} | python3 assignment3.py -d > {}.out"

# Combine all results
cat "$temp_dir"/*.out > "$temp_dir/combined_results"

# Calculate the mean scores
python3 assignment3.py -m < "$temp_dir/combined_results"

# Clean up temporary directory
rm -rf "$temp_dir"