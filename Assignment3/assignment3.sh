#!/bin/bash

# Set input files
input_files=(
    "/homes/aavanderleij/BDC_2024/testFiles/subset.fastq"
    # add more files here if needed
)
# set output file
output_file="output.csv"

# Clear the output file if it exists
> "$output_file"

# check if more then one input file
if [ ${#input_files[@]} -gt 1 ]; then
    multiple_files=true
else
    multiple_files=false
fi

# Create a temporary directory
temp_dir=$(mktemp -d)

for input_file in "${input_files[@]}"; do
    file_name=$(basename "$input_file")

    # Extract every 4th line and split the file into smaller chunks
    # (parallel got stuck if piped directly into assignment3.py)
    awk 'NR % 4 == 0' "$input_file" | split -l 1000 - "$temp_dir/split_${file_name}_"

    # find and process each split file in parallel
    find "$temp_dir" -type f -name "split_${file_name}_*" | parallel -j 20 "cat {} | python3 assignment3.py -d > {}.out"

    # Combine decoded results
    cat "$temp_dir/split_${file_name}_"*.out > "$temp_dir/decoded_${file_name}"

    # Calculate mean scores
    mean_scores=$(python3 assignment3.py -m < "$temp_dir/decoded_${file_name}")

    # if more then one file
    if $multiple_files; then
      # write file name
      echo "$file_name" >> "$output_file"
    fi

    # write mean score to output file
    echo "$mean_scores" >> "$output_file"

done

# Clean up temporary directory
rm -rf "$temp_dir"
