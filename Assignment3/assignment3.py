#!/usr/bin/env python3

"""
Script for assignment 3 of big data computing

Script decoding for the pred score form fastq files and calculating the mean.
This script was made to work with assignment3.sh

this scrit has two modes
Decode Quality Scores:
   Use the `-d` or `--decode` option to decode quality scores from Fastq files.
   It reads Fastq data from stdin, extracts the fourth line (quality scores),
   decodes these scores, and prints them.

   Example usage:
    python3 assignment3.py -d < fastq_file.fastq

Compute Mean Scores:
   Use the `-m` or `--mean` option to compute mean scores across all decoded quality scores.
   It reads decoded quality scores from stdin, calculates the mean, and prints the result.

   Example usage:
    python3 assignment3.py -m < decoded_scores.txt
"""

import sys
from itertools import zip_longest
import argparse as ap
import numpy


def argparser():
    """
    Argparse function for handling commandline arguments.
    """
    parser = ap.ArgumentParser(description="Process quality scores and compute mean scores.")

    parser.add_argument("-d", "--decode", action="store_true",
                      help="Decode quality scores.")
    parser.add_argument("-m", "--mean", action="store_true",
                      help="Compute mean scores across all quality scores.")

    args = parser.parse_args()

    return args


def decode_fastq_quality_score(encoded_quality_scores):
    """
    Translates the base call quality score to a numeric value. Quality scores are Phred +33
    encoded, using ASCII characters to represent the numerical quality scores.

    Args:
        encoded_quality_scores (str): Phred +33 encoded base call quality score
    Returns:
        list: Numeric base call quality score
    """
    decoded_score = [ord(ascii_chr) - 33 for ascii_chr in encoded_quality_scores.strip()]
    return decoded_score


def get_mean_score(decoded_list):
    """
    Get mean score of lists based on index
    Args:
        decoded_list: list of decoded pred scores from a fastq file

    Returns:
        mean_score_list: list with mean pred scores

    """
    mean_score_list = [numpy.mean(x) for x in zip_longest(*decoded_list, fillvalue=0)]
    return mean_score_list

def line_to_list(line):
    """
    takes line fom the commandline and make them into a python list
    """
    line = line.strip()
    line = line.strip("[]")
    decoded_list = list(map(int, line.split(',')))

    return decoded_list

def main():
    """
    Read arguments from the commandline and start the right process
    """
    # Get args
    args = argparser()

    # check if in decode mode
    if args.decode:
        # read input directly form stdin
        for line in sys.stdin:
            # Decode quality scores
            decoded_scores = decode_fastq_quality_score(line)
            # print scores (for use in bash script)
            print(decoded_scores)

    # check if in mean mode
    elif args.mean:
        # create list for collecting decoded score lists
        decoded_lists = []
        # Read decoded quality scores from stdin
        for line in sys.stdin:
            # make line into list
            decoded_list = line_to_list(line)
            decoded_lists.append(decoded_list)

        # Compute mean scores
        mean_scores = get_mean_score(decoded_lists)
        for i, score in enumerate(mean_scores):
            # print scores (for use in bash script)
            print(i, score)


if __name__ == "__main__":
    main()
