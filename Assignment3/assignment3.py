#!/usr/bin/env python3

"""
Script for assignment 3 of big data computing
"""
import argparse as ap
import csv
import sys
from itertools import zip_longest
import numpy
import argparse as ap
import ast


def argparser():
    parser = ap.ArgumentParser(description="Process quality scores and compute mean scores.")

    parser.add_argument("-d", "--decode", action="store_true",
                      help="Decode quality scores and print them.")
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
    print("get mean...")
    mean_score_list = [numpy.mean(x) for x in zip_longest(*decoded_list, fillvalue=0)]
    return mean_score_list


def write_outfile(csvfile, mean_score_list):
    """
    Write the output to a csv file or print to terminal if no file is given.

    Args:
        csvfile: the file name of output file
        mean_score_list: list of mean pred scores

    Returns:

    """
    print("write outfile...")
    base_num = list(range(1, len(mean_score_list) + 1))
    # add base number by value
    final_list = list(zip(base_num, mean_score_list))

    # write into csv file
    with open(csvfile, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(final_list)
    return 0


def main():
    # Get args
    args = argparser()
    if args.decode:
        # read input directly form stdin
        for line in sys.stdin:

            # Decode quality scores
            decoded_scores = decode_fastq_quality_score(line)
            print(decoded_scores)

    elif args.mean:
        # Read decoded quality scores from stdin
        decoded_lists = []

        for line in sys.stdin:
            line = line.strip()
            line = line.strip("[]")
            decoded_list = list(map(int, line.split(',')))
            decoded_lists.append(decoded_list)

        # Compute mean scores
        mean_scores = get_mean_score(decoded_lists)
        print(mean_scores)
        write_outfile("output.csv", mean_scores)


if __name__ == "__main__":
    main()
