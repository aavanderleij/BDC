""""
Using the Python "multiprocessing" module, write a parallel implementation of a script that calculates the average
PHRED score per base for all reads in a fastq file.

It should have the following command-line form:

script.py fastqfile.fastq -n number_of_processes [-o outputfile.csv]

The optional last argument is a csv file of the form "base_nr, average_PHRED\n" with the average PHRED value at that
base position for the entire fastq input file.

An example fastq file can be found on the BIN Network at: /commons/Themas/Thema12/HPC/rnaseq.fastq but it should work
on any valid fastq format file (e.g. from NCBI GEO)

NB1: Not all reads need to be the same length!

NB2: Try to write the basic functionality first, calculating average PHRED value, and then expand that functionality
to work in parallel, using multiprocessing.

NB3: Your script needs to accept _exactly_ the options above, _and require no others_
"""
import itertools
import os
import sys
from statistics import mean
from typing import List
from itertools import zip_longest
import argparse
import csv
from multiprocessing import Pool

import numpy as numpy


def argparser():
    parser = argparse.ArgumentParser(description=
                                     "calculates the average PHRED score per base for all reads in a fastq file ")
    parser.add_argument("fastqfile", type=str)
    parser.add_argument("-n", "--number_of_processes", type=int, required=True)
    parser.add_argument("-o", "--outputfile", type=str, required=False, default="outputfile.csv")

    args = parser.parse_args()

    print("argument received")

    print("fastqfile = " + args.fastqfile)
    print("number of processes = " + str(args.number_of_processes))
    print("outfile = " + args.outputfile)

    return args


encoded_quality_score_list = list()


def get_file_length(fastq_file):
    with open(fastq_file) as open_file:
        for count, line in enumerate(open_file):
            pass
    # quick check if lines divede by 4
    if (count + 1) % 4 != 0:
        sys.exit("line count cant be diveded by 4 \n Faulty file?")
    # remember python list starts at 0
    open_file.close()
    return count + 1


def get_size_chunks(n_procceses, file_line_count):
    # get amount of reads
    read_amount = file_line_count // 4
    # get the size of chunks
    chunk_size = read_amount // n_procceses + (read_amount % n_procceses)
    print(chunk_size * 4)
    return chunk_size * 4


def get_part_file(fastq_file, start, chunk_size):
    with open(fastq_file, "r") as file:
        file_chunk = []
        for line in itertools.islice(file, start, (start + chunk_size)):
            file_chunk.append(line)
    return file_chunk


def get_quality_score_lines(fastq_file):
    with open(fastq_file) as open_file:
        print("opening file...")
        print("finding score lines...")
        # read lines
        for count, line in enumerate(open_file):
            # count + 1 to make the list start with 1 instead of 0
            # get every 4th line in the FastQ file
            if (count + 1) % 4 == 0:
                # remove newline (\n)
                line = line[:-2]
                # add all lines to a list
                encoded_quality_score_list.append(line)
    return encoded_quality_score_list


# change to only one line as input
def decode_fastq_quality_score(encoded_quality_score):
    """
    decode the quality sore from the fastQ file from ascii to numerical score
    Q-score is the ascii code - 33
    """

    decoded_score = list()
    for ascii_chr in encoded_quality_score:
        score_number = ord(ascii_chr) - 33
        decoded_score.append(score_number)

    # return only 1 line of input
    return decoded_score


def procces_wrapper(n_processes, encoded_quality_score_list):
    """
    get the functions together for one job to go in the pool function
    """
    job_pool = Pool(n_processes)
    print("start pools...")
    # put single line
    decoded_score_list = (job_pool.map(decode_fastq_quality_score, encoded_quality_score_list))
    job_pool.close()
    print("jobs done")

    return decoded_score_list


def get_mean_score(decoded_list):
    print("get mean...")
    sum_list = [numpy.mean(x) for x in (zip_longest(*decoded_list))]

    return sum_list


def write_outfile(args, sum_list):
    print("write outfile...")
    base_num = list(range(1, len(sum_list) + 1))

    # add base number by value
    final_list = list(zip(base_num, sum_list))

    # write into csv file
    with open(args.outputfile, "w", newline="\n") as file:
        writer = csv.writer(file)
        writer.writerows(final_list)

    return 0


def main():
    args = argparser()

    file_length = get_file_length(args.fastqfile)
    get_size_chunks(args.number_of_processes, file_length)
    quality_score_lines_list = get_quality_score_lines(args.fastqfile)
    print("decode score...")
    decoded_score_list = procces_wrapper(args.number_of_processes, quality_score_lines_list)
    sum_list = get_mean_score(decoded_score_list)
    write_outfile(args, sum_list)
    print("all done!")


if __name__ == "__main__":
    sys.exit(main())
