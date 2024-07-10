#!/usr/bin/env python3

"""
Je moet in je repository deze opdracht in een aparte map genaamd "Assignment1" (let op de
hoofdletters, case-sensitive!)aanleveren. Het script zelf moet "assignment1.py" heten (alweer,
let op de hoofdletters!). Het moet gegeven 1 of meerdere FastQ files met behulp van 1 of meer
CPUs de gemiddelde PHRED scores van alle files per basepositie bepalen. (Als er meerdere FastQ
files aangeleverd worden, dan moet je daarvoor apart de PHRED score berekenen. En als dan ook de
"-o" optie wordt opgegeven, deze apart als "fastqbestand1.fastq.csv" opslaan.) Het dient als
volgt aangeroepen te kunnen worden: python3 assignment1.py -n <aantal_cpus> [OPTIONEEL: -o
<output csv file>] fastabestand1.fastq [fastabestand2.fastq ... fastabestandN.fastq]

"""
import argparse as ap
import csv
import itertools
import sys
from itertools import zip_longest
from multiprocessing import Pool
import numpy


def argparser():
    """
        Parses command line arguments for the script.

        - `-n` (required): Specifies the number of cores to use.
        - `-o` (optional): Specifies the output CSV file to store the results. If not
          provided, the output will be directed to the terminal (STDOUT).
        - `fastq_files` (required): One or more Fastq Format files to be processed.

        Returns:
            args: The parsed arguments as an object

        Example:
            python3 assignment1.py -n <aantal_cpus> [OPTIONEEL: -o <output csv file>]
             fastabestand1.fastq [fastabestand2.fastq fastabestandN.fastq]
        """

    arg_parser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
    arg_parser.add_argument("-n", action="store",
                            dest="n", required=True, type=int,
                            help="Aantal cores om te gebruiken.")
    arg_parser.add_argument("-o", action="store", dest="csvfile",
                            type=ap.FileType('w', encoding='UTF-8'),
                            required=False,
                            help="CSV file om de output in op te slaan. Default is output naar "
                                 "terminal STDOUT")
    arg_parser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                            help="Minstens 1 Illumina Fastq Format file om te verwerken")
    args = arg_parser.parse_args()

    print("arguments received")

    print("fastq files = " + str(args.fastq_files))
    print("number of processes = " + str(args.n))
    print("outfile = " + str(args.csvfile))

    return args


encoded_quality_score_list = []


def get_size_chunks(n_processes, file_line_count):
    """
    calculated the chunk size of fasta file based on total file count and amount of processes
    Args:
        n_processes int: amount of processes
        file_line_count int: total amount of lines in a fastq file

    Returns:
        chunk_size * 4: amount of line for a singel process/chunk

    """
    # get amount of reads
    read_amount = file_line_count // 4
    # get the size of chunks
    chunk_size = read_amount // n_processes + (read_amount % n_processes)
    return chunk_size * 4


def get_part_file(fastq_file, start, chunk_size):
    """
    Returns a chunk of lines from a FASTQ file.

    Args:
        fastq_file (str): The path to the FASTQ file.
        start (int): The starting line number from which to begin.
        chunk_size (int): size of the chunk.

    Returns:
        A single chunk of the fastq file, list of strings
    """

    with open(fastq_file, "r", encoding='UTF-8') as file:
        file_chunk = []
        for line in itertools.islice(file, start, (start + chunk_size)):
            file_chunk.append(line)
    return file_chunk


def get_quality_score_lines(fastq_file):
    """
    gets only the q base call quality scores from a fasta file
    Args:
        fastq_file:

    Returns:
        list contaning the encoded base call quality scores lines

    """
    count = 0
    for count, line in enumerate(fastq_file):

        # count + 1 to make the list start with 1 instead of 0
        # get every 4th line in the FastQ file
        if (count + 1) % 4 == 0:
            # remove newline (\n)
            line = line[:-2]
            # add all lines to a list
            encoded_quality_score_list.append(line)

    return encoded_quality_score_list, count


# change to only one line as input
def decode_fastq_quality_score(encoded_quality_scores):
    """
    translates the base call quality score to a numeric value. Quality scores are Phred +33
    encoded,using ASCII characters to represent the numerical quality scores.

    Args:
        encoded_quality_scores list: Phred +33 encoded base call quality score
    Returns:
        decoded score: numeric base call quality score
    """

    decoded_score = []
    for ascii_chr in encoded_quality_scores:
        numeric_score = ord(ascii_chr) - 33
        decoded_score.append(numeric_score)

    # return only 1 line of input
    return decoded_score


def process_wrapper(n_processes, encoded_score_list):
    """
    get the functions together for one job to go in the pool function
    """
    with Pool(n_processes) as job_pool:
        print("start pools...")
        # put single line
        decoded_score_list = (job_pool.map(decode_fastq_quality_score, encoded_score_list))
    print("jobs done")

    return decoded_score_list


def get_mean_score(decoded_list):
    """
    Calculates the mean quality score for each position across multiple sequences.

    This function takes a list of lists, where each inner list contains quality scores for a
    sequence, and calculates the mean score at each position across all sequences. If sequences
    are of different lengths, positions without scores are treated as containing `None` and are
    ignored in the mean calculation.

    Args:
        decoded_list: (list of list with int): A list where
        each element is a list of quality scores for a sequence.

    Returns:
        list of int: A list of mean quality scores for each position across all sequences.

    """
    print("get mean quality score...")
    sum_list = [numpy.mean(x) for x in (zip_longest(*decoded_list))]

    return sum_list


def write_outfile(args, sum_list, fastqfile):
    """
    Writes the mean quality scores to an output destination.
    Either a specified csv file or printed to the terminal if no file is given

    Args:
        args: list of args from argparser
        sum_list: list of int, A list of mean quality scores for each position across all sequences.
        fastqfile: csv file

    Returns:
        0

    """
    print("write outfile...")
    base_num = list(range(1, len(sum_list) + 1))

    # add base number by value
    final_list = list(zip(base_num, sum_list))

    if args.csvfile is None:
        # stdout

        for row in final_list:
            sys.stdout.write(str(row) + "\n")

    else:

        # write into csv file

        writer = csv.writer(args.csvfile)
        # if only 1 file is given, don't writhe name in outfile
        if len(args.fastq_files) != 1:
            writer.writerow([fastqfile.name])
        writer.writerows(final_list)

    return 0


def main():
    """
    Main function to process FASTQ files and calculate mean quality scores using multi processing.

    Returns:
        0

    """
    args = argparser()

    for fastqfile in args.fastq_files:
        quality_score_lines_list, file_length = get_quality_score_lines(fastqfile)
        get_size_chunks(args.n, file_length)
        print("decode score...")
        decoded_score_list = process_wrapper(args.n, quality_score_lines_list)
        sum_list = get_mean_score(decoded_score_list)
        write_outfile(args, sum_list, fastqfile)
    print("all done!")


if __name__ == "__main__":
    sys.exit(main())
