"""
Deze opdracht gaat, over het verwerken van Fastq files om hun gemiddelde PHRED score te bepalen.
Wat deze keer veranderd is het gebruik van MPI. Met de mpi4py module kun je het zelf opzetten van
de afzonderlijke processen vermijden.Daarnaast kun je gebruik maken van de MPI communicatiekanalen
om het werk te verdelen; deze hoef je niet expliciet op te zetten, ze zijn er gewoon. Je moet
dus je code refactoren om van mpi4py gebruik te maken, en miet meer van multiprocessing.
Verder gebruik je SLURM om de aanvankelijke processen op te starten; dit hoeft (mag!) dus niet
meer met de hand uit een terminal.

Student: Antsje van der Leij
student number: 343279
"""

import csv
import itertools
import sys
from itertools import zip_longest
from itertools import chain
import argparse as ap
from mpi4py import MPI
import numpy

def argparser():
    """
        Parses command line arguments for the script.

        - `-o` (optional): Specifies the output CSV file to store the results. If not
          provided, the output will be directed to the terminal (STDOUT).
        - `fastq_files` (required): One or more Fastq Format files to be processed.

        Returns:
            args: The parsed arguments as an object

        Example:

    """
    arg_parser = ap.ArgumentParser(description="Script voor Opdracht 4 van Big Data Computing")
    arg_parser.add_argument("-o", action="store", dest="csvfile",
                            type=ap.FileType('w', encoding='UTF-8'),
                            required=False,
                            help="CSV file om de output in op te slaan. Default is output naar "
                                 "terminal STDOUT")
    arg_parser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                            help="Minstens 1 Illumina Fastq Format file om te verwerken")
    args = arg_parser.parse_args()

    return args


def get_size_chunks(n_procceses, file_line_count):
    """
    calculated the chunk size of fasta file based on total file count and amount of processes
    Args:
        n_processes int: amount of processes
        file_line_count int: total amount of lines in a fastq file

    Returns:
        chunk_size * 4: amount of line for a single process/chunk

    """
    # get amount of reads
    read_amount = file_line_count // 4
    # get the size of chunks
    chunk_size = read_amount // n_procceses + (read_amount % n_procceses)
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
    with open(fastq_file, "r", encoding='utf-8') as file:
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
    encoded_quality_score_list = []
    for count, line in enumerate(fastq_file):

        # count + 1 to make the list start with 1 instead of 0
        # get every 4th line in the FastQ file
        if (count + 1) % 4 == 0:
            # remove newline (\n)
            line = line.rstrip('\n')
            # add all lines to a list
            encoded_quality_score_list.append(line)

    return encoded_quality_score_list, count


def decode_fastq_quality_score(encoded_quality_score):
    """
    decode the quality sore from the fastQ file from ascii to numerical score
    Q-score is the ascii code - 33

    Args:
        chunk of fastq quality score line from a fastq file
    """
    decoded_scores = []
    for line in encoded_quality_score:
        decoded_score = [ord(ascii_chr) - 33 for ascii_chr in line]
        decoded_scores.extend(decoded_score)
    return decoded_scores


def process_wrapper(chunk):
    """
    decode a chunk (list) of fastq result lines and return a chunk (list) of results
    """
    return decode_fastq_quality_score(chunk)


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


def write_outfile(csvfile, mean_score_list, fastqfile_name, multi_file):
    """
    Write the output to a csv file or print to terminal if no file is given.

    Args:
        csvfile: the file name of output file
        mean_score_list: list of mean pred scores
        fastqfile_name: name of fastqfile to keep track of mutiple files

    Returns:

    """
    print("write outfile...")
    base_num = list(range(1, len(mean_score_list) + 1))
    # add base number by value
    final_list = list(zip(base_num, mean_score_list))
    if csvfile is None:
        # stdout
        writer = csv.writer(sys.stdout)
        writer.writerows(final_list)
    else:
        # write into csv file
        with open(csvfile.name, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # Add fastqfile name to the CSV if more then one file
            if multi_file:
                writer.writerow([fastqfile_name])  # Add fastqfile name to the CSV
            writer.writerows(final_list)
    return 0


def divide_chunks(lst, n_chunks):
    """
    divides list into equal chunks

    Args:
        lst: list
        n_chunks: number of chunks wanted
    """
    chunk_size = int(numpy.floor(len(lst) // n_chunks))
    chunk_list = [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    return chunk_list


def main():
    """
    The main function, called if script is called by name
    """
    args = argparser()
    comm = MPI.COMM_WORLD
    comm_size = comm.Get_size()
    my_rank = comm.Get_rank()
    print(f"Hello! this is rank {my_rank} on {MPI.Get_processor_name()}.")
    outfile = args.csvfile
    fastqfiles = args.fastq_files
    for file_idx, fastqfile in enumerate(fastqfiles):

        if my_rank == 0:  # we zijn een controller
            # Process each file and append the quality lines to a list
            quality_score_lines_list, _ = get_quality_score_lines(fastqfile)
            # divide the data into chunks for every worker
            job_data = divide_chunks(quality_score_lines_list, comm_size)
            # scater job data over every worker
            data = comm.scatter(job_data, root=0)


        else:  # we zijn een werker
            data = comm.scatter(None, root=0)

        # decode qquality lines for every process
        decoded_lines = [decode_fastq_quality_score(line) for line in data]

        # Gather the processed data back to the controller
        all_decoded_scores = comm.gather(decoded_lines, root=0)



        if my_rank == 0:
            # check if one fastqfile and set flag
            multi_file_flag = len(fastqfiles) != 1
            # make one list of all output
            all_decoded_scores = chain.from_iterable(all_decoded_scores)
            # Calculate mean scores and write out results for each fastqfile
            mean_score_list = get_mean_score(all_decoded_scores)
            fastqfile_name = fastqfiles[file_idx - 1].name
            write_outfile(outfile, mean_score_list, fastqfile_name, multi_file_flag)


if __name__ == "__main__":
    sys.exit(main())
