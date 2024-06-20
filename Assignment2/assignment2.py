"""
De opdracht moet "assignment2.py" heten en onder een mapje "Assignment2" staan in de "BDC"
repo die je al hebt aangemaakt voor de vorige opdracht. (Let op de HoofdLeTTeRs!).

De input en output is hetzelfde als voor assignment1; 1 of meerdere fastq files als input,
en de output is standaard de gemiddelde PHRED score per positie in de reads van elk fastq file
afzonderlijk, in CSV format. Dit gedeelte kun je geheel hergebruiken! In het bijzonder,
de functie die je als target gebruikte in assignment 1 kan nu als target dienen voor je Peon's.

De opdracht vraagt je nu om je script zo aan te passen dat deze in server modus of in client
modus loopt.

De server die splitst de data op in chunks, gegeven door de "–chunks" parameter. Zoals besproken
in de klas tijdens het college beinvloedt deze chunk grootte de performance behoorlijk! De server
beheert ook de Manager() die de gedeelde Queue's over het netwerk toegankelijk maakt.

De client is een werker die een connectie maakt met de server, en in de Queue kijkt of er werk te
doen is. Elke client kan ook nog de verwerkingsfunctie op aparte cores draaien (de "Peon's" in
het college-voorbeeld); dit wordt gegeven door dezelfde "-n" optie als in de eerste opdracht.

Als een client klaar is met het verwerken van een chunk, dan stopt hij het resultaat in de results
Queue van de server (dit mag een lijst zijn, of een dictionary; wat handiger is voor jou). Dan
wacht de client of er weer chunks verschijnen in de jobs Queue.

De server wacht tot alle chunks die uitgedeeld zijn ook weer terug zijn gekomen, en stopt dan een
"POISONPILL" boodschap in de jobs Queue.

Clients die dit ontvangen stoppen de boodschap terug en schakelen zichzelf uit. De server
schrijft de resultaten in CSV naar de standaard output of in files.

Nogmaals, je start 1 en hetzelfde script hetzij in server modus ("-s" optie) hetzij in client
modus ("-c" optie)!

Het script aanroepen gaat als volgt voor de server:
python3 assignment2.py -s rnaseqfile.fastq --host <een workstation> --port <een poort> --chunks

<een getal> En voor de clients als volgt:
python3 assignment2.py -c --host <diezelfde host als voor server> --port <diezelfde poort als
voor server>
"""

import argparse as ap
import csv
import itertools
import multiprocessing as mp
import queue
import sys
import time
from itertools import zip_longest
from multiprocessing.managers import BaseManager
import numpy

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
PORTNUM = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'


def argparser():
    """
        Parses command line arguments for the script.

        - `-n` (required): Specifies the number of cores to use.
        - `-o` (optional): Specifies the output CSV file to store the results. If not
          provided, the output will be directed to the terminal (STDOUT).
        - `fastq_files` (required): One or more Fastq Format files to be processed.

        Returns:
            args: The parsed arguments as an object

        Example server mode:
            python3 assignment2.py -s rnaseqfile.fastq --host <een workstation>
            --port <een poort> --chunks

        Example client mode:
             python3 assignment2.py -c --host <diezelfde host als voor server> --port
            <diezelfde poort als voor server>
    """
    arg_parser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over "
                    "the network.")
    mode = arg_parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true",
                      help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true",
                      help="Run the program in Client mode; see extra options needed below")

    server_args = arg_parser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile",
                             type=ap.FileType('w', encoding='UTF-8'),
                             required=False,
                             help="CSV file om de output in op te slaan. Default is output naar "
                                  "terminal STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'),
                             nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int,
                             help="Aantal chunks of de fastq file(s) in op te splitsen.")

    client_args = arg_parser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                             dest="n", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")
    client_args.add_argument("--host", action="store", type=str,
                             help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int,
                             help="The port on which the Server is listening")

    args = arg_parser.parse_args()

    return args


def make_server_manager(port, authkey):
    """
    Create a manager for the server, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.

    Args:
        port: port number of server
        authkey: authentication key
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        """
        A manager class for the server to handle shared queues. This class is used within the
        make_server_manager function to create and manage shared queues for communication between
        server and client processes.

        This class extends the BaseManager class and provides methods to manage shared queues for
        the server.
        """

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print(f'Server started at port {port}')
    return manager


def runserver(func, data, outfile, fastqfiles):
    """
    Execute tasks on the server and manage the output to CSV files.

    Args:
        func: The function to be applied to each chunk of data.
        data: The data to be processed, divided into chunks.
        outfile: The CSV file to write the output to.
        fastqfiles: A list of fastq files.

    """
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for idx, chunk_list in enumerate(data):
        for chunk in chunk_list:
            shared_job_q.put({'func': func, 'arg': chunk, 'file_idx': idx})

    time.sleep(2)
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!")
            if len(results) == sum(len(chunks) for chunks in data):
                print("Got all results!")
                break

        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)

    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()

    # get mean
    all_decoded_scores = [[] for _ in range(len(fastqfiles))]
    for job in results:
        # job is a dict
        decoded_score = job["result"]
        file_idx = job["file_idx"]
        all_decoded_scores[file_idx].extend(decoded_score)

    # Calculate mean scores and write out results for each fastqfile
    for file_idx, decoded_scores in enumerate(all_decoded_scores):
        mean_score_list = get_mean_score(decoded_scores)
        fastqfile_name = fastqfiles[file_idx].name
        write_outfile(outfile, mean_score_list, fastqfile_name)


def make_client_manager(ip_address, port, authkey):
    """
    Create a manager for a client. This manager connects to a server on the
    given address and exposes the get_job_q and get_result_q methods for
    accessing the shared queues from the server.
    Return a manager object.

    Args:
        ip_address: The IP address of the server.
        port: The port number on which the server is listening.
        authkey: The authentication key used to connect to the server.
    """

    class ServerQueueManager(BaseManager):
        """
        A manager class for the server to handle shared queues. This class is used within the
        make_client_manager function to create and manage shared queues for communication between
        server and client processes.

        This class extends the BaseManager class and provides methods to manage shared queues for
        the server.
        """

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip_address, port), authkey=authkey)
    manager.connect()

    print(f'Client connected to {ip_address}:{port}')
    return manager


def runclient(num_processes):
    """
    Starts a client process that connects to the server and runs multiple worker processes to
    execute tasks received from the server concurrently.

    Args:
        num_processes:The number of worker processes to start.

    Returns:

    """
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    Starts multiple worker processes to execute tasks from the job queue concurrently.

    Args:
        job_q: The job queue from which workers retrieve tasks.
        result_q: The result queue where workers put processed results.
        num_processes: The number of worker processes to start.
    """

    processes = []
    for n_process in range(num_processes):
        tem_p = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(tem_p)
        tem_p.start()
    print(f"Started {len(processes)} workers!")
    for tem_p in processes:
        tem_p.join()


def peon(job_q, result_q):
    """
    A worker function that retrieves jobs from the job queue, processes them, and puts the
    results into the result queue.

        Args:
            job_q: The job queue from which the worker retrieves tasks.
            result_q: The result queue where the worker puts processed results.
    """

    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return

            try:
                result = job['func'](job['arg'])
                print(f"Peon {my_name} Workwork on {job['arg']}!")
                result_q.put({'file_idx': job['file_idx'], 'result': result})
            except NameError:
                print("Can't find yer fun Bob!")
                result_q.put({'file_idx': job['file_idx'], 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)


def get_size_chunks(n_procceses, file_line_count):
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
        decoded_scores.append(decoded_score)
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


def write_outfile(csvfile, mean_score_list, fastqfile_name):
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
    if args.s:
        print("start server mode")
        data = []
        outfile = args.csvfile
        fastqfiles = args.fastq_files  # Assuming this is a list of fastq files

        for fastqfile in fastqfiles:
            # Process each file and append the results to data
            quality_score_lines_list, file_length = get_quality_score_lines(fastqfile)
            if args.chunks is None:
                args.chunks = mp.cpu_count()
            job_data = divide_chunks(quality_score_lines_list, args.chunks)
            data.append(job_data)

        server = mp.Process(target=runserver, args=(process_wrapper, data, outfile, fastqfiles))
        server.start()
        time.sleep(1)
        server.join()
    elif args.c:
        print("start client mode")
        client = mp.Process(target=runclient, args=(args.n or 1,))
        client.start()
        client.join()


if __name__ == "__main__":
    sys.exit(main())
