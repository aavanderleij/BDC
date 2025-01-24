
# imports
import os
import sys
max_threads = int(sys.argv[1])
# set max treads
os.environ["POLARS_MAX_THREADS"] = str(max_threads)
import polars as pl
print(f'Using polars version {pl.__version__}')
print(f"Amount of threads used by polars: {pl.threadpool_size()}")
from datetime import datetime, timezone
from pathlib import Path

import timeit
import numpy as np
import matplotlib.pyplot as plt




# get uri
sqlite_file_path = Path("data/watlas-2023.sqlite").absolute()

db_uri = f"sqlite:///{sqlite_file_path.as_posix()}"

# subset for testing
tracking_time_start = datetime.strptime("2023-09-01 00:00:00", '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
tracking_time_end = datetime.strptime("2023-09-01 23:00:00", '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

# format time to unix timestamp in milliseconds
tracking_time_start = int(tracking_time_start.timestamp() * 1000)
tracking_time_end = int(tracking_time_end.timestamp() * 1000)

print(tracking_time_start, tracking_time_end)

print("set_querry")


print(f"polars thread pool size {pl.thread_pool_size()}")
# # set query with start and end
query = f"""
        SELECT TAG, TIME, X, Y, NBS, VARX, VARY, COVXY
        FROM LOCALIZATIONS
          WHERE TIME > {tracking_time_start}
          AND TIME < {tracking_time_end}
        ORDER BY TIME ASC;
        """

# query with all data form sqlite
# query = f"""
#         SELECT TAG, TIME, X, Y, NBS, VARX, VARY, COVXY
#         FROM LOCALIZATIONS
#           WHERE TIME > {tracking_time_start}
#           AND TIME < {tracking_time_end}
#         ORDER BY TIME ASC;
#         """
# run query
watlas_df = pl.read_database_uri(query, db_uri)
# get shape
print(watlas_df.head())
print(watlas_df.shape)
print(watlas_df.estimated_size("mb"))