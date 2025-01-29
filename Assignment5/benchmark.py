# imports
import os
import sys

# max_threads = int(sys.argv[1])
# # set max treads
# os.environ["POLARS_MAX_THREADS"] = str(max_threads)
import polars as pl

print(f'Using polars version {pl.__version__}')
print(f"Amount of threads used by polars: {pl.threadpool_size()}")
from datetime import datetime, timezone
from pathlib import Path
from timeit import timeit


def get_subset_query():
    """
    set time for subseting data
    """

    # subset for testing
    tracking_time_start = datetime.strptime("2023-09-01 00:00:00", '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    tracking_time_end = datetime.strptime("2023-09-01 23:00:00", '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

    # format time to unix timestamp in milliseconds
    tracking_time_start = int(tracking_time_start.timestamp() * 1000)
    tracking_time_end = int(tracking_time_end.timestamp() * 1000)

    # # set query with start and end
    query = f"""
                SELECT TAG, TIME, X, Y, NBS, VARX, VARY, COVXY
                FROM LOCALIZATIONS
                  WHERE TIME > {tracking_time_start}
                  AND TIME < {tracking_time_end}
                ORDER BY TIME ASC;
                """

    return query


def get_sql_data():
    # get uri
    sqlite_file_path = Path("data/watlas-2023.sqlite").absolute()

    db_uri = f"sqlite:///{sqlite_file_path.as_posix()}"

    # print("set_querry")
    #
    # print(f"polars thread pool size {pl.thread_pool_size()}")

    query = get_subset_query()

    # query with all data form sqlite
    # query = f"""
    #         SELECT TAG, TIME, X, Y, NBS, VARX, VARY, COVXY
    #         FROM LOCALIZATIONS
    #         ORDER BY TIME ASC;
    #         """
    # run query
    watlas_df = pl.read_database_uri(query, db_uri)
    # get shape
    # print(watlas_df.head())
    # print(watlas_df.shape)
    # print(watlas_df.estimated_size("mb"))





def main():
    time_get_data = timeit(lambda: get_sql_data(), number=5)
    print(f"Time getting data from file: {time_get_data}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
