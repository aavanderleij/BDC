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

# constands
sqlite_file = "data/watlas-2023.sqlite"
tag_file = 'data/tags_watlas_all.xlsx'


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


def get_sql_data(sqlite_file_path=sqlite_file):
    # get uri
    sqlite_file_path = Path(sqlite_file_path).absolute()

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

    # add short tag number column
    watlas_df = watlas_df.with_columns([
        pl.col("TAG"),
        pl.col("TAG").cast(pl.String).str.slice(-4).cast(pl.Int64).alias("tag")])

    return watlas_df


def read_tag_file(tag_csv_path=tag_file):
    """
    Read a xlsx file and return a polars dataframe
    Returns:
    """

    tag_csv_path = Path(tag_csv_path)

    if tag_csv_path.exists():

        tags_df = pl.read_excel(tag_csv_path)
        # remove any spaces in species (bar-tailed godwit -> bar-tailed_godwit)
        tags_df = tags_df.with_columns(pl.col("species").str.replace(" ", "_").alias("species"))
    else:
        sys.exit(f"tag file not found: {tag_csv_path.absolute()}. Check config file")

    return tags_df


def join_tags(watlas_df, tags_df=read_tag_file()):
    """
    join a big dataframe with a smaller meta data frame to get species column
    Returns:

    """
    watlas_df = watlas_df.join(tags_df.select(["tag", "species"]), on="tag", how="left")

    return watlas_df


def main():
    # get data + query warmup
    watlas_df = get_sql_data()

    # benchmark reading SQLite
    time_get_data = timeit(lambda: get_sql_data(), number=1)
    print(f"Time getting data from file: {time_get_data}")

    # print size
    print(f"shape of polars df: {watlas_df.shape}")
    print(f'size of df: {watlas_df.estimated_size("mb")} mb')

    # benchmark reading SQLite
    time_join = timeit(lambda: join_tags(watlas_df=watlas_df), number=1)
    print(f"Time getting data from joining tags: {time_join}")

    watlas_df = join_tags(watlas_df)

    return 0


if __name__ == "__main__":
    sys.exit(main())
