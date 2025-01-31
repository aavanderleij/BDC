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
# output file
output_file = f"benchmark_results_{pl.thread_pool_size()}.csv"
# header
with open(output_file, "w") as f:
    f.write("test,time,repeat\n")

sqlite_file_path = Path(sqlite_file).absolute()
file_size = sqlite_file_path.stat().st_size

file_size_kb = file_size / 1024
file_size_mb = file_size_kb / 1024
print(f"File size: {file_size_mb:.2f} MB")


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

    # query with all data form sqlite
    query = f"""
            SELECT TAG, TIME, X, Y, NBS, VARX, VARY, COVXY
            FROM LOCALIZATIONS
            ORDER BY TIME ASC;
            """
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


def get_simple_travel_distance(watlas_df):
    """
    Gets the Euclidean distance in meters between consecutive localization in a coordinate.
    Add claculated distance to each as column "distance"
    """

    watlas_df = watlas_df.with_columns(
        (
            ((pl.col("X") - pl.col("X").shift(1)) ** 2
             + (pl.col("Y") - pl.col("Y").shift(1)) ** 2).sqrt()
        ).alias("distance")
    )

    return watlas_df


def get_speed(watlas_df):
    """
    Calculate speed in meters per second for a watlas dataframe.
    Add claculated speed as column "speed"
    """
    # check if distance is already calculated
    if "distance" not in watlas_df.columns:
        watlas_df = get_simple_travel_distance(watlas_df)

    # get distance
    distance = watlas_df["distance"]
    # get the time interval in seconds between rows in the "TIME" column
    time = (watlas_df["TIME"] - watlas_df["TIME"].shift(1)) / 1000
    # calculate speed
    speed = distance / time

    # add speed_in and speed_out to dataframe
    watlas_df = watlas_df.with_columns([
        speed.alias("speed_in"),
        speed.shift(-1).alias("speed_out")])

    return watlas_df


def aggregate_dataframe(watlas_df, interval="15s"):
    """
        Aggregate a polars dataframe containing WATLAS data to the time specified interval.
        This thins the data to only have rows with given intervals.

        Args:
            watlas_df:
            interval (str): the time interval to aggregate (default 15 seconds)

    """
    watlas_df = watlas_df.with_columns(
        # convert unix time from TIME column to human-readable time
        pl.from_epoch(pl.col("TIME"), time_unit="ms").alias("time")
    )

    watlas_df = watlas_df.group_by_dynamic("time", every=interval, group_by="TAG").agg(
        [
            # aggregate columns X, Y and NBS by getting the mean of those values per interval
            # drop COVXY, covariance loses meaning if an average is taken.
            pl.col("*").exclude("VARX", "VARY", "COVXY", "TIME", "tag").exclude(pl.Utf8).mean(),

            # keep first value of string columns
            pl.col(pl.Utf8).first(),
            # keep first value of TIME
            pl.col("TIME").first(),
            # keep "tag" as int
            pl.col("tag").first(),

            # the variance of an average is the sum of variances / sample size square
            (pl.col("VARX").sum() / (pl.col("VARX").count() ** 2)).alias("VARX"),
            (pl.col("VARY").sum() / (pl.col("VARY").count() ** 2)).alias("VARY"),
        ]
    )
    return watlas_df


def smooth_data(watlas_df, moving_window=5):
    """
    Applies a median smooth defined by a rolling window to the X and Y

    Args:
        watlas_df (pl.Datafame):
        moving_window (int): the window size:
    """
    watlas_df = watlas_df.with_columns(
        pl.col("X").alias("X_raw"),  # Keep original values
        pl.col("Y").alias("Y_raw"),  # Keep original values
        # Apply the forward and reverse rolling median on X
        pl.col("X")
        .reverse()
        .rolling_median(window_size=moving_window, min_samples=1)
        .reverse()
        .rolling_median(window_size=moving_window, min_samples=1)
        .alias("X"),
        # Apply the forward and reverse rolling median on Y
        pl.col("Y")
        .reverse()
        .rolling_median(window_size=moving_window, min_samples=1)
        .reverse()
        .rolling_median(window_size=moving_window, min_samples=1)
    )
    return watlas_df


def group_by_tag(watlas_df):
    """
    Group by tag and apply median smooth, caluclate distace, speed and turn angle
    Returns:

    """

    # empty dataframe list
    df_list = []

    # smooth and calculate per tag (these calculations have to be done per individual bird)
    for _, wat_df in watlas_df.group_by("tag"):
        # order by time
        wat_df = wat_df.sort(by="TIME")
        # apply median smooth
        wat_df = smooth_data(wat_df)
        # calculate distance and speed per tag
        wat_df = get_speed(wat_df)
        # calculate turn angle per tag

        df_list.append(wat_df)

    # concat dataframes to single dataframe
    watlas_df = pl.concat(df_list)

    return watlas_df


def speed_and_smooth_by_tag(watlas_df):
    """
    Group by tag and apply median smooth, caluclate distace, speed and turn angle
    Returns:

    """

    # empty dataframe list
    df_list = []

    # smooth and calculate per tag (these calculations have to be done per individual bird)
    for _, wat_df in watlas_df.group_by("tag"):
        # order by time
        wat_df = wat_df.sort(by="TIME")
        # apply median smooth
        wat_df = smooth_data(wat_df)
        # calculate distance and speed per tag
        wat_df = get_speed(wat_df)
        # calculate turn angle per tag

        df_list.append(wat_df)

    # concat dataframes to single dataframe
    watlas_df = pl.concat(df_list)

    return watlas_df


def log_benchmark(test, time, repeats):
    with open(output_file, "a") as f:
        f.write(f"{test},{time},{repeats}\n")


def main():
    # get data + query warmup
    watlas_df = get_sql_data()

    repeats = 10

    # benchmark reading SQLite
    time_get_data = timeit(lambda: get_sql_data(), number=repeats)
    print(f"Time getting data from file: {time_get_data}")
    log_benchmark("read_sql", time_get_data, repeats)

    # print size
    print(f"shape of polars df: {watlas_df.shape}")
    print(f'size of df: {watlas_df.estimated_size("mb")} mb')

    # benchmark join left
    time_join = timeit(lambda: join_tags(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from joining tags: {time_join}")
    log_benchmark("join", time_join, repeats)

    # benchmark calculating simple distance
    time_dist = timeit(lambda: get_simple_travel_distance(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from calculating simple distance: {time_dist}")
    log_benchmark("calculate_dist", time_dist, repeats)

    # benchmark caluclate speed
    time_speed = timeit(lambda: get_speed(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from calculating speed: {time_speed}")
    log_benchmark("calculate_speed", time_speed, repeats)

    # benchmark run median smooth
    time_smooth = timeit(lambda: smooth_data(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from preforming median smooth: {time_smooth}")
    log_benchmark("median_smooth", time_smooth, repeats)

    # benchmark aggregate
    time_aggr = timeit(lambda: aggregate_dataframe(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from aggregating dataframe: {time_aggr}")
    log_benchmark("aggregate", time_aggr, repeats)

    # benchmark group by
    time_group = timeit(lambda: group_by_tag(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from calculating within a group_by: {time_group}")
    log_benchmark("group_by", time_group, repeats)

    # benchmark sort
    shuffled_df = watlas_df.sample(fraction=1, shuffle=True)
    time_sort = timeit(lambda: shuffled_df.sort(by='TIME'), number=repeats)
    print(f"Time getting sorting data: {time_sort}")
    log_benchmark("sort", time_sort, repeats)

    # benchmark multiple operations together
    time_tag = timeit(lambda: speed_and_smooth_by_tag(watlas_df=watlas_df), number=repeats)
    print(f"Time getting data from calculating within a group_by: {time_tag}")
    log_benchmark("multi_test", time_tag, repeats)

    return 0


if __name__ == "__main__":
    sys.exit(main())
