from datetime import datetime

import requests
import snowflake.connector
from plugins.spark_snowflake_conn import *
from pyspark.sql.functions import (col, count, current_date, desc, explode,
                                   from_json)
from pyspark.sql.types import ArrayType, StringType

TODAY = datetime.today().strftime("%Y-%m-%d")


def load(sql, df, table_name):

    create_snowflake_table(sql)
    write_snowflake_spark_dataframe(table_name, df)


def transformation_artist_genre_count():

    artist_info_table = (
        extract()
        .filter(col("date_time") == TODAY)
        .dropDuplicates(["artist_id"])
        .withColumn("artist_genre", explode(col("artist_genre")))
    )

    artist_genre_count = (
        artist_info_table.groupBy("artist_genre")
        .agg(count("artist_genre").alias("count"))
        .withColumn("date_time", current_date())
    )

    sql = """
        CREATE TABLE IF NOT EXISTS artist_genre_count(
            artist_genre VARCHAR(100),
            genre_count int,
            date_tiem DATE
        )
    """

    load(sql, artist_genre_count, "artist_genre_count")


def transformation_genre_count():

    artist_info_table = (
        extract() .filter(
            col("date_time") == TODAY) .dropDuplicates(
            ["title"]) .withColumn(
                "song_genre",
                from_json(
                    col("song_genre"),
                    ArrayType(
                        StringType()))) .withColumn(
                            "song_genre",
                            explode(
                                col("song_genre"))))

    spotify_genre_count = (
        artist_info_table.groupBy("song_genre")
        .agg(count("song_genre").alias("genre_count"))
        .withColumn("date_time", current_date())
        .orderBy(desc("genre_count"))
    )

    sql = """
    CREATE TABLE IF NOT EXISTS spotify_genre_count(
        song_genre VARCHAR(100),
        genre_count int,
        date_time DATE
    )
    """

    load(sql, spotify_genre_count, "spotify_genre_count")


def extract():

    spark = create_spark_session("chart_genre_count_table")
    table = read_snowflake_spark_dataframe(spark, "artist_info_globalTop50")

    return table


if __name__ == "__main__":
    transformation_genre_count()
    transformation_artist_genre_count()
