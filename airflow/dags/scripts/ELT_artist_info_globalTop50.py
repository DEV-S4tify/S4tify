import os
from datetime import datetime

import requests
import snowflake.connector
from plugins.spark_snowflake_conn import *
from pyspark.sql.functions import (col, concat_ws, current_date, explode, lit,
                                   regexp_replace, split)
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField,
                               StructType)

BUCKET_NAME = "de5-s4tify"
OBJECT_NAME = "raw_data"

TODAY = datetime.now().strftime("%Y-%m-%d")


def load():

    sql = """
        CREATE TABLE IF NOT EXISTS artist_info_globalTop50(
            artist_id VARCHAR(100),
            rank INT,
            title VARCHAR(100),
            artist VARCHAR(100),
            artist_name VARCHAR(100),
            artist_genre ARRAY,
            date_time DATE,
            song_genre ARRAY
    )
    """

    create_snowflake_table(sql)

    transform_df = transformation()

    write_spark_csv(f"join_artsit_info_chart_{TODAY}", transform_df)


def transformation():

    artist_info_schema = StructType(
        [
            StructField("artist", StringType(), True),
            StructField("artist_id", StringType(), True),
            StructField("artist_genre", StringType(), True),
        ]
    )

    global_top50_schema = StructType(
        [
            StructField("rank", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("artist", StringType(), True),
            StructField("artist_id", StringType(), True),
        ]
    )

    # 데이터 읽고 중복 제거
    artist_info_df = extract("artist_info", artist_info_schema).dropDuplicates(
        ["artist_id"]
    )
    global_top50_df = extract("crawling_data", global_top50_schema)

    global_top50_df = global_top50_df.withColumn(
        "artist_id", explode("artist_id"))

    artist_info_top50_df = global_top50_df.join(
        artist_info_df, on="artist_id", how="outer"
    )

    artist_info_top50_df = artist_info_top50_df.withColumn(
        "date_time", current_date())

    artist_info_top50_df = artist_info_top50_df.withColumn(
        "artist", concat_ws(",", col("artist"))
    )
    artist_info_top50_df = artist_info_top50_df.withColumn(
        "artist_genre", concat_ws(",", col("artist_genre"))
    )

    return artist_info_top50_df


def extract(file_name, schema):

    spark = create_spark_session("artist_global_table")

    df = spark.read.csv(
        f"s3a://{BUCKET_NAME}/{OBJECT_NAME}/{file_name}/spotify_{file_name}_{TODAY}.csv",
        header=True,
        schema=schema,
    )

    if file_name == "crawling_data":
        df = df.withColumn(
            "artist",
            split(
                regexp_replace(
                    col("artist"),
                    r"[\[\]']",
                    ""),
                ", ")).withColumn(
            "artist_id",
            split(
                regexp_replace(
                    col("artist_id"),
                    r"[\[\]']",
                    ""),
                ", "),
        )
    if file_name == "artist_info":
        df = df.withColumn(
            "artist_genre", regexp_replace(df["artist_genre"], "[\\[\\]']", "")
        )  # 불필요한 문자 제거
        df = df.withColumn(
            "artist_genre", split(df["artist_genre"], ", ")
        )  # 쉼표 기준으로 배열 변환
        df = df.withColumnRenamed("artist", "artist_name")

    df.show()

    return df


if __name__ == "__main__":
    load()
