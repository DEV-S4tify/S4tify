import os
from datetime import datetime

import requests
import snowflake.connector
from plugins.spark_snowflake_conn import *
from pyspark.sql.functions import (col, current_date, explode, lit,
                                   regexp_replace, split, when)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from airflow.models import Variable

LAST_FM_API_KEY = Variable.get("LAST_FM_API_KEY")

BUCKET_NAME = "de5-s4tify"
OBJECT_NAME = "raw_data"

TODAY = datetime.now().strftime("%Y-%m-%d")


def load():

    # 테이블 있는지 확인하는 sql
    sql = """
        CREATE TABLE IF NOT EXISTS artist_info_globalTop50(
            artist_id VARCHAR(100),
            rank INT,
            title VARCHAR(100),
            artist VARCHAR(100),
            artist_name VARCHAR(100),
            artist_genre ARRAY,
            date_time DATE
    )
    """

    create_snowflake_table(sql)

    transform_df = transformation()
    transform_df.show()

    # Null 값이 있는 행 출력
    # transform_df.filter(col("title") == "Sweet Dreams (feat. Miguel)").show(truncate=False)

    write_snowflake_spark_dataframe("artist_info_globalTop50", transform_df)


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
    artist_info_df = extract(
        "spotify_artist_info",
        artist_info_schema).dropDuplicates(
        ["artist_id"])
    global_top50_df = extract("spotify_crawling_data", global_top50_schema)

    global_top50_df = global_top50_df.withColumn(
        "artist_id", explode("artist_id"))

    artist_info_top50_df = global_top50_df.join(
        artist_info_df, on="artist_id", how="outer"
    )

    artist_info_top50_df = artist_info_top50_df.withColumn(
        "date_time", current_date())

    return artist_info_top50_df


def extract(file_name, schema):

    spark = create_spark_session("artist_global_table")

    df = spark.read.csv(
        f"s3a://{BUCKET_NAME}/{OBJECT_NAME}/{file_name}_{TODAY}.csv",
        header=True,
        schema=schema,
    )

    if file_name == "spotify_crawling_data":
        df = (
            df.withColumn(
                "artist", split(
                    regexp_replace(
                        col("artist"), r"[\[\]']", ""), ", ")) .withColumn(
                "artist_id", split(
                    regexp_replace(
                        col("artist_id"), r"[\[\]']", ""), ", "), ) .fillna(
                {
                    "title": ""}) .withColumn(
                "artist", when(
                    col("artist").isNull(), lit(
                        [""])).otherwise(
                    col("artist")), ))
    if file_name == "spotify_artist_info":
        df = df.withColumn(
            "artist_genre", regexp_replace(df["artist_genre"], "[\\[\\]']", "")
        )  # 불필요한 문자 제거
        df = df.withColumn(
            "artist_genre", split(df["artist_genre"], ", ")
        )  # 쉼표 기준으로 배열 변환
        df = df.withColumnRenamed("artist", "artist_name")

    return df


if __name__ == "__main__":
    load()
