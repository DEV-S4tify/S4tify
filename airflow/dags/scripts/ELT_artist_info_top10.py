from datetime import datetime

import requests
from dags.plugins.spark_snowflake_conn import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, regexp_replace, split, udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

LAST_FM_API_KEY = os.getenv("LAST_FM_API_KEY")

BUCKET_NAME = "de5-s4tify"
OBJECT_NAME = "raw_data"

TODAY = datetime.now().strftime("%Y-%m-%d")


def load():

    # 테이블 있는지 확인하는 sql
    sql = """
        CREATE TABLE IF NOT EXISTS artist_info_top10(
            artist_id VARCHAR(100),
            artist VARCHAR(100),
            artist_genre ARRAY,
            album VARCHAR(100),
            song_id VARCHAR(100),
            title VARCHAR(100),
            date_time DATE,
            song_genre ARRAY
        )
        """

    # 테이블 없으면 생성
    create_snowflake_table(sql)

    transform_df = transformation()

    write_snowflake_spark_dataframe("artist_info_top10", transform_df)


def transformation():

    # 스키마 정의
    artist_info_schema = StructType(
        [
            StructField("artist", StringType(), True),
            StructField("artist_id", StringType(), True),
            StructField("artist_genre", StringType(), True),
        ]
    )

    artist_top10_schema = StructType(
        [
            StructField("album", StringType(), True),
            StructField("artist_id", StringType(), True),
            StructField("song_id", StringType(), True),
            StructField("title", StringType(), True),
        ]
    )

    # 데이터 읽어오고 중복 제거
    artist_top10_df = extract(
        "spotify_artist_top10", artist_top10_schema
    ).dropDuplicates(["song_id"])
    artist_info_df = extract(
        "spotify_artist_info",
        artist_info_schema).dropDuplicates(
        ["artist_id"])

    artist_info_top10_df = artist_info_df.join(
        artist_top10_df, on="artist_id", how="outer"
    )

    # 날짜 데이터 추가
    artist_info_top10_df = artist_info_top10_df.withColumn(
        "date_time", current_date())

    # 노래 장르 데이터 추가
    artist_info_top10_df = artist_info_top10_df.withColumn(
        "song_genre", add_song_genre_udf(col("artist"), col("title"))
    )

    return artist_info_top10_df


def add_song_genre(artist, track):

    url = f"https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LAST_FM_API_KEY}&artist={artist}&track={track}&format=json"

    try:
        response = requests.get(url).json()
        return [
            genre["name"] for genre in response.get(
                "track",
                {}).get(
                "toptags",
                {}).get(
                "tag",
                [])]
    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return ["API Error"]
    except KeyError:
        return ["Unknown"]


add_song_genre_udf = udf(add_song_genre, ArrayType(StringType()))


def extract(file_name, schema):

    spark = create_spark_session("artist_top10_table")
    df = spark.read.csv(
        f"s3a://{BUCKET_NAME}/{OBJECT_NAME}/{file_name}_{TODAY}.csv",
        header=True,
        schema=schema,
    )

    if file_name == "spotify_artist_info":
        df = df.withColumn(
            "artist_genre", regexp_replace(df["artist_genre"], "[\\[\\]']", "")
        )  # 불필요한 문자 제거
        df = df.withColumn(
            "artist_genre", split(df["artist_genre"], ", ")
        )  # 쉼표 기준으로 배열 변환

    return df


if __name__ == "__main__":
    load()
