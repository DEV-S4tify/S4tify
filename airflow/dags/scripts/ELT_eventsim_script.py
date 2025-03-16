import os

from dags.plugins.spark_snowflake_conn import create_spark_session
from dags.plugins.variables import snowflake_gold_options, snowflake_options
from pyspark.sql.functions import desc

# SNOWFLAKE 설정
SNOWFLAKE_SOURCE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_SOURCE_SCHEMA = os.environ.get("SNOWFLAKE_SILVER_SCHMEA", "RAW_DATA")
SNOWFLAKE_TARGET_SONG_TABLE = "EVENTSIM_SONG_COUNTS"
SNOWFLAKE_TARGET_ARTIST_TABLE = "EVENTSIM_ARTIST_COUNTS"
SNOWFLAKE_TARGET_SCHEMA = os.environ.get("SNOWFLAKE_GOLD_SCHEMA", "ANALYTICS")


def main():
    spark = create_spark_session("Spark_Snowflake_Processing")

    # Snowflake에서 데이터 읽기
    df = (
        spark.read.format("snowflake")
        .options(**snowflake_options)
        .option("dbtable", SNOWFLAKE_SOURCE_TABLE)
        .load()
    )

    df.show()  # 데이터 확인

    # SONG 집계 처리
    song_counts = (
        df.groupBy("SONG", "ARTIST")
        .count()
        .withColumnRenamed("count", "song_count")
        .orderBy(desc("song_count"))
    )

    # ARTIST 집계 처리
    artist_counts = (
        df.groupBy("ARTIST")
        .count()
        .withColumnRenamed("count", "artist_count")
        .orderBy(desc("artist_count"))
    )

    # Snowflake에 저장
    song_counts.write.format("snowflake").options(
        **snowflake_gold_options).option(
        "dbtable", SNOWFLAKE_TARGET_SONG_TABLE).mode("overwrite").save()

    artist_counts.write.format("snowflake").options(
        **snowflake_gold_options).option(
        "dbtable", SNOWFLAKE_TARGET_ARTIST_TABLE).mode("overwrite").save()

    print("(INFO): 데이터 적재 완료")
    spark.stop()


if __name__ == "__main__":
    main()
