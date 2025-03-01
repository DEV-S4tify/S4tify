import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, ".."))

from dags.utils.spark_utils import spark_session_builder


S3_BUCKET = sys.argv[1]  # DAG에서 전달된 S3 버킷 경로
SNOWFLAKE_TABLE = "EVENTS_TABLE"
SNOWFLAKE_TEMP_TABLE = "EVENTS_TABLE_TEMP"
SNOWFLAKE_SCHEMA = "PUBLIC"

# Snowflake 연결 정보 
options = {
    "sfURL": "https://your_snowflake_account.snowflakecomputing.com/",
    "sfDatabase": "YOUR_DATABASE",
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": "YOUR_WAREHOUSE",
    "sfRole": "YOUR_ROLE",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD"
}

spark = spark_session_builder("EventsimS3ToSnowflake")

df = spark.read.json(f"{S3_BUCKET}/eventsim_music_streaming/*/*.json")

# 데이터 전처리
df_clean = df.select(
    col("song"), col("method"), col("auth"), col("level"),
    col("artist"), col("length"), col("location"),
    col("sessionId"), col("page"), col("userId"),
    col("ts"), col("status")
).dropna()  # Null 값 제거

# Snowflake Temp Table에 데이터 저장
df_clean.write \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", SNOWFLAKE_TEMP_TABLE) \
    .mode("overwrite") \
    .save()

print("✅ Temp Table에 데이터 저장 완료!")

# Snowflake에서 MERGE 수행
merge_sql = f"""
    MERGE INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
    USING {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE} AS source
    ON target.userId = source.userId AND target.ts = source.ts
    WHEN MATCHED THEN
        UPDATE SET 
            target.song = source.song,
            target.method = source.method,
            target.auth = source.auth,
            target.level = source.level,
            target.artist = source.artist,
            target.length = source.length,
            target.location = source.location,
            target.sessionId = source.sessionId,
            target.page = source.page,
            target.status = source.status
    WHEN NOT MATCHED THEN
        INSERT (song, method, auth, level, artist, length, location, sessionId, page, userId, ts, status)
        VALUES (source.song, source.method, source.auth, source.level, source.artist, source.length, source.location, source.sessionId, source.page, source.userId, source.ts, source.status);
"""

# Snowflake에서 MERGE 실행
from pyspark.sql import DataFrame
def execute_snowflake_query(query: str, options: dict):
    snowflake_df: DataFrame = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("query", query) \
        .load()
    return snowflake_df

execute_snowflake_query(merge_sql, options)

print("✅ Snowflake에 UPSERT 완료!")

# Spark 종료
spark.stop()