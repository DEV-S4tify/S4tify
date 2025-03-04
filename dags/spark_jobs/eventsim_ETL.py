import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))  # S4tify 루트 디렉토리
sys.path.append(BASE_DIR)  # sys.path에 S4tify 추가

from dags.utils.spark_utils import spark_session_builder, execute_snowflake_query

load_dotenv("../../.env") # 환경 변수 로드

# SNOW_FLAKE 설정
SNOWFLAKE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_TEMP_TABLE = "EVENTS_TABLE_TEMP"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_PROPERTIES = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "db": os.environ.get("SNOWFLAKE_DB", "DATA_WAREHOUSE"),
    "warehouse": os.environ.get("SNOWFLAKE_WH", "COMPUTE_WH"),
    "schema": SNOWFLAKE_SCHEMA if SNOWFLAKE_SCHEMA else "raw_data",
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{os.environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com'
}


S3_BUCKET = "s3a://eventsim-log"
DATA_INTERVAL_START = '2025-02-28'

# 날짜 변환 (data_interval_start -> year/month/day 형식)
date_obj = datetime.strptime(DATA_INTERVAL_START, "%Y-%m-%d")
year = date_obj.strftime("%Y")
month = date_obj.strftime("%m")
day = date_obj.strftime("%d")


# -------------------------------------------------
spark = spark_session_builder("app")

schema = StructType([
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("location", StringType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("userId", IntegerType(), True),
    StructField("ts", LongType(), True)  # Snowflake의 BIGINT에 맞게 LongType 사용
])

# S3에서 데이터 읽어오기
df = spark.read \
    .json(f"{S3_BUCKET}/topics/eventsim_music_streaming/year={year}/month={month}/day={day}/*.json")

df_clean = df.dropna(subset=["song", "artist"]) 
# df_clean = df.wehre("song IS NOT NULL AND artist IS NOT NULL")

# -------------------CREATE TABLE--------------------
# 테이블 생성
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS raw_data.EVENTSIM_LOG (
    song STRING,
    artist STRING,
    location STRING,
    sessionId INT,
    userId INT,
    ts BIGINT
);
"""
execute_snowflake_query(create_table_sql, SNOWFLAKE_PROPERTIES)
# -----------------------UPSERT----------------------
# Snowflake TEMP 테이블에 데이터 적재
df_clean.write \
    .format("jdbc")\
    .options(**SNOWFLAKE_PROPERTIES) \
    .option("dbtable", f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE}") \
    .mode("overwrite") \
    .save()
print("TEMP Table에 데이터 적재 완료")

# Snowflake에서 MERGE 수행
merge_sql = f"""
MERGE INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
USING {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE} AS s
    ON target.USERID = s."userId"
        AND target.TS = s."ts"
        AND target.SONG = s."song"
WHEN MATCHED THEN
    UPDATE SET 
        target.LOCATION = s."location",
        target.SESSIONID = s."sessionId"
WHEN NOT MATCHED THEN
    INSERT ("SONG", "ARTIST", "LOCATION", "SESSIONID", "USERID", "TS")
    VALUES (s."song", s."artist", s."location", s."sessionId", s."userId", s."ts");
"""
execute_snowflake_query(merge_sql, SNOWFLAKE_PROPERTIES)
print("Merge 완료")

spark.stop()
