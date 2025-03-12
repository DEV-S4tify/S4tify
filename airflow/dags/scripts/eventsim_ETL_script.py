import os
import sys
from datetime import datetime
from dotenv import load_dotenv

from spark_utils import execute_snowflake_query, spark_session_builder
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

from airflow.models import Variable

load_dotenv()

# SNOW_FLAKE 설정
SNOWFLAKE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_TEMP_TABLE = "EVENTS_TABLE_TEMP"
SNOWFLAKE_SCHEMA = "RAW_DATA"
SNOWFLAKE_PROPERTIES = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "db": os.environ.get("SNOWFLAKE_DB", "S4TIFY"),
    "warehouse": os.environ.get("SNOWFLAKE_WH", "COMPUTE_WH"),
    "schema": SNOWFLAKE_SCHEMA if SNOWFLAKE_SCHEMA else "RAW_DATA",
    "role": os.environ.get("SNOWFLAKE_ROLE", "ANALYTICS_USERS"),
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{os.environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
}
S3_BUCKET = sys.argv[1]
DATA_INTERVAL_START = sys.argv[2]
# 날짜 변환 (data_interval_start -> year/month/day 형식)
date_obj = datetime.strptime(DATA_INTERVAL_START, "%Y-%m-%d")
year = date_obj.strftime("%Y")
month = date_obj.strftime("%m")
day = date_obj.strftime("%d")

# -------------------------------------------------
spark = spark_session_builder("app")

schema = StructType(
    [
        StructField("song", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("location", StringType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("userId", IntegerType(), True),
        # Snowflake의 BIGINT에 맞게 LongType 사용
        StructField("ts", LongType(), True),
    ]
)

# S3에서 데이터 읽어오기
df = spark.read.json(
    f"{S3_BUCKET}/topics/eventsim_music_streaming/year={year}/month={month}/day={day}/*.json"
)

df_clean = df.dropna(subset=["song", "artist"])
# df_clean = df.wehre("song IS NOT NULL AND artist IS NOT NULL")

# -------------------CREATE TABLE--------------------
# 테이블 생성
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    song STRING,
    artist STRING,
    location STRING,
    sessionId INT,
    userId INT,
    ts BIGINT
);
"""
execute_snowflake_query(create_table_sql, SNOWFLAKE_PROPERTIES)
print("Create Table")

# -----------------------UPSERT----------------------
# Snowflake TEMP 테이블에 데이터 적재
create_temp_table_sql = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE} (
    song STRING,
    artist STRING,
    location STRING,
    sessionId INT,
    userId INT,
    ts BIGINT
);
"""
execute_snowflake_query(create_temp_table_sql, SNOWFLAKE_PROPERTIES)
print("TEMP 테이블 확인 완료")

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

# -------------------DROP TABLE--------------------
# 임시 테이블 삭제제
drop_table_sql = f"""
DROP TABLE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE};
"""
execute_snowflake_query(drop_table_sql, SNOWFLAKE_PROPERTIES)
print("Drop Table")

spark.stop()
