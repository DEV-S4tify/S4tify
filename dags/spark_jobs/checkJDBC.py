import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql.functions import col

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))  # S4tify 루트 디렉토리
sys.path.append(BASE_DIR)  # sys.path에 S4tify 추가

from dags.utils.spark_utils import spark_session_builder

load_dotenv("../../.env") # 환경 변수 로드

# SNOW_FLAKE 설정
SNOWFLAKE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_TEMP_TABLE = "EVENTS_TABLE_TEMP"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_PROPERTIES = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "DATA_WAREHOUSE"),
    "schema": SNOWFLAKE_SCHEMA if SNOWFLAKE_SCHEMA else "raw_data",
    "dtable": f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}",
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{os.environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com/?warehouse=COMPUTE_WH'
}

print(SNOWFLAKE_PROPERTIES['url'])


S3_BUCKET = "s3a://eventsim-log"
DATA_INTERVAL_START = '2025-02-28'

# 날짜 변환 (data_interval_start -> year/month/day 형식)
date_obj = datetime.strptime(DATA_INTERVAL_START, "%Y-%m-%d")
year = date_obj.strftime("%Y")
month = date_obj.strftime("%m")
day = date_obj.strftime("%d")


# -------------------------------------------------
spark = spark_session_builder("app")

# S3에서 데이터 읽어오기
# df = spark.read \
#     .json(f"{S3_BUCKET}/topics/eventsim_music_streaming/year={year}/month={month}/day={day}/*.json")

# df_clean = df.dropna(subset=["song", "artist"])
# df_clean = df.wehre("song IS NOT NULL AND artist IS NOT NULL")

# --------------------------------------------------
def execute_snowflake_query(query, snowflake_options):
    '''
    Snowflake에서 SQL 쿼리를 실행하는 함수
    '''
    try:
        spark.read \
            .format ("jdbc") \
            .options(**snowflake_options) \
            .option("query", query) \
            .load()
    except Exception as e:
        print(f"Execute_snowflake_query: {e}")

create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA};"

# ** 테이블 생성성
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    song STRING,
    artist STRING,
    location STRING,
    sessionId INT,
    userId STRING,
    ts BIGINT,
);
"""

execute_snowflake_query(create_schema_sql, SNOWFLAKE_PROPERTIES)
execute_snowflake_query(create_schema_sql, SNOWFLAKE_PROPERTIES)


# Snowflake에서 MERGE 수행
merge_sql = f"""
    MERGE INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
    USING {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE} AS source
    ON target.userId = source.userId AND target.ts = source.ts AND target.song = source.song
    WHEN MATCHED THEN
        UPDATE SET 
            target.location = source.location,
            target.sessionId = source.sessionId,
    WHEN NOT MATCHED THEN
        INSERT (song, artist, length, location, sessionId, userId, ts)
        VALUES (source.song, source.artist, source.length, source.location, source.sessionId, source.userId, source.ts);
"""

spark.stop()
