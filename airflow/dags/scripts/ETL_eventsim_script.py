import sys
from datetime import datetime

from dags.plugins.snowflake_utils import execute_snowflake_query
from dags.plugins.spark_snowflake_conn import create_spark_session
from dags.plugins.variables import SNOWFLAKE_PROPERTIES, snowflake_options

# SNOW_FLAKE 설정
SNOWFLAKE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_TEMP_TABLE = "EVENTS_TABLE_TEMP"
SNOWFLAKE_SCHEMA = "RAW_DATA"

S3_BUCKET = sys.argv[1]
DATA_INTERVAL_START = sys.argv[2]
# 날짜 변환 (data_interval_start -> year/month/day 형식)
date_obj = datetime.strptime(DATA_INTERVAL_START, "%Y-%m-%d")
year = date_obj.strftime("%Y")
month = date_obj.strftime("%m")
day = date_obj.strftime("%d")

# -------------------------------------------------
spark = create_spark_session("etl_streaming_session")

# S3에서 데이터 읽어오기
df = spark.read.json(
    f"{S3_BUCKET}/topics/eventsim_music_streaming/year={year}/month={month}/day={day}/*.json"
)

df_clean = (
    df.select(
        "song",
        "artist",
        "location",
        "sessionId",
        "userId",
        "ts") .filter(
            (df.song.isNotNull()) & (
                df.artist.isNotNull()) & (
                    df.page != "Home")) .fillna("NULL"))
# df_clean = df.wehre("song IS NOT NULL AND artist IS NOT NULL")

print(df_clean.show(5))
print(f"Data count: {df_clean.count()}")

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
print("테이블 생성 완료")
# -----------------------UPSERT----------------------
# Snowflake TEMP 테이블에 데이터 적재
df_clean.write.format("snowflake").options(**snowflake_options).option(
    "dbtable", f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE}"
).mode("overwrite").save()
print("TEMP 테이블 적재 완료")

# Snowflake에서 MERGE 수행
merge_sql = f"""
MERGE INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
USING {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE} AS s
    ON target.USERID = s.userId
        AND target.TS = s.ts
        AND target.SONG = s.song
WHEN MATCHED THEN
    UPDATE SET
        target.LOCATION = s.location,
        target.SESSIONID = s.sessionId
WHEN NOT MATCHED THEN
    INSERT ("SONG", "ARTIST", "LOCATION", "SESSIONID", "USERID", "TS")
    VALUES (s.song, s.artist, s.location, s.sessionId, s.userId, s.ts);
"""
execute_snowflake_query(merge_sql, SNOWFLAKE_PROPERTIES)
print("Merge 완료")

# -------------------DROP TABLE--------------------
# 임시 테이블 삭제
drop_table_sql = f"""
DROP TABLE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TEMP_TABLE};
"""
execute_snowflake_query(drop_table_sql, SNOWFLAKE_PROPERTIES)
print("Drop Table")

spark.stop()
