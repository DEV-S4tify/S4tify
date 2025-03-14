import os
from datetime import datetime

import snowflake.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when

from airflow.models import Variable

# Spark JARs 설정
SPARK_HOME = "/opt/spark/"
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_HOME, "jars", "snowflake-jdbc-3.9.2.jar"),
        os.path.join(SPARK_HOME, "jars", "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_HOME, "jars", "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)

# Snowflake 연결 정보 설정
SNOWFLAKE_OPTIONS = {
    "user": Variable.get("SNOWFLAKE_USER"),
    "password": Variable.get("SNOWFLAKE_PASSWORD"),
    "account": Variable.get("SNOWFLAKE_ACCOUNT"),
    "db": Variable.get("SNOWFLAKE_DB", "S4TIFY"),
    "warehouse": Variable.get("SNOWFLAKE_WH", "COMPUTE_WH"),
    "schema": "RAW_DATA",
    "role": "ACCOUNTADMIN",
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{Variable.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
}

# Spark Session 생성 함수
def spark_session_builder(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", SPARK_JARS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", Variable.get("AWS_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", Variable.get("AWS_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

# Snowflake에서 SQL 실행 함수
def check_and_create_table():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_OPTIONS["user"],
            password=SNOWFLAKE_OPTIONS["password"],
            account=SNOWFLAKE_OPTIONS["account"],
            database=SNOWFLAKE_OPTIONS["db"],
            schema=SNOWFLAKE_OPTIONS["schema"],
            warehouse=SNOWFLAKE_OPTIONS["warehouse"],
            role=SNOWFLAKE_OPTIONS["role"],
        )
        cur = conn.cursor()

        # 테이블 존재 여부 확인
        cur.execute(
            f"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_OPTIONS["schema"]}'
            AND UPPER(TABLE_NAME) = 'MUSIC_CHARTS'
        """
        )
        result = cur.fetchone()

        if result is None:
            # 테이블이 없으면 생성
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_OPTIONS['schema']}.music_charts (
                rank INT,
                title STRING,
                artist STRING,
                genre STRING,  -- 🎵 genre 컬럼 추가
                lastPos INT,
                image STRING,
                peakPos INT,
                isNew BOOLEAN,
                source STRING,
                date DATE -- 날짜 컬럼 추가
            )
            """
            cur.execute(create_table_query)
            print("✅ music_charts 테이블 생성 완료.")
        else:
            print("ℹ️ music_charts 테이블이 이미 존재합니다.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"⚠️ 테이블 확인 및 생성 중 오류 발생: {e}")


# 문자열에서 작은따옴표 처리 및 NULL 값 처리
def escape_quotes(value):
    if value is None:
        return "NULL"
    return "'{}'".format(value.replace("'", "''"))


# Snowflake에서 SQL 실행 함수
def insert_data_into_snowflake(df, table_name):
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_OPTIONS["user"],
            password=SNOWFLAKE_OPTIONS["password"],
            account=SNOWFLAKE_OPTIONS["account"],
            database=SNOWFLAKE_OPTIONS["db"],
            schema=SNOWFLAKE_OPTIONS["schema"],
            warehouse=SNOWFLAKE_OPTIONS["warehouse"],
            role=SNOWFLAKE_OPTIONS["role"],
        )
        cur = conn.cursor()

        for row in df.collect():
            rank = "NULL" if row["rank"] is None else row["rank"]
            title = escape_quotes(row["title"]) if row["title"] is not None else "NULL"
            artist = escape_quotes(row["artist"]) if row["artist"] is not None else "NULL"
            genre = escape_quotes(row["genre"]) if row["genre"] is not None else "NULL"  # 🎵 genre 추가
            lastPos = "NULL" if row["lastPos"] is None else row["lastPos"]
            image = escape_quotes(row["image"]) if row["image"] is not None else "NULL"
            peakPos = "NULL" if row["peakPos"] is None else row["peakPos"]
            isNew = "NULL" if row["isNew"] is None else ("TRUE" if row["isNew"] else "FALSE")
            source = escape_quotes(row["source"]) if row["source"] is not None else "NULL"
            date = f"'{row['date']}'"  # date 컬럼 추가

            query = f"""
                INSERT INTO {table_name} (rank, title, artist, genre, lastPos, image, peakPos, isNew, source, date)
                VALUES ({rank}, {title}, {artist}, {genre}, {lastPos}, {image}, {peakPos}, {isNew}, {source}, {date})
            """
            cur.execute(query)

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Data inserted into Snowflake successfully.")

    except Exception as e:
        print(query)
        print(f"⚠️ Error inserting data into Snowflake: {e}")


# Spark 세션 생성
spark = spark_session_builder("S3_to_Snowflake")

# 오늘 날짜 기반 S3 데이터 경로 생성
TODAY = datetime.now().strftime("%Y-%m-%d")
S3_BUCKET = "s3a://de5-s4tify"
chart_sources = {
    "bugs": f"{S3_BUCKET}/raw_data/bugs_chart_data/bugs_chart_{TODAY}.csv",
    "flo": f"{S3_BUCKET}/raw_data/flo_chart_data/flo_chart_{TODAY}.csv",
    "genie": f"{S3_BUCKET}/raw_data/genie_chart_data/genie_chart_{TODAY}.csv",
    "melon": f"{S3_BUCKET}/raw_data/melon_chart_data/melon_chart_{TODAY}.csv",
    "vibe": f"{S3_BUCKET}/raw_data/vibe_chart_data/vibe_chart_{TODAY}.csv",
}

def read_chart_data(source, path):
    try:
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(path)
        )
        df.printSchema()  # 데이터 스키마 출력해서 `genre`와 `date` 확인
        return df.withColumn("source", lit(source))
    except Exception as e:
        print(f"⚠️ {source} 데이터 로드 실패: {e}")
        return None


# 차트 데이터 읽기 및 병합
dfs = [read_chart_data(source, path) for source, path in chart_sources.items()]
dfs = [df for df in dfs if df is not None]

for df in dfs:
    df.show(40)

if dfs:
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.unionByName(df, allowMissingColumns=True)

    final_df = merged_df.select(
        when(col("rank").rlike("^[0-9]+$"),
             col("rank").cast("int")).alias("rank"),
        col("title"),
        col("artist"),
        col("genre"),  # genre 컬럼 추가
        when(col("lastPos").rlike("^[0-9]+$"), col("lastPos").cast("int")).alias(
            "lastPos"
        ),
        col("image"),
        when(col("peakPos").rlike("^[0-9]+$"), col("peakPos").cast("int")).alias(
            "peakPos"
        ),
        when(col("isNew").rlike("^(true|false)$"), col("isNew").cast("boolean")).alias(
            "isNew"
        ),
        col("source"),
        col("date"),  # date 컬럼 추가
    )

    final_df.show(40)

    # 데이터 확인
    final_df.groupBy("source").agg(count("*").alias("count")).show()

    # Snowflake에서 테이블 존재 여부 확인 및 생성
    check_and_create_table()

    # Snowflake에 데이터 적재
    insert_data_into_snowflake(final_df, "music_charts")

else:
    print("❌ 저장할 차트 데이터가 없습니다.")

# Spark 세션 종료
spark.stop()
