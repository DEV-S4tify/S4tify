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
    "schema": (
        Variable.get("SNOWFLAKE_SCHEMA")
        if Variable.get("SNOWFLAKE_SCHEMA")
        else "raw_data"
    ),
    "role": "ACCOUNTADMIN",
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{Variable.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
    # "url" : "jdbc:snowflake://kjqeovi-gr23658.snowflakecomputing.com",
}


# Spark Session 생성 함수
def spark_session_builder(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name) .config(
            "spark.jars",
            SPARK_JARS) .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") .config(
                "spark.hadoop.fs.s3a.access.key",
                Variable.get("AWS_ACCESS_KEY")) .config(
                    "spark.hadoop.fs.s3a.secret.key",
                    Variable.get("AWS_SECRET_KEY")) .config(
                        "spark.hadoop.fs.s3a.endpoint",
                        "s3.amazonaws.com") .config(
                            "spark.hadoop.fs.s3a.aws.credentials.provider",
                            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ) .getOrCreate())


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
        cur.execute(f"SHOW TABLES LIKE 'music_charts'")
        result = cur.fetchone()

        if result is None:
            # 테이블이 존재하지 않으면 생성
            create_table_query = """
            CREATE OR REPLACE TABLE ADHOC.music_charts (
                rank INT,
                title STRING,
                artist STRING,
                lastPos INT,
                image STRING,
                peakPos INT,
                isNew BOOLEAN,
                source STRING
            );
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
        return "NULL"  # None인 경우에는 'NULL'로 처리
    return "'{}'".format(
        value.replace("'", "''")
    )  # 작은따옴표는 두 개로 이스케이프 처리


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

        # DataFrame을 순회하며 INSERT 쿼리 실행
        for row in df.collect():
            # None 값을 NULL로 처리하고, 문자열 값은 작은따옴표로 감쌈
            rank = f"NULL" if row["rank"] is None else row["rank"]
            title = escape_quotes(
                f"NULL" if row["title"] is None else f"'{row['title']}'"
            )
            artist = escape_quotes(
                f"NULL" if row["artist"] is None else f"'{row['artist']}'"
            )
            lastPos = f"NULL" if row["lastPos"] is None else row["lastPos"]
            image = escape_quotes(
                f"NULL" if row["image"] is None else f"'{row['image']}'"
            )
            peakPos = f"NULL" if row["peakPos"] is None else row["peakPos"]
            # isNew 값은 TRUE/FALSE로 처리하고 NULL은 그대로 처리
            isNew = (
                f"NULL"
                if row["isNew"] is None
                else ("True" if row["isNew"] else "FALSE")
            )
            source = escape_quotes(
                f"NULL" if row["source"] is None else f"'{row['source']}'"
            )

            # 삽입할 쿼리 (컬럼 이름은 큰따옴표 없이)
            query = f"""
                INSERT INTO {table_name} (rank, title, artist, lastPos, image, peakPos, isNew, source)
                VALUES ({rank}, {title}, {artist}, {lastPos}, {image}, {peakPos}, {isNew}, {source})
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
TODAY = datetime.now().strftime("%Y%m%d")
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
        when(
            col("rank").rlike("^[0-9]+$"),
            col("rank").cast("int")).alias("rank"),
        col("title"),
        col("artist"),
        when(
            col("lastPos").rlike("^[0-9]+$"),
            col("lastPos").cast("int")).alias("lastPos"),
        col("image"),
        when(
                col("peakPos").rlike("^[0-9]+$"),
                col("peakPos").cast("int")).alias("peakPos"),
        when(
                    col("isNew").rlike("^(true|false)$"),
                    col("isNew").cast("boolean")).alias("isNew"),
        col("source"),
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
