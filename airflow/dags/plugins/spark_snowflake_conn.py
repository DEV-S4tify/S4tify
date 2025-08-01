import os
import shutil
from datetime import datetime

from dags.plugins.variables import SPARK_JARS
from pyspark.sql import SparkSession
from snowflake.connector.pandas_tools import write_pandas

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

TODAY = datetime.now().strftime("%Y-%m-%d")

snowflake_options = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfDatabase": os.getenv("SNOWFLAKE_DB"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
}


def create_spark_session(app_name: str):
    # 만약 정의된 connection이 cluster라면 master를 spark master 주소로 변경
    spark = (
        SparkSession.builder.appName(f"{app_name}")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.jars", SPARK_JARS)
        .getOrCreate()
    )

    return spark


def write_pandas_snowflake(df, table_name):

    conn = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_CONN", schema="RAW_DAT")

    success, num_chunks, num_rows, output = write_pandas(
        conn, df, f"{table_name}")


def write_spark_csv(file_name, df):
    temp_folder = f"data/{file_name}"
    df.coalesce(1).write.mode("overwrite").csv(temp_folder, header=True)

    # 저장된 디렉터리에서 CSV 파일 찾기
    csv_file = [f for f in os.listdir(temp_folder) if f.startswith("part-")][0]

    # 새 파일명 설정과 이동
    output_path = f"data/{file_name}.csv"
    shutil.move(os.path.join(temp_folder, csv_file), output_path)

    # 임시 폴더 삭제
    shutil.rmtree(temp_folder)


def create_snowflake_table(sql):

    hook = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_CONN", schema="RAW_DATA")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN")
        cur.execute(sql)
        cur.execute("COMMIT")
        conn.commit()

    except Exception as e:
        print(f"error:{e}")
        cur.execute("ROLLBACK")


def write_snowflake_spark_dataframe(table_name, df):

    snowflake_opts = snowflake_options.copy()

    if table_name in ["spotify_genre_count", "artist_genre_count"]:
        snowflake_opts["sfSchema"] = "ANALYTICS"

    df.write.format("snowflake").options(**snowflake_opts).option(
        "dbtable", f"{table_name}"
    ).mode("append").save()


def read_snowflake_spark_dataframe(spark, table_name):

    df = (
        spark.read.format("snowflake")
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .load()
    )

    return df
