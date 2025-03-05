import os

import snowflake.connector
from airflow.models import connection
from pyspark.sql import SparkSession

# Spark JARs 설정
SPARK_HOME = "/opt/spark/"
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_HOME, "jars", "snowflake-jdbc-3.9.2.jar"),
        os.path.join(SPARK_HOME, "jars", "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_HOME, "jars", "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)


# Spark Session builder
def spark_session_builder(app_name: str) -> SparkSession:
    """_summary_
        spark session builder for AWS S3 and Snowflake
    Args:
        app_name (str): spark session anme

    Returns:
        SparkSession
    """
    return (
        SparkSession.builder.appName(f"{app_name}")
        .config("spark.jars", SPARK_JARS)
        .getOrCreate()
    )


def execute_snowflake_query(query, snowflake_options):
    """
    Snowflake에서 SQL 쿼리를 실행하는 함수
    """
    try:
        conn = snowflake.connector.connect(
            user=snowflake_options["user"],
            password=snowflake_options["password"],
            account=snowflake_options["account"],
            database=snowflake_options["db"],
            schema=snowflake_options["schema"],
            warehouse=snowflake_options["warehouse"],
        )
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Query executed successfully.")
    except Exception as e:
        print(f"Execute_snowflake_query Error: {e}")
