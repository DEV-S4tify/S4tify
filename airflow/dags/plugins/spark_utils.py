import os

import snowflake.connector
from pyspark.sql import SparkSession
import pandas as pd
from airflow.exceptions import AirflowFailException

# Spark JARs 설정
# SPARK_HOME 설정
SPARK_HOME = "/opt/spark"
os.environ["SPARK_HOME"] = SPARK_HOME

# JAR 경로 설정
SPARK_JARS_DIR = os.path.join(SPARK_HOME, "jars")
SPARK_JARS_LIST = [
    "snowflake-jdbc-3.9.2.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar"
]
SPARK_JARS = ",".join([os.path.join(SPARK_JARS_DIR, jar) for jar in SPARK_JARS_LIST])


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
        .config("spark.driver.extraClassPath", "/opt/spark/jars/snowflake-jdbc-3.9.2.jar")
        .config("spark.executor.extraClassPath", SPARK_JARS)
        .getOrCreate()
    )


def execute_snowflake_query(query: str, snowflake_options: dict, data=None, fetch=False):
    """
    Snowflake에서 SQL 쿼리를 실행하거나 데이터를 조회하는 함수
    
    Args:
        query (str): 실행할 SQL 쿼리
        snowflake_options (dict): Snowflake 접속 정보
        data (list, optional): executemany를 사용할 경우 전달할 데이터 리스트
        fetch (bool, optional): SELECT 쿼리 실행 후 데이터를 반환할지 여부
    
    Returns:
        pd.DataFrame | None: fetch=True인 경우 DataFrame 반환, 그렇지 않으면 None 반환
    """
    print(f"snowflake_opt: {snowflake_options}")

    try:
        conn = snowflake.connector.connect(
            user=snowflake_options["user"],
            password=snowflake_options["password"],
            account=snowflake_options["account"],
            database=snowflake_options["db"],
            schema=snowflake_options["schema"],
            warehouse=snowflake_options["warehouse"],
            role=snowflake_options["role"],
        )
        cur = conn.cursor()
        
        if data:
            if isinstance(data, list):
                cur.executemany(query, data)
            else:
                cur.execute(query, data)
            conn.commit()
        
        elif not fetch:
            cur.execute(query)
            conn.commit()
        
        if fetch:
            result = cur.fetchall()  # 데이터 가져오기
            print('result: ' + result)
            if cur.description:  # 컬럼 정보가 존재할 경우에만 DataFrame 생성
                df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
            else:
                df = pd.DataFrame()  # 빈 DataFrame 반환
            cur.close()
            conn.close()
            return df
        
        cur.close()
        conn.close()
        print("Query executed successfully.")
    except Exception as e:
        print(f"Execute_snowflake_query Error: {e}")
        print(f'Query: {query}')
        print(f'Data: {data}')
        raise AirflowFailException("execute query error")

def escape_quotes(value):
    if value is None:
        return "NULL"
    return "'{}'".format(value.replace("'", "''"))